/*
 * SGNode.java - A SkipGraph implementation.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: SkipGraph.java 1179 2015-05-23 07:11:17Z teranisi $
 */
package org.piax.gtrans.ov.sg;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ddll.Node.Mode;
import org.piax.gtrans.ov.ddll.NodeManager;
import org.piax.gtrans.ov.ddll.NodeManagerIf;
import org.piax.gtrans.ov.sg.SGMessagingFramework.SGReplyMessage;
import org.piax.gtrans.ov.sg.SGMessagingFramework.SGRequestMessage;
import org.piax.gtrans.ov.sg.SGNode.SGMode;
import org.piax.gtrans.ov.sg.SGNode.Tile;
import org.piax.util.KeyComparator;
import org.piax.util.MersenneTwister;
import org.piax.util.StrictMap;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 排他制御に関するノート
 * 
 * 経路表       全てのSGNodeの経路表を単一の Reader Writer Lock (rtlock) で保護
 * keyHash     rtlockで保護
 * 経路表エントリ 経路表のエントリ(DDLL Node)を参照する際は DDLL Nodeをread lock
 * 
 * ロック順序:
 *   rtlock -> DDLL Node
 *   
 * rtlockが解放されているときは，全てのsgmode==INSERTEDなノードの高さが揃っている必要がある．
 * 特に，sgmode==INSERTINGからINSERTEDに変更する処理は，rtlockをwrite lockし，
 * 他のノードと高さを揃えてから行う必要があることに注意．
 */

/**
 * Skip graphクラス．Skip graphに参加する単一物理ピアに対応する．
 * 複数のキーを登録できるが，メンバシップベクタは複数のキーで共通である．
 */
public class SkipGraph<E extends Endpoint> extends RPCInvoker<SkipGraphIf<E>, E>
        implements SkipGraphIf<E> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(SkipGraph.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("sg");

    /**
     * A state for each level of a skip graph.
     */
    public static enum LvState {
        INSERTING, INSERTED
    };

    NodeManager manager;
    E myLocator;
    final PeerId peerId;
    final MembershipVector mv;
    ReentrantReadWriteLock rtlock = new ReentrantReadWriteLock();
    private static final KeyComparator keyComp = KeyComparator.getInstance();

    NavigableMap<Comparable<?>, SGNode<E>> keyHash =
            new ConcurrentSkipListMap<Comparable<?>, SGNode<E>>(keyComp);
    private Random rand = new MersenneTwister();
    final SGExecQueryCallback execQueryCallback;
    final Timer timer;

    /** DDLL's left node check period for level 0 (msec) */
    public static int DDLL_CHECK_PERIOD_L0 = 10 * 1000;
    /** DDLL's left node check period for level 1 and above (msec) */
    public static int DDLL_CHECK_PERIOD_L1 = 30 * 1000;

    /**
     * Range Queryでトラバース中に通信エラーが起きた場合に戻れるノード数
     */
    public static int RQ_NRECENT = 10;
    public static int RPC_TIMEOUT = 20000;
    /*
     * every QID_EXPIRETION_TASK_PERIOD, all QueryId entries older than
     * QID_EXPIRE milliseconds are removed. 
     */
    /** expiration time for purging stale QueryIDs */ 
    public static int QID_EXPIRE = 120 * 1000; // 2min
    /** period for executing a task for purging stale QueryIDs */
    public static int QID_EXPIRATION_TASK_PERIOD = 20 * 1000; // 20sec

    /*
     * for scalable range queries
     */
    final SGMessagingFramework<E> sgmf;

    /** the period for flushing partial results in intermediate nodes */
    public static int RQ_FLUSH_PERIOD = 2000;
    /** additional grace time before removing RQReturn in intermediate nodes */
    public static int RQ_EXPIRATION_GRACE = 5 * 1000;
    /** range query retransmission period */
    public static int RQ_RETRANS_PERIOD = 10 * 1000;
    /** used as the query string for finding insert points.
     * see {@link SkipGraph#find(Endpoint, DdllKey, boolean)} */
    public static String QUERY_INSERT_POINT_SPECIAL =
            "*InsertPointSpecial*";
    /** timeout for {@link #find(Endpoint, DdllKey, boolean)} */
    public static int FIND_INSERT_POINT_TIMEOUT = 30 * 1000;

    /** pseudo PeerID used by {@link #rqDisseminate(RQMessage, boolean)} */
    private final static UniqId FIXPEERID = UniqId.PLUS_INFINITY;
    /** pseudo Link instance that represents the link should be fixed */
    private final Link FIXLEFT;

    /**
     * create a SkipGraph instance.
     * 
     * @param trans
     *            underlying transport
     * @param execQueryCallback
     *            a callback object called when
     *            #execQuery(Deque, Comparable, QueryCondition, Object)
     *            is called
     * @throws IdConflictException an exception occurs when the default transport id is registered.
     * @throws IOException an exception thrown when an I/O error occurred.
     */
    public SkipGraph(ChannelTransport<E> trans,
            SGExecQueryCallback execQueryCallback)
            throws IdConflictException, IOException {
        this(DEFAULT_TRANSPORT_ID, trans, execQueryCallback);
    }
    
    public SkipGraph(TransportId transId, ChannelTransport<E> trans, 
            SGExecQueryCallback execQueryCallback)
            throws IdConflictException, IOException {
        super(transId, trans);
        this.myLocator = trans.getEndpoint();
        this.peerId = trans.getPeerId();
        this.mv = new MembershipVector();
        this.manager = new NodeManager(new TransportId(transId.toString()
                + "-ddll"), trans);
        this.execQueryCallback = execQueryCallback;

        logger.debug("SkipGraph: transId={}", trans.getTransportId());

        timer = new Timer("SGTimer@" + myLocator, true);
        FIXLEFT = new Link(myLocator, new DdllKey(0, FIXPEERID));

        sgmf = new SGMessagingFramework<E>(this);

        timer.schedule(new PurgeTask(),
                (long) (Math.random() * QID_EXPIRATION_TASK_PERIOD),
                QID_EXPIRATION_TASK_PERIOD);
    }

    /**
     * a TimerTask for purging stale QueryId entries from
     * {@link SGNode#queryHistory}.
     */
    class PurgeTask extends TimerTask {
        @Override
        public void run() {
            final long threshold = System.currentTimeMillis() - QID_EXPIRE;
            rtLockR();
            for (SGNode<E> snode : keyHash.values()) {
                synchronized (snode.queryHistory) {
                    for (Iterator<QueryId> it = snode.queryHistory.iterator();
                            it.hasNext();) {
                        QueryId q = it.next();
                        if (q.timestamp < threshold) {
                            it.remove();
                        }
                    }
                }
            }
            rtUnlockR();
        }
    }

//    @Override
//    public synchronized void online() {
//        if (isOnline())
//            return;
//        super.online();
//        manager.online();
//    }
//
//    @Override
//    public synchronized void offline() {
//        if (!isOnline())
//            return;
//        manager.offline();
//        super.offline();
//    }

    @Override
    public synchronized void fin() {
        manager.fin();
        sgmf.fin();
        super.fin();
    }

    /**
     * get SGNode instance (for debugging only)
     * 
     * @param rawkey the raw key.
     * @return the corresponding SGNode object.
     */
    public SGNode<E> getSGNode(Comparable<?> rawkey) {
        SGNode<E> snode = keyHash.get(rawkey);
        return snode;
    }

    /**
     * get PeerId
     * 
     * @return PeerId
     */
    public PeerId getPeerId() {
        return peerId;
    }

    /*
     * reader-writer locks
     */
    void rtLockR() {
        rtlock.readLock().lock();
    }

    void rtUnlockR() {
        rtlock.readLock().unlock();
    }

    void rtLockW() {
        rtlock.writeLock().lock();
    }

    void rtUnlockW() {
        rtlock.writeLock().unlock();
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
//        if (!isOnline()) {
//            buf.append("(offline)\n");
//        }
        buf.append("PeerId: " + peerId + "\n");
        buf.append("    mv: " + mv + "\n");
        for (SGNode<E> snode : keyHash.values()) {
            buf.append(snode);
        }
        return buf.toString();
    }

    public String toStringShort() {
        return keyHash.keySet().toString();
    }

    /**
     * get a the routing table in plain text format.
     * 
     * @return routing table
     */
    public String showTable() {
        return toString();
    }

    /**
     * gather all links from all SGNodes.
     * 
     * @return all links from all SGNodes.
     */
    private NavigableMap<DdllKey, Link> getAllLinks(boolean insertedOnly) {
        assert rtlock.getReadLockCount() > 0
                || rtlock.isWriteLockedByCurrentThread();
        NavigableMap<DdllKey, Link> allLinks =
                new ConcurrentSkipListMap<DdllKey, Link>();
        for (SGNode<E> n : keyHash.values()) {
            for (Link link : n.getAllLinks(insertedOnly)) {
                allLinks.put(link.key, link);
            }
        }
        return allLinks;
    }

    /**
     * add a key to the skip graph.
     * 
     * @param rawkey
     *            the key
     * @return true if the key was successfully added
     * @throws IOException
     *             thrown when some I/O error occurred in communicating with the
     *             introducer
     * @throws UnavailableException
     *             thrown when the introducer has no key
     */
    public boolean addKey(Comparable<?> rawkey) throws IOException,
            UnavailableException {
        return addKey(null, rawkey);
    }

    /**
     * add a key to the skip graph.
     * <p>
     * 同一キーは複数登録できない． 1物理ノード上に複数キーを並行挿入できるようにすることが
     * 困難だったので，synchronized method としている．
     * 
     * @param introducer
     *            some node that has been inserted to the skip graph
     * @param rawkey
     *            the key
     * @return true if the key was successfully added
     * @throws IOException
     *             thrown when some I/O error occurred in communicating with the
     *             introducer
     * @throws UnavailableException
     *             thrown when the introducer has no key
     */
    public synchronized boolean addKey(E introducer,
            Comparable<?> rawkey) throws UnavailableException, IOException {
        if (myLocator.equals(introducer)) {
            introducer = null;
        }
        rtLockW();
        if (keyHash.containsKey(rawkey)) {
            rtUnlockW();
            logger.debug("addKey: already registered: {}", rawkey);
            return false;
        }
        SGNode<E> n = new SGNode<E>(this, mv, rawkey);
        keyHash.put(rawkey, n);
        rtUnlockW();
        try {
            boolean rc = n.addKey(introducer);
            if (!rc) {
                logger.error("addKey failed!");
                rtLockW();
                keyHash.remove(rawkey);
                rtUnlockW();
                return false;
            }
            return rc;
        } catch (UnavailableException e) {
            rtLockW();
            keyHash.remove(rawkey);
            rtUnlockW();
            throw e;
        } catch (IOException e) {
            rtLockW();
            keyHash.remove(rawkey);
            rtUnlockW();
            throw e;
        }
    }

    /**
     * remove a key from the skip graph.
     * 
     * @param rawkey
     *            the key
     * @return true if the key was successfully removed
     * @throws IOException
     *             thrown if some I/O error occurred.
     */
    public boolean removeKey(Comparable<?> rawkey) throws IOException {
        logger.debug("removeKey key={}\n{}", rawkey, this);
        SGNode<E> snode = keyHash.get(rawkey);
        if (snode == null) {
            return false;
        }
        // removeKeyは時間がかかるため，その間にgetSGNodeInfo()等が呼ばれる可能性がある．
        // このため，keyHashからkeyを削除するのは後で行う．
        boolean rc = snode.removeKey();
        if (rc) {
            rtLockW();
            keyHash.remove(rawkey);
            rtUnlockW();
        }
        return rc;
    }

    @Override
    public Link[] getLocalLinks() {
        List<Link> list = new ArrayList<Link>();
        rtLockR();
        try {
            for (SGNode<E> snode : keyHash.values()) {
                Link link = snode.getMyLinkAtLevel0();
                if (link != null) {
                    list.add(link);
                }
            }
        } finally {
            rtUnlockR();
        }
        Link[] links = list.toArray(new Link[list.size()]);
        return links;
    }

    /**
     * ローカルの経路表からkeyに最も近いリンクを検索する．
     * RPCで呼ばれる．
     * 返される BestLink u は，isOrdered(u.link.key, key, u.rightLink.key) を満たす．
     * 
     * @param key
     *            検索するキー
     * @param accurate
     *            正確な値が必要ならばtrue (左リンクを信用しない)
     * @return BestLink
     * @throws UnavailableException
     *             ローカルの経路表にキーが1つも登録されていない場合
     */
    @Deprecated
    @Override
    public BestLink findClosestLocal(DdllKey key, boolean accurate)
            throws UnavailableException {
        logger.debug("findClosestLocal({}) is called at node {}", key, myLocator);
        logger.debug(this.toString());
        BestLink best = null;
        rtLockR();
        try {
            for (SGNode<E> snode : keyHash.values()) {
                BestLink b = snode.findClosestLocal(key, accurate);
                logger.debug("{} says b = {} for {}", snode.rawkey, b, key);
                if (b == null) {
                    continue;
                }
                if (b.isImmedPred) {
                    best = b;
                    break;
                }
                if (best == null) {
                    best = b;
                    continue;
                }
                // best <= b <= key case
                if (Node.isOrdered(best.link.key, b.link.key, key)) {
                    best = b;
                }
            }
        } finally {
            rtUnlockR();
        }
        logger.debug("findClosestLocal({}) returns {}", key, best);
        if (best == null) {
            throw new UnavailableException(myLocator + " has no key");
        }
        // the following assertion is not attained in a rare case.
        // ノードが key 10 を保持していて 20を登録しようとしているとき，
        // 10が20からのSetRを受信したが，20がまだSetRAck を受信する前に
        // findClosestLocal(30)が呼ばれると，10はsnode.findClosestLocalで20を返すが
        // 20はまだinsertingなのでnullを返す．その結果，best=20となってしまう．
        // assert best.iAmClosest || !best.link.addr.equals(myLocator);

        return best;
    }

    /**
     * find the immediate left link of `key' from the local routing table.
     *
     * @param key  the key
     * @return the immediate left link of `key'
     * @throws UnavailableException
     *              thrown if no key is available at the local routing table
     */
    private Link findLeftLink(DdllKey key) throws UnavailableException {
        logger.debug("findLeftLink({}) is called at node {}", key, myLocator);
        logger.debug(this.toString());
        rtLockR();
        NavigableMap<DdllKey, Link> allLinks = getAllLinks(false);
        rtUnlockR();
        Map.Entry<DdllKey, Link> ent = allLinks.floorEntry(key);
        if (ent == null) {
            ent = allLinks.lastEntry();
            if (ent == null) {
                throw new UnavailableException(myLocator + " has no key");
            }
        }
        Link left = ent.getValue();
        return left;
        /*BestLink best;
        if (left.key.peerId.equals(peerId)) {
            Map.Entry<DdllKey, Link> entR = allLinks.higherEntry(left.key);
            if (entR == null) {
                entR = allLinks.firstEntry();
            }
            Link right = entR.getValue();
            best = new BestLink(left, right, true);
        } else {
            best = new BestLink(left, null, false);
        }
        return best;*/
    }

    @Override
    public SGNodeInfo getSGNodeInfo(Comparable<?> target, int level,
            MembershipVector mv, int nTraversed) throws NoSuchKeyException {
        SGNode<E> snode = keyHash.get(target);
        if (snode == null) {
            throw new NoSuchKeyException(target + ", " + keyHash);
        }
        return snode.getSGNodeInfo(level, mv, nTraversed);
    }

    /**
     * find a location to insert `key'.
     * <p>
     * iterative routingを使用．
     * 
     * <pre>
     * アルゴリズム: 
     * ・introducerからBestLinkを得る (findBestの呼び出し)．
     *   introducer==nullの場合，自ノードの経路表を使う．
     * ・introducerとの通信で通信エラーが発生した場合はIOExceptionをthrowする．
     * ・エラーでなければ再帰的に自分自身を呼び出す．
     * ・IOExceptionをキャッチしたら，introducerからBestLinkを再度得てやりなおす．
     * </pre>
     * 
     * @param introducer
     *            the node to communicate with.
     * @param key
     *            the query key
     * @param accurate
     *            true if accurate location is required (do not trust left links
     *            of DDLL node)
     * @return the insertion point for `key'
     * @throws UnavailableException
     *             自ノードあるいはseedにkeyが登録されていない
     * @throws IOException
     *             seedと通信する際にエラーが発生
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public InsertPoint findOld(E introducer, DdllKey key, boolean accurate)
            throws UnavailableException, IOException {
        while (true) {
            BestLink bestLink = null;
            if (introducer == null) {
                // use the local routing table
                // NoSuchKeyException is thrown if no key is registered
                bestLink = findClosestLocal(key, accurate);
            } else {
                // obtain the best node from the seed node
                SkipGraphIf<E> stub = getStub(introducer);
                try {
                    logger.debug("find: trying to call findClosestLocal({}) at node {}",
                            key, introducer);
                    bestLink = stub.findClosestLocal(key, accurate);
                } catch (UnavailableException e) {
                    throw e;
                } catch (RPCException e) {
                    logger.debug("", e);
                    if (e.getCause() instanceof IOException) {
                        throw (IOException) e.getCause();
                    }
                    throw new IOException(e.getCause());
                }
            }
            logger.debug("find: got {}", bestLink);
            if (bestLink.isImmedPred) {
                logger.debug("find: returns {}", bestLink.link);
                return new InsertPoint(bestLink.link, bestLink.rightLink);
            }

            // recursively call myself
            try {
                return findOld((E) bestLink.link.addr, key, accurate);
            } catch (UnavailableException e) {
                // あるノード(自ノードを含む)nの経路表からノードx(=bestLink)を得て，
                // xのfindClosestLocal() を呼び出した際に，xの全てのkeyが
                // 削除されていた場合に，この例外が発生する．
                // いずれnの経路表が更新されてx以外のノードを指すはずなのでリトライする．
                logger.debug("find: got {}, retry in 1000msec", e);
                try {
                    // TODO checked by yos. possibility of hardcoding
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                }
                // fall through
            } catch (IOException e) {
                logger.debug("find: got {}", e);
                // 既にタイムアウト時間が経過しているはずなので sleep せずに
                // リトライする．
                // fall through
            }
        }
    }

    /**
     * find a location to insert `key'.
     * 
     * @param introducer
     *            the node to communicate with.
     * @param key
     *            the query key
     * @param accurate
     *            true if accurate location is required (do not trust left links
     *            of DDLL node)
     * @return the insertion point for `key'
     * @throws UnavailableException
     *             自ノードあるいはseedにkeyが登録されていない
     * @throws IOException
     *             communication error
     */
    public InsertPoint find(E introducer, DdllKey key, boolean accurate)
            throws UnavailableException, IOException {
        NavigableMap<DdllKey, Link> links;
        if (introducer == null) {
            // use the local routing table
            rtLockR();
            links = getAllLinks(true);
            rtUnlockR();
            if (links.size() == 0) {
                throw new UnavailableException(
                        "no key is available at local node");
            }
        } else { // ask the introducer
            SkipGraphIf<E> stub = getStub(introducer);
            Link[] remoteLinks;
            try {
                remoteLinks = stub.getLocalLinks();
            } catch (RPCException e) {
                logger.debug("", e);
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw new IOException(e.getCause());
            }
            if (remoteLinks.length == 0) {
                throw new UnavailableException(
                        "no key is available at remote node: " + introducer);
            }
            logger.debug("find: remoteLinks = {}", Arrays.toString(remoteLinks));
            links = new ConcurrentSkipListMap<DdllKey, Link>();
            links.put(remoteLinks[0].key, remoteLinks[0]);
        }

        Range<DdllKey> range = new Range<DdllKey>(key, true, key, true);
        TransOptions opts = new TransOptions(FIND_INSERT_POINT_TIMEOUT, ResponseType.DIRECT);
        RQReturn<E> rqRet = rqStartKeyRange(
                Collections.<Range<DdllKey>> singleton(range),
                QUERY_INSERT_POINT_SPECIAL,
                RQ_RETRANS_PERIOD, opts, links);
        
        try {
            Collection<RemoteValue<?>> col = rqRet.get(FIND_INSERT_POINT_TIMEOUT);
            // the case where the key has been inserted
            logger.debug("find: col = {}", col);
        } catch (InterruptedException e) {
            throw new IOException("range query timeout");
        }
        // 本来，col から insp を取得すべきだが，col には aux 情報がないので，
        // 仕方なく rqRet.rvals から直接取得している．
        for (DdllKeyRange<RemoteValue<?>> kr : rqRet.rvals.values()) {
            if (kr.aux.getOption() != null) {
                InsertPoint insp = (InsertPoint) kr.aux.getOption();
                logger.debug("find: insert point = {}, {}", insp, kr);
                return insp;
            }
        }
        /*
         * FIND_INSERT_POINT_TIMEOUT 内に挿入位置が得られなかった．
         * skip graph を修復中などの場合，ここに到達する可能性がある．
         * 呼び出し側でリトライさせるために TemporaryIOException をスローする．
         */
        throw new TemporaryIOException("could not find insert point");
    }

    /**
     * ローカルに登録されている全てのSGNodeが保持する経路表の高さをlevelに設定する．
     * 各経路表の現在の高さよりも高い部分は，ローカルノード内で相互に接続する．
     * 
     * @param level the leve.
     */
    void adjustHeight(int level) {
        logger.debug("adjustHeight to {}", level);
        logger.debug(this.toString());
        rtLockW();
        try {
            List<Node> list = new ArrayList<Node>();
            for (SGNode<E> snode : keyHash.values()) {
                if (snode.sgmode != SGMode.INSERTED
                        || snode.table.size() > level) {
                    continue;
                }
                Node n = snode.createDdllNode(level);
                Tile t = new Tile(n, LvState.INSERTED);
                snode.table.add(level, t);
                list.add(n);
            }
            Node.initializeConnectedNodes(list);
        } catch (IndexOutOfBoundsException e) {
            logger.debug("****EX****");
            logger.error("", e);
            logger.debug(this.toString());
            System.exit(1);
        } finally {
            rtUnlockW();
        }
        // logger.debug("adjust height to " + level + "(after)");
        // logger.debug(this.toString());
    }

    /**
     * 経路表の高さを返す．
     * Level0しかない経路表の高さは1．
     * INSERTEDであるノードの経路表高さは一致していなければならない．
     * 
     * @return the height of the routing tables
     */
    public int getHeight() {
        int h = 0;
        rtLockR();
        try {
            for (SGNode<E> snode : keyHash.values()) {
                int l = snode.getInsertedHeight();
                h = Math.max(h, l);
            }
        } finally {
            rtUnlockR();
        }
        return h;
    }

    /**
     * 指定された複数の範囲にクエリを転送し、得られた結果（リスト）を返す。
     * 
     * @param ranges
     *            範囲のリスト
     * @param query
     *            クエリ
     * @return クエリ結果のリスト
     */
    public List<RemoteValue<?>> forwardQuery(Collection<? extends Range<?>> ranges,
            Object query) {
        logger.trace("ENTRY:");
        try {
            List<RemoteValue<?>> l = new ArrayList<RemoteValue<?>>();
            for (Range<?> r : ranges) {
                l.addAll(forwardQuery0(r, query));
            }
            return l;
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * Simple implementation of a range query. We assume that this SkipGraph
     * instance contains some key.
     * 
     * @param range
     *            a query range
     * @param callbackOv
     * @param query
     * @return
     */
    @SuppressWarnings("unchecked")
    private List<RemoteValue<?>> forwardQuery0(Range<?> range, Object query) {
        logger.trace("ENTRY:");
        DdllKey fromKey = new DdllKey(range.from, UniqId.MINUS_INFINITY); // inclusive
        DdllKey toKey = new DdllKey(range.to, UniqId.PLUS_INFINITY); // inclusive
        List<RemoteValue<?>> rset = new ArrayList<RemoteValue<?>>();
        QueryId qid = new QueryId(peerId, rand.nextLong());
        // n1 -> n2 -> n3 と辿って，n3と通信できなかった場合，再度n2と通信して
        // n2の右リンクが修復されるのを待つ．n2とも通信できなくなると，n1に戻る．
        // このために，直前に辿った RQ_NRECENT 個のノードを保持する．
        LinkedList<Link> trace = new LinkedList<Link>();
        // nから右方向に，レベル0のリンクを使ってトラバースする．
        boolean getStartNode = true;
        Link n = null;
        while (true) {
            if (getStartNode) {
                // get the start node on the left side
                try {
                    InsertPoint links = find(null, fromKey, true);
                    n = links.left;
                } catch (UnavailableException e) {
                    // offline?
                    logger.error("", e);
                    return null;
                } catch (IOException e) {
                    // should not happen
                    logger.error("", e);
                    return null;
                }
                getStartNode = false;
            }
            logger.debug("forwardQuery: query to {}", n);
            // ここのstubではexecQueryを呼び出すため、デフォルトのタイムアウト値を用いる必要がある
            SkipGraphIf<E> stub = getStub((E) n.addr);
            ExecQueryReturn eqr;
            // doActionフラグは，Range Queryを受信したノードで対応するアクションを
            // 実行するかどうかを決定する．
            // find()で求めた左端のノードnでは，n.keyはrangeには含まれていないため，
            // range.isInでチェックしてアクションの実行を省略する．
            boolean doAction = range.contains((Comparable<?>)n.key.getPrimaryKey());
            try {
                eqr = stub.invokeExecQuery(n.key.getPrimaryKey(), null, qid,
                                doAction, query);
            } catch (Throwable e) {
                assert e instanceof NoSuchKeyException
                        || e instanceof RPCException;
                Throwable cause = (e instanceof RPCException ? e.getCause() : e);
                if (!(cause instanceof NoSuchKeyException 
                        || cause instanceof InterruptedIOException)) {
                    // execQuery() をRPCで処理した際に起こるstub回りタイムアウトを除く例外と
                    // execQuery() の外で起こる invokeExecQuery() における例外のケース
                    // バグとして扱う．
                    logger.error("forwardQueryLeft: got {} when calling " +
                    		"invokeExecQuery() on {}", cause, n);
                    break;
                }
                logger.debug("", cause);
                if (trace.size() == 0) {
                    logger.debug("forwardQuery: start over");
                    getStartNode = true;
                } else {
                    n = trace.removeLast();
                    logger.debug("forwardQuery: retry from {}", n);
                }
                continue;
            }
            // eqr.key == null if the message is retransmitted
            if (doAction && eqr.key != null) {
                rset.add(eqr.key);
            }
            if (keyComp.isOrdered(n.key, toKey, eqr.right.key)
                    && toKey.compareTo(eqr.right.key) != 0) {
                logger.debug("forwardQuery: reached to the right end {}",
                        eqr.right);
                break;
            }
            trace.addLast(n);
            n = eqr.right;
            if (trace.size() > RQ_NRECENT) {
                trace.removeFirst();
            }
            logger.debug("trace= {}", trace);
        }
        return rset;
    }

    /**
     * 指定されたkeyとNavigableCondの条件を満たすキーを持つピア（最大num個）にクエリを転送し、
     * 得られた結果（リスト）を返す。
     * <p>
     * 例えば、keyが10、NavigableCondがLOWER、numが5の場合、10を越えなくて、
     * 10の近傍にある最大5個のキーが探索対象となる。 numに2以上をセットする用途としては、
     * DHTのように特定のkeyの近傍集合に
     * アクセスするケースがある。 尚、探索対象となるキーは、指定されたkeyと同じ型を持つ必要が
     * ある。
     * 
     * @param isPlusDir +方向にrangeにサーチする場合はtrue
     * @param range 探索対象となるRange
     * @param maxNum 条件を満たす最大個数
     * @param query the query passed to execQuery()
     * @return クエリ結果のリスト
     */
    public List<RemoteValue<?>> forwardQuery(boolean isPlusDir, Range<?> range, 
            int maxNum, Object query) {
        logger.trace("ENTRY:");
        logger.debug("isPlusdir:{}, range:{}, num:{}", isPlusDir, range, maxNum);
        try {
            if (!isPlusDir) {
                // lower
                return forwardQueryLeft(range, maxNum, query, false);
            } else {
                throw new UnsupportedOperationException("upper is not supported");
            }
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * 
     * @param range 探索対象となるRange
     * @param maxNum 条件を満たす最大個数
     * @param query the query passed to execQuery()
     * @param wrapAround
     * @return クエリ結果のリスト
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<RemoteValue<?>> forwardQueryLeft(Range<?> range, int num, 
            Object query, boolean wrapAround) {
        Comparable rawFromKey = range.to;
        DdllKey fromKey = range.toInclusive ? new DdllKey(rawFromKey, UniqId.PLUS_INFINITY) :
            new DdllKey(rawFromKey, UniqId.MINUS_INFINITY);
        Comparable rawToKey = range.from;
        DdllKey toKey = range.fromInclusive ? new DdllKey(rawToKey, UniqId.MINUS_INFINITY) :
            new DdllKey(rawToKey, UniqId.PLUS_INFINITY);
        
        List<RemoteValue<?>> rset = new ArrayList<RemoteValue<?>>();
        QueryId qid = new QueryId(peerId, rand.nextLong());
        // n1 -> n2 -> n3 と辿って，n3と通信できなかった場合，再度n2と通信して
        // n2の右リンクが修復されるのを待つ．n2とも通信できなくなると，n1に戻る．
        // このために，直前に辿った RQ_NRECENT 個のノードを保持する．
        LinkedList<Link> trace = new LinkedList<Link>();
        // nから左方向に，レベル0のリンクを使ってトラバースする．
        boolean getStartNode = true;
        Link n = null, nRight = null;
        while (true) {
            if (getStartNode) {
                // get the start node
                try {
                    InsertPoint links = find(null, fromKey, true);
                    n = links.left;
                    nRight = links.right;
                } catch (UnavailableException e) {
                    // offline?
                    logger.error("", e);
                    return null;
                } catch (IOException e) {
                    // should not happen
                    logger.error("", e);
                    return null;
                }
                getStartNode = false;
            }
            if (!new Range(toKey, fromKey).contains(n.key)) {
                logger.debug("forwardQueryLeft: finish (reached end of range)");
                break;
            }
            // 例えば key 0 しか存在しないときに，find() で -1 を検索すると，一周して
            // key 0 が帰ってくる．この場合はクエリ対象が存在しないので終了する．
            if (!wrapAround && keyComp.compare(rawFromKey, n.key.getPrimaryKey()) < 0) {
                logger.debug("forwardQueryLeft: finish (no node is smaller than rawFromKey)");
                break;
            }
            logger.debug("forwardQueryLeft: query to " + n);
            // ここのstubではexecQueryを呼び出すため、デフォルトのタイムアウト値を用いる必要がある
            SkipGraphIf stub = getStub((E) n.addr);
            ExecQueryReturn eqr;
            // doActionフラグは，Queryを受信したノードで対応するアクションを
            // 実行するかどうかを決定する．
            boolean doAction = true;
            try {
                eqr = stub.invokeExecQuery(n.key.getPrimaryKey(), nRight,
                                qid, doAction, query);
            } catch (RightNodeMismatch e) {
                // XXX: not tested
                logger.debug("", e);
                if (keyComp.isOrdered(n.key, e.curRight.key, nRight.key)) {
                    // ---> X
                    // N <------- nRight
                    // おそらくnRightがXからのSetLをまだ受信していない．
                    // N := X としてやりなおす
                    n = e.curRight;
                    logger.debug("forwardQueryLeft: right node mismatch. "
                            + "restart from {}", n);
                    continue;
                }
                // --------------> X
                // N <--- nRight
                // Nの右ノードがnRightを行き過ぎている．以下の可能性がある．
                // (1) nRightは削除された．
                // (2) XがnRightを誤って故障したと判断し，Nの右リンクをXに強制的に付け替え．
                // とりあえず，そのまま処理を継続することにする．
                nRight = e.curRight;
                logger.debug("forwardQueryLeft: right node mismatch. "
                        + "continue from {}", n);
                continue;
            } catch (Throwable e) {
                assert e instanceof UnavailableException
                        || e instanceof RPCException;
                Throwable cause = (e instanceof RPCException ? e.getCause() : e);
                if (!(cause instanceof UnavailableException
                        || cause instanceof InterruptedIOException)) {
                    // execQuery() をRPCで処理した際に起こるstub回りタイムアウトを除く例外と
                    // execQuery() の外で起こる invokeExecQuery() における例外のケース
                    // バグとして扱う．
                    logger.error("forwardQueryLeft: got {}"
                            + " when calling invokeExecQuery() on ", cause, n);
                    break;
                }
                logger.debug("", cause);
                if (trace.size() == 0) {
                    logger.debug("forwardQueryLeft: start over");
                    getStartNode = true;
                } else {
                    // nextは最後に通信したノード
                    Link next = trace.removeLast();
                    logger.debug("forwardQueryLeft: retry from {}", next);
                    // nextに，左リンクの修復を要求する
                    // TODO: nextの複数のレベルで左リンクがnの可能性があるため，
                    // 本来はNodeではなくSkipGraphのレベルで左リンクの修復を要求するべき．
                    NodeManagerIf stub2 =
                            manager.getStub(next.addr);
                    try {
                        stub2.startFix(next.key, n, false);
                    } catch (Exception e2) {
                        logger.info("", e2);
                    }
                    // nextで左リンクを修復するためには，getStatがタイムアウトする時間
                    // が必要なので，その時間待つ．
                    try {
                        // TODO checked by yos. possibility of hardcoding
                        Thread.sleep(Node.GETSTAT_OP_TIMEOUT + 100);
                    } catch (InterruptedException e2) {
                    }
                    n = next;
                    nRight = null; // XXX: Think!!!
                }
                continue;
            }
            // eqr.key == null if the message is retransmitted
            if (doAction && eqr.key != null) {
                rset.add(eqr.key);
            }
            if (rset.size() >= num) {
                logger.debug("forwardQueryLeft: got enough");
                break;
            }
            // 一周したかチェック
            // XXX: 左リンクが正しいものとして扱っている
            if (Node.isOrdered(eqr.left.key, fromKey, n.key)) {
                logger.debug("forwardQueryLeft: circulated");
                break;
            }
            // n の左ノードが n と等しい場合，可能性としては
            // (1)n の SetL の受信が遅れている (2)nしか存在しない，の2通り．
            // (1)の場合，nの右ノードはnを指さないが，(2)の場合はnを指す．
            // (1)の場合はnに再送，(2)の場合は終了する．
            if (n.key.equals(eqr.left.key) && n.key.equals(eqr.right.key)) {
                logger.debug("forwardQueryLeft: just single node exists");
                break;
            }
            if (!n.equals(eqr.left)) {
                trace.addLast(n);
                nRight = n;
                n = eqr.left;
                if (trace.size() > RQ_NRECENT) {
                    trace.removeFirst();
                }
                logger.debug("trace= {}", trace);
            }
        }
        return rset;
    }

    /**
     * Range Query実行時にRPCで呼び出されるメソッド．
     * 指定されたパラメータで execQuery を実行する． 
     * curRight != null の場合，レベル0の右リンクがcurRightと等しい場合にのみ実行．
     * execQueryの結果(RemoteValue)と，レベル0での左右のリンクを
     * {@link ExecQueryReturn} に詰めて返す．
     * 
     * @throws NoSuchKeyException
     *             rawkeyが存在しない
     * @throws RightNodeMismatch
     *             curRightがマッチしない
     */
    public ExecQueryReturn invokeExecQuery(
            Comparable<?> rawkey, Link curRight, QueryId qid, boolean doAction,
            Object query) throws NoSuchKeyException,
            RightNodeMismatch {
        logger.trace("ENTRY:");
        logger.debug("invokeExecQuery: rawkey={}, curRight={}"
                + ", qid={}, doAction={}", rawkey, curRight, qid, doAction);
        ExecQueryReturn eqr = new ExecQueryReturn();
        SGNode<E> snode;
        rtLockR(); // to avoid race condition with removeKey()
        try {
            snode = keyHash.get(rawkey);
            if (snode == null) {
                throw new NoSuchKeyException(rawkey + ", " + keyHash);
            }
            Node node = snode.table.get(0).node;
            node.lock();
            if (node.getMode() == Mode.GRACE || node.getMode() == Mode.OUT) {
                // GRACEならばもう少しスマートな方法がありそうだが，とりあえず．
                node.unlock();
                throw new NoSuchKeyException(rawkey + " has been deleted");
            }
            eqr.right = node.getRight();
            eqr.left = node.getLeft();
            node.unlock();
            if (curRight != null && !curRight.equals(eqr.right)) {
                throw new RightNodeMismatch(eqr.right);
            }
            if (snode.sgmode == SGMode.DELETING) {
                // removeKey()を実行中だが，level0のDDLLノードはまだ削除されていない場合，
                // execQueryは実行しない．
                logger.debug("invokeExecQuery: ignore deleting node {}", snode);
                return eqr;
            }
            if (!doAction) {
                return eqr;
            }
        } finally {
            rtUnlockR();
        }
        boolean firsttime = false;
        synchronized (snode.queryHistory) {
            if (!snode.queryHistory.contains(qid)) {
                qid.timestamp = System.currentTimeMillis();
                snode.queryHistory.add(qid);
                firsttime = true;
            }
        }
        if (firsttime) {
            try {
                RemoteValue<?> l = execQuery(rawkey, query);
                eqr.key = l;
            } catch (Throwable e) {
                // handle a RuntimeException or Error
                RemoteValue<Object> rval = new RemoteValue<Object>(peerId, e);
                eqr.key = rval;
            }
        }
        // 再送の場合は eqr.key = null
        logger.debug("invokeExecQuery returns {}", eqr);
        return eqr;
    }

    /**
     * Range Query実行時に呼び出されるメソッド．
     * 
     * @param key the key.
     * @param query the query.
     */
    RemoteValue<?> execQuery(Comparable<?> key, Object query) {
        logger.trace("ENTRY:");
        try {
            logger.debug("execQuery: key={}, query={}", key, query);
            if (execQueryCallback == null) {
                return new RemoteValue<Object>(peerId, key);
            }
            return execQueryCallback.sgExecQuery(key, query);
        } finally {
            logger.trace("EXIT:");
        }
    }

    /*
     * Implementation of scalable range queries.
     * multi-range forwardingを実装しているが，論文通りではない．
     */

    /**
     * Perform a range query.
     * <p>
     * this procedure tries to send a query message to all the nodes within the
     * specified ranges.  this method is asynchronous; this method returns
     * immediately and results are returned asynchronously via
     * ReturnSet.
     * <p> 
     * if some results could not be obtained within {@link #RQ_RETRANS_PERIOD},
     * the originating node retransmits a range query message that covers the
     * missing ranges until it exceeds the timeout.
     *
     * @param ranges    set of query ranges
     * @param query     the object passed to the nodes in the query range
     * @param opts      the options.
     * @return ReturnSet to obtain query results
     */
    public FutureQueue<?> scalableRangeQuery(Collection<? extends Range<?>> ranges,
            Object query, TransOptions opts) {
        if (ranges.size() == 0) {
            return FutureQueue.emptyQueue();
        }
        RQReturn<E> rqRet = rqStartRawRange(ranges, query, 
                        RQ_RETRANS_PERIOD, opts, null);
        return rqRet != null ? rqRet.fq : null;
        // try {
        // Collection<RemoteValue<?>> ret = rqRet.get(timeout);
        // } finally {
        // rqDeleteRQReturn(rqRet);
        // }
    }

    /**
     * perform a range query (internal).
     * 
     * @param ovChain
     * @param ranges
     *            the ranges for the range query
     * @param qCond
     * @param query
     * @param timeout
     *            time to give up (in msec)
     * @param retransPeriod
     *            retransmission period (in msec)
     * @param scalableReturn
     *            if true, range query results are aggregated in intermediate
     *            nodes and obtained in O(log n) hops. if false, results are
     *            sent directly to the querying node, which results O(n)
     *            messages.
     * @param allLinks
     *            all links to split the ranges. if null, the return value of
     *            {@link #getAllLinks(boolean)} is used.
     * @return RQReturn
     */
    private RQReturn<E> rqStartRawRange(
            Collection<? extends Range<?>> ranges, Object query,
            int retransPeriod, TransOptions opts,
            NavigableMap<DdllKey, Link> allLinks) {
        // convert ranges of Comparable<?> into ranges of <DdllKey>.
        Collection<Range<DdllKey>> subRanges = new ArrayList<Range<DdllKey>>();
        for (Range<? extends Comparable<?>> range : ranges) {
            Range<DdllKey> keyRange =
                    new Range<DdllKey>(new DdllKey(range.from, range.fromInclusive
                            ? UniqId.MINUS_INFINITY : UniqId.PLUS_INFINITY), false,
                            new DdllKey(range.to, range.toInclusive
                                    ? UniqId.PLUS_INFINITY : UniqId.MINUS_INFINITY),
                            false);
            subRanges.add(keyRange);
        }
        return rqStartKeyRange(subRanges, query, 
                retransPeriod, opts, allLinks);
    }

    private RQReturn<E> rqStartKeyRange(
            Collection<Range<DdllKey>> ranges, Object query,
            int retransPeriod, TransOptions opts,
            NavigableMap<DdllKey, Link> allLinks) {
        QueryId qid = new QueryId(peerId, rand.nextLong());
        RQMessage<E> msg =
                RQMessage.newRQMessage4Root(sgmf, ranges, qid, 
                        query, (int)TransOptions.timeout(opts) + RQ_EXPIRATION_GRACE, opts);
        rqDisseminate(msg, allLinks);
        // schedule retransmission in the root node
        if (msg.rqRet != null) {
            msg.rqRet.scheduleTask(timer, retransPeriod);
        }
        return msg.rqRet;
    }

    void rqDisseminate(RQMessage<E> msg) {
        rqDisseminate(msg, null);
    }

    /* 
     * レンジクエリの考え方:
     *
     * 10----------------->60------------->100
     * 10--------->40----->60----->80----->100
     * 10->20->30->40->50->60->70->80->90->100
     * 
     * Node 10 から [30, 70] で range query した場合:
     * 
     * 10の動作:
     * 40に RQMessage [30, 60),
     * 60に RQMessage [60, 70]を送信 
     * 
     * 40 ([30, 60) を受信している) の動作:
     * 30に RQMessage [30, 40),
     * 50に RQMessage [50, 60)を送信
     * 
     * 左端の範囲は注意が必要である． 
     * 30が受信した [30, 40) は，実際は ((30, -infinity), (40, +infinity))]
     * と解釈する．((-30, -infinity), (30, ???)) の範囲が残るため，
     * (-30, -infinity)の左ノード(ここでは20)にクエリを転送する．
     * 20と，20のLevel 0の右リンク(30)によって 
     * ((-30, -infinity), (30, ???)) の範囲はカバーされるため，この範囲は処理済み
     * として消去する．
     */
    void rqDisseminate(RQMessage<E> msg, NavigableMap<DdllKey, Link> allLinks) {
        final String h = "rqDiss(" + peerId + ", " + toStringShort() + ")";
        logger.debug("{}: msg = {}", h, msg);
        msg.addTrace(h);
        logger.debug("{}: trace = {}", h, msg.trace);

        if (allLinks == null) {
            rtLockR();
            allLinks = getAllLinks(false);
            rtUnlockR();
        } else {
            // store allLinks for retransmission
            msg.cachedAllLinks = allLinks;
        }
        if (allLinks.isEmpty()) {
            // 挿入が完了していないノードにクエリが転送された．
            // （強制終了してすぐに再挿入した場合も，そのことを知らない
            // ノードからクエリが転送される可能性がある）．
            // ここでは単にクエリを無視することにする．
            logger.warn("routing table is empty!: {}", this);
            // In this case, msg.rqRet is null
            return;
        }

        /*
         * split ranges into subranges and assign a delegate node for them.
         * also aggregate each subranges by destination peerIds.
         */
        @SuppressWarnings("rawtypes")
        StrictMap<UniqId, List<Range<DdllKey>>> map =
                new StrictMap<UniqId, List<Range<DdllKey>>>(new HashMap());
        StrictMap<UniqId, Link> idMap =
                new StrictMap<UniqId, Link>(new HashMap<UniqId, Link>());
        List<DdllKeyRange<RemoteValue<?>>> rvals =
                new ArrayList<DdllKeyRange<RemoteValue<?>>>();
        for (Range<DdllKey> subRange : msg.subRanges) {
            List<DdllKeyRange<Link>> subsubRanges =
                    rqSplit(subRange, allLinks, msg.failedLinks, rvals);
            if (subsubRanges == null) {
                continue;
            }

            for (DdllKeyRange<Link> kr : subsubRanges) {
                UniqId pid =
                        (kr.aux == FIXLEFT ? FIXPEERID : kr.aux.key.getUniqId());
                List<Range<DdllKey>> list = map.get(pid);
                if (list == null) {
                    list = new ArrayList<Range<DdllKey>>();
                    map.put(pid, list);
                    idMap.put(pid, kr.aux);
                }
                list.add(kr.range);
            }
        }
        logger.debug("{}: aggregated: {}", h, map);

        /*
         * prepare RQReturn for catching results from children
         */
        if (msg.rqRet == null) {
            msg.rqRet = new RQReturn<E>(this, msg, (msg.isRoot ? 0 : msg.expire),
                            msg.isRoot);
            if (!msg.isDirectReturn && !msg.isRoot) {
                // note that in the root node, retransmission timer is
                // scheduled by the caller (ugly!).
                timer.schedule(msg.rqRet, RQ_FLUSH_PERIOD, RQ_FLUSH_PERIOD);
            }
        }
        RQReturn<E> rqRet = msg.rqRet;
        rqRet.updateHops(msg.hops);

        /*
         * send the aggregated requests to children.
         * also gather failed ranges that should be retransmit.
         */
        List<Range<DdllKey>> failedRanges = new ArrayList<Range<DdllKey>>();
        synchronized (rqRet) {
            for (Map.Entry<UniqId, List<Range<DdllKey>>> ent : map.entrySet()) {
                UniqId p = ent.getKey();
                if (p.equals(FIXPEERID)) {
                    failedRanges.addAll(ent.getValue());
                } else if (!p.equals(peerId)) {
                    List<Range<DdllKey>> subRanges = ent.getValue();
                    logger.debug("{}: forward {}, {}", h, p, subRanges);
                    RQMessage<E> m =
                            msg.newChildInstance(subRanges, "rqDiss@"
                                    + peerId + " to " + p);
                    Link l = idMap.get(p);
                    rqRet.childMsgs.put(l, m);
                    m.send(idMap.get(p));
                }
            }
        }
        // N----------C
        // N------B---C
        // N-->A--B---C
        // RQ [r1)[r2)
        //    [--r3--)
        // A, Bが故障している場合，failedRanges には r1 と r2 が入る．
        // これをマージする(r3)．この処理は必須ではない．
        failedRanges = RangeUtils.concatAdjacentRanges(failedRanges);
        logger.debug(h + ": merged failedRanges = " + failedRanges);

        /*
         * fix the local routing table
         */
        if (!msg.failedLinks.isEmpty()) {
            rtLockR();
            try {
                for (SGNode<E> sgnode : keyHash.values()) {
                    for (Link link : msg.failedLinks) {
                        sgnode.fixLeftLinks(link, msg.failedLinks, msg,
                                failedRanges);
                    }
                }
            } finally {
                rtUnlockR();
            }
        }

        /*
         * 自ノードがexecQueryを実行すべき範囲を matched に格納する．
         * 各subrange s に対し，SGNode nのキーがsに含まれているならば，
         * matched に (n, [n, n.right[0]]) を格納する．
         * ただし，sの右端がn.right[0]をはみ出る場合は (n, [n, sの右端]) を格納する． 
         */
        @SuppressWarnings("rawtypes")
        StrictMap<SGNode<E>, Range<DdllKey>> matched =
                new StrictMap<SGNode<E>, Range<DdllKey>>(new HashMap());
        rtLockR();
        try {
            for (SGNode<E> n : keyHash.values()) {
                Tile t = n.getTile(0);
                if (t == null || t.mode != LvState.INSERTED) {
                    continue;
                }
                Link right = n.getRightAtLevel0();
                for (Range<DdllKey> subRange : msg.subRanges) {
                    if (subRange.contains(n.key)) {
                        DdllKey rightKey = right.key;
                        // THINK!
                        if (n.key.compareTo(rightKey) > 0) {
                            // n's right node at level 0 is the leftmost node
                            rightKey = subRange.to;
                        }
                        // 本来，最後の引数は false (開区間) であるべきだが，そうすると
                        // skip graphにキーが1つしか存在しない場合，
                        // n.key == rightKey となり，Rangeのinstanceが生成できない
                        // ため，最後の引数は true としている．
                        // また，このケースは，
                        // org.piax.gtrans.sg.RQReturn.addRemoteValue(RemoteValue<?>,
                        // Range<DdllKey>)
                        // で特例として扱っている (isSingleton()) のあたり．
                        Range<DdllKey> range =
                                new Range<DdllKey>(n.key, true, rightKey, true);
                        matched.put(n, range);
                    }
                }
            }
        } finally {
            rtUnlockR();
        }

        /*
         * execute the execQuery at local node
         */
        logger.debug("{}: matched: {}", h, matched);
        for (Map.Entry<SGNode<E>, Range<DdllKey>> ent : matched.entrySet()) {
            SGNode<E> n = ent.getKey();
            Range<DdllKey> range = ent.getValue();
            RemoteValue<?> rval;
            if (QUERY_INSERT_POINT_SPECIAL.equals(msg.query)) {
                // use RemoteValue's option field to store an InsertPoint
                rval = new RemoteValue<Object>(peerId);
                rval.setOption(new InsertPoint(n.getMyLinkAtLevel0(), n
                        .getRightAtLevel0()));
            } else {
                boolean firsttime = false;
                synchronized (n.queryHistory) {
                    if (!n.queryHistory.contains(msg.qid)) {
                        msg.qid.timestamp = System.currentTimeMillis();
                        n.queryHistory.add(msg.qid);
                        firsttime = true;
                    }
                }
                if (!firsttime) {
                    logger.debug("{}: not firsttime", h);
                    continue;
                }
                rval = execQuery(n.rawkey, msg.query);
                logger.debug("{}: execQuery returns: {}", h, rval);
            }
            DdllKeyRange<RemoteValue<?>> er =
                    new DdllKeyRange<RemoteValue<?>>(rval, range);
            rvals.add(er);
        }

        /*
         * store the results of execQuery to rqRet.
         * note that addRemoteValue() may send a reply internally.
         */
        synchronized (rqRet) {
            for (DdllKeyRange<RemoteValue<?>> er : rvals) {
                rqRet.addRemoteValue(er.aux, er.range);
            }
        }
        if (msg.isDirectReturn && !msg.isRoot) {
            rqRet.flush();
            rqRet.dispose();
        }
    }

    /**
     * Split a range into subranges, by the keys in allLinks. Also assign an
     * appropriate remote delegate node for each subrange.
     * 
     * @param range0    the range to be split
     * @param allLinks
     * @param rvals
     * @return subranges
     */
    private List<DdllKeyRange<Link>> rqSplit(final Range<DdllKey> range0,
            final NavigableMap<DdllKey, Link> allLinks,
            Collection<Link> failedLinks, List<DdllKeyRange<RemoteValue<?>>> rvals) {
        String h = "rqSplit(" + peerId + ", " + toStringShort() + ")";

        if (failedLinks == null) {
            failedLinks = Collections.emptySet();
        }
        /*
         * failedLinkを作成した時点と、ここに到達した時は、状況が異なるので
         * 自身がfailedLinkに含まれることはありうる
         */
        for (Iterator<Link> fi = failedLinks.iterator();fi.hasNext();) {
            Link f = fi.next();
            // 自身ならば削除する
            if (f.key.getUniqId().equals(new UniqId(peerId))) {
                failedLinks.remove(f);
            }
        }

        // Range       [---------------] に対し，
        //          N-----A (L0)
        // のように，L0において自ノード(N)と右ノード(A)の間にrangeの左端が入る場合，rangeを
        // 以下のように縮小する．
        // Range ......   [------------]
        // Shrunk      [--)
        // 縮小された区間(Shrunk)には，値が存在しない．これを示すため，ダミーの値nullを
        // rvalsに追加する．
        Range<DdllKey> range = range0;
        rtLockR();
        try {
            for (SGNode<E> node : keyHash.values()) {
                /*if (node.sgmode != SGMode.INSERTED) {
                    continue;
                }*/
                Tile t = node.getTile(0);
                if (t == null || t.mode != LvState.INSERTED) {
                    continue;
                }
                Link right = node.getRightAtLevel0();
                logger.debug("{}, node = {}, range = {}, right = {}",
                        h, node, range, right);
                assert right != null;
                if (!range.contains(node.key)) {
                    // add the information about the shrunk area to the rvals.
                    // this information is essential for the requesting peer to
                    // determine whether all results have been received.
                    Range<DdllKey> removed =
                            RangeUtils.removedRange(range, node.key, right.key);
                    if (removed == null) {
                        continue;
                    }
                    // add a dummy value for the shrunk range
                    RemoteValue<Object> rv =
                            new RemoteValue<Object>(peerId);
                    InsertPoint insp =
                            new InsertPoint(node.getMyLinkAtLevel0(), right);
                    rv.setOption(insp);
                    DdllKeyRange<RemoteValue<?>> kr =
                            new DdllKeyRange<RemoteValue<?>>(rv, removed);
                    logger.debug("{}: dummy rval {}", h, kr);
                    rvals.add(kr);
                    // shrink the range
                    range = RangeUtils.retainRange(range, node.key, right.key);
                    logger.debug("{}: retain = {}", h, range);
                    if (range == null) {
                        return null;
                    }
                }
            }
        } finally {
            rtUnlockR();
        }
        if (range0 != range) {
            logger.debug("{}: shrunk, {} => {}", h, range0, range);
        }

        // Split the given range into subranges by all the keys in my routing
        // table and failedLinks
        //
        // Range [------------------]
        // Keys _____x____x_____x____
        // Result [--)[---)[----)[---]

        // extAllLinks = allLinks + failedLinks
        final NavigableMap<DdllKey, Link> extAllLinks =
                new ConcurrentSkipListMap<DdllKey, Link>(allLinks);
        for (Link l : failedLinks) {
            extAllLinks.put(l.key, l);
        }
        List<DdllKeyRange<Link>> ranges = DdllKeyRange.split(range, extAllLinks);

        // XXX: THINK!
        if (false) {
            DdllKeyRange<Link> firstRange = ranges.get(0);
            if (firstRange.aux == null) {
                for (Link l : failedLinks) {
                    if (l.key.compareTo(firstRange.range.from) == 0) {
                        logger.debug("fill in first entry's delegate from failedlinks");
                        firstRange.aux = l;
                        break;
                    }
                }
            }
        }

        /*
         * extAllLinksで分割された ranges に対して，delegateするノードを決める．
         * 
         * 基本的には，区間 [Ni, N(i+1)) の担当ノードはNiとするが，故障ノードを考慮する
         * と複雑になる．DDLLの特性上，リンクは右ノードから修復する必要がある．
         *
         * RQ区間を分割した，[N1, N2)[N2, N3)...[N(k-1), Nk) を考える．
         * ただし，N2..N(k-1)は故障ノード．Nkは通常ノード．
         * また，N1は，対応するノードが存在する場合としない場合がある．
         * 
         * Case1: N1が通常ノード => N1 が [N1, N2) 担当
         * Case2: N1が故障ノード =>
         *      Case 2-1: Nkが自ノードの場合:
         *          N1..N(k-1)の少なくとも1つを経路表に含んでいる場合 => FIXLEFT
         *            N1..N(k-1)のすべてを経路表に含んでいない場合 = すべて通知された
         *            故障ノードということ．
         *          含んでいない場合(i.e., ranges = [N1, Nk))，N1の左側でも最も
         *          右のノードX．Xが故障している場合は FIXLEFT
         *      Case 2-2: Nkが自ノードでない場合，Nkより右側で最も左の生きているノードY
         * Case3: N1がnull(担当ノード不明) =>
         *      Case 3-1: Nkが自ノードの場合:
         *          N2..N(k-1)の少なくとも1つを経路表に含んでいる場合 => FIXLEFT
         *          含んでいないならば(i.e., ranges = [N1, Nk))，N1の左側で最も
         *          右のノードX．Xが故障している場合は FIXLEFT
         *      Case 3-2: Nkが自ノードでない場合，Nkより右側で最も左の生きているノードY
         *      
         * 経路がループする可能性について:
         * 
         * ....Y     [N1 ...Nk)..
         * (ただしNkに一致するノードは存在しない)
         *           
         *       <--RQ-->
         * S ... [X ... )...U...Y
         * 
         * ・XとUが故障している場合を想定．
         * ・SはYに転送 (修復する必要があるので)
         * ・Yは，RQ範囲の担当ノードが自ノードであるため，RQ範囲の左端より左側で最も右のノード
         *   Sに転送．（無限ループ）
         * 
         * Yは，Uのように，故障ノードがRQの範囲外の場合でも修復する必要がある．
         * (Yが左リンクを修復すると，YとSが結ばれるため，SがRQ範囲にデータがないことを報告
         * できる)
         * このため，Yの左リンクが故障している場合，無条件で FIXLEFT とする．
         * TODO: とすると，上のアルゴリズムはもう少し単純に記述できそう．
         */
        // 単純にするためにrangesの最後にダミーの [Y, ?) (sentinel) を入れる
        {
            DdllKeyRange<Link> l = ranges.get(ranges.size() - 1);
            DdllKey key = l.range.to;
            Map.Entry<DdllKey, Link> ent = allLinks.ceilingEntry(key);
            if (ent == null) {
                ent = allLinks.firstEntry();
            }
            while (true) {
                if (!failedLinks.contains(ent.getValue())) {
                    break;
                }
                ent = allLinks.higherEntry(ent.getKey());
                if (ent == null) {
                    ent = allLinks.firstEntry();
                }
                assert ent != null;
            }
            // add a sentinel
            ranges.add(new DdllKeyRange<Link>(ent.getValue(), null));
        }

        logger.debug("{}: allLinks = {}", h, allLinks.values());
        logger.debug("{}: extAllLinks = {}", h, extAllLinks.values());
        logger.debug("{}: failedLinks = {}", h, failedLinks);
        logger.debug("{}: ranges = {}", h, ranges);

        List<DdllKeyRange<Link>> ranges2 = new ArrayList<DdllKeyRange<Link>>();
        List<DdllKeyRange<Link>> carryovers = new ArrayList<DdllKeyRange<Link>>();
        for (int i = 0; i < ranges.size(); i++) {
            DdllKeyRange<Link> r = ranges.get(i);
            if (r.aux == null || failedLinks.contains(r.aux)) {
                carryovers.add(r);
            } else {
                if (!carryovers.isEmpty()) {
                    logger.debug("carryovers = {}", carryovers);
                    DdllKeyRange<Link> missingRange = null;
                    for (DdllKeyRange<Link> kr : carryovers) {
                        if (missingRange == null) {
                            missingRange = kr;
                        } else {
                            missingRange = missingRange.concatenate(kr, false);
                        }
                    }

                    if (r.aux.key.getUniqId().equals(new UniqId(peerId))) {
                        boolean knownNodeFailure = false;
                        for (DdllKeyRange<Link> kr : carryovers) {
                            if (kr.aux == null) {
                                continue;
                            }
                            for (DdllKey k : allLinks.keySet()) {
                                if (k.compareTo(kr.aux.key) == 0) {
                                    knownNodeFailure = true;
                                    break;
                                }
                            }
                        }
                        boolean immedLeftFailure = false;
                        if (!knownNodeFailure) {
                            //
                            Map.Entry<DdllKey, Link> ent =
                                    allLinks.lowerEntry(r.aux.key);
                            if (ent == null) {
                                ent = allLinks.lastEntry();
                            }
                            if (failedLinks.contains(ent.getValue())) {
                                immedLeftFailure = true;
                            }
                        }
                        logger.debug("knownNodeFailure = {}, immedLeftFailure = {}",
                                knownNodeFailure, immedLeftFailure);
                        if (knownNodeFailure || immedLeftFailure) {
                            missingRange.aux = FIXLEFT;
                        } else {
                            try {
                                Link left =
                                        findLeftLink(missingRange.range.from);
                                if (failedLinks.contains(left)) {
                                    missingRange.aux = FIXLEFT;
                                } else {
                                    missingRange.aux = left;
                                }
                                /*BestLink bl;
                                bl =
                                        findClosestLocal(
                                                missingRange.range.from, false);
                                // ここで bl のノードが failedLinks に含まれていたら?
                                // RQ = [A---)[B---)[C---)
                                // A.aux = null, B = myself
                                // BのL0の左リンクはAより左を指しているか，あるいは
                                // failedLinksに含まれている．
                                // <---------B (L0)
                                if (failedLinks.contains(bl.link)) {
                                    missingRange.aux = FIXLEFT;
                                } else {
                                    missingRange.aux = bl.link;
                                }*/
                            } catch (UnavailableException e) {
                                logger.error("", e);
                                // XXX: THINK
                            }
                        }
                    } else {
                        missingRange.aux = r.aux;
                    }
                    ranges2.add(missingRange);
                    carryovers.clear();
                }
                if (i < ranges.size() - 1) { // avoid adding the sentinel
                    ranges2.add(r);
                }
            }
        }
        logger.debug("{}: results = {}", h, ranges2);
        return ranges2;
    }

    /**
     * called from {@link RQMessage#onReceivingReply(SkipGraph, SGReplyMessage)}
     * when a reply message is received.
     * 
     * @param rq the range query return.
     * @param sender the query sender.
     * @param vals the values.
     * @param hops the hops.
     */
    void rqSetReturnValue(RQReturn<E> rq, PeerId sender,
            Collection<DdllKeyRange<RemoteValue<?>>> vals, int hops) {
        String h = "rqSetReturnValue(" + peerId + ", " + toStringShort() + ")";
        logger.debug("{}, sender = {}, rq = {}, rvals = {}, hops = {}",
                h, sender, rq, vals, hops);
        boolean rc = rq.confirmResponseFromChildNode(sender);
        if (!rc && !rq.parentMsg.isDirectReturn) {
            // it is normal because we might receive multiple responses from a
            // single node due to flushing.
            logger.debug("{}: {} is not contained in childMsgs of {}",
                    h, sender, rq);
        }
        rq.incrementRcvCount();
        rq.updateHops(hops);
        for (DdllKeyRange<RemoteValue<?>> er : vals) {
            rq.addRemoteValue(er.aux, er.range);
        }
        logger.debug("{}: rq = {}", h, rq);
    }

    /*
     * End of scalable range queries
     */
    
    /*
     * Instant Fix Propagation Memo:
     * 
     * L3 A=======1=======>E
     *        B--------------------->F#########G
     *     
     * L2 A---B*****2*****E#########>F
     * L1 A---B***3***D<2=E
     * L0 A---B*4*C<3=D---E
     * 
     * AがBの故障を検出した場合のメッセージフロー．
     * 
     * 凡例:
     *   ***: 修復
     *   ===: 再送するRange Query Requestの流れ
     *   ###: propageteRightwardの流れ
     *   
     * TODO: failedLinksに含まれるノードが他にもキーを保持している場合，failedLinksに
     * 追加するべき (multi-key対応)
     */

    /**
     * fix routing tables of local and remote nodes.
     * <p>
     * this method is called by {@link RQMessage#onTimeOut(SkipGraph)} when node
     * failure is detected (i.e., Ack timeout).
     * 
     * @param failedLinks
     *            the failed links
     * @param parentMsg
     *            the range query message that was sent to the failed node
     * @param failedRanges
     *            the ranges that were sent to the failed nodes.
     */
    void fixRoutingTables(Collection<Link> failedLinks, RQMessage<E> parentMsg,
            Collection<Range<DdllKey>> failedRanges) {
        String h = "fixRoutingTable@" + myLocator;
        logger.debug("{}: failedLinks = {}, failedRanges = {}, parentMsg = {}",
                h, failedLinks, failedRanges, parentMsg);

        if (parentMsg.rqRet == null) {
            parentMsg.rqRet =
                    new RQReturn<E>(this, parentMsg, parentMsg.expire, false);
            parentMsg.sgmf = sgmf;
            logger.debug("{}: create new RQReturn: {}", h, parentMsg.rqRet);
        }
        RQMessage<E> msg =
                parentMsg.newChildInstance(failedRanges, "fixRT@" + peerId);
        msg.addFailedLinks(failedLinks);
        rqDisseminate(msg);
    }

    /**
     * one-way RPCs
     */

    @Override
    public void requestMsgReceived(SGRequestMessage<E> sgMessage) {
        sgmf.requestMsgReceived(sgMessage);
    }

    @Override
    public void replyMsgReceived(SGReplyMessage<E> sgReplyMessage) {
        sgmf.replyMsgReceived(sgReplyMessage);
    }

    @Override
    public void ackReceived(int msgId) {
        sgmf.ackReceived(msgId);
    }

    @Override
    public void fixAndPropagateSingle(Comparable<?> target, Link failedLink,
            Collection<Link> failedLinks, DdllKey rLimit) {
        SGNode<E> snode = keyHash.get(target);
        if (snode == null) {
            logger.debug("no such key {}", target);
            return;
        }
        snode.fixAndPropagateRight(failedLink, failedLinks, rLimit);
    }
    
    int getHeight(Comparable<?> key) {
        SGNode s = keyHash.get(key);
        if (s != null) {
            rtLockR();
            try {
                return s.getInsertedHeight();
            } finally {
                rtUnlockR();
            }
        }
        return 0;
    }

    Link getLocal(Comparable<?> key) {
        return keyHash.get(key).getMyLinkAtLevel0();
    }
    
    @SuppressWarnings("unchecked")
    Link[] getNeighbors(Comparable<?> key, boolean right, int level) {
        ArrayList<Link> ret = new ArrayList<Link>();
        if (key == null) {
            keyHash.values().forEach((snode)->{
                SGNode s = snode;
                ret.addAll(s.getAllLinks(true));
            });
        }
        else {
            SGNode s = keyHash.get(key);
            if (right) {
                ret.add(s.getRightLink(level));
            }
            else {
                ret.add(s.getLeftLink(level));
            }
        }
        return ret.toArray(new Link[0]);
    }
    
    /**
     * A class used for passing a link to the closest node.
     */
    @SuppressWarnings("serial")
    public static class BestLink implements Serializable {
        // link to the closest node
        final Link link;
        // the right link of the closest node (if known).
        final Link rightLink;
        // true if the node pointed by `link' is the immediate predecessor of
        // the specified key
        final boolean isImmedPred;

        public BestLink(Link link, Link rightLink, boolean isImmedPred) {
            this.link = link;
            this.rightLink = rightLink;
            this.isImmedPred = isImmedPred;
        }

        @Override
        public String toString() {
            return "BestLink(link=" + link + ", right=" + rightLink + ", "
                    + isImmedPred + ")";
        }
    }

    /**
     * QueryId is used by the range query algorithm to uniquely identify a query
     * message.
     */
    @SuppressWarnings("serial")
    static class QueryId implements Serializable {
        final PeerId sourcePeer;
        final long id;
        long timestamp; // filled in by each receiver peer

        private QueryId(PeerId sourcePeer, long id) {
            this.sourcePeer = sourcePeer;
            this.id = id;
        }

        @Override
        public boolean equals(Object obj) {
            QueryId o = (QueryId) obj;
            return sourcePeer.equals(o.sourcePeer) && id == o.id;
        }

        @Override
        public int hashCode() {
            return sourcePeer.hashCode() ^ (int) id;
        }

        @Override
        public String toString() {
            return "qid[" + sourcePeer + "-" + id + "]";
        }
    }

    /**
     * This is a class used as a return type for
     * SkipGraph#getSGNodeInfo
     */
    @SuppressWarnings("serial")
    public static class SGNodeInfo implements Serializable {
        final Link me; // sender node
        final Link left; // its left node (at level+1)
        final Link right; // its right node (at level+0)

        public SGNodeInfo(Link me, Link left, Link right) {
            this.me = me;
            this.left = left;
            this.right = right;
        }

        // SGNodeInfoで考えられるパターン:
        // ・mv mismatch -> proceed right
        // ・mv match && inserted -> found inserted
        // ・mv match && proceed OK -> proceed right
        // ・mv match && proceed NG -> confliction
        // encounters a node whose membership vector does not match, or
        // insertion conflicted but I win
        public boolean proceedRight() {
            return right != null;
        }

        // encounters a inserted node whose membership vector matches
        public boolean foundInserted() {
            return me != null;
        }

        @Override
        public String toString() {
            return "SGNodeInfo[me=" + me + ", l=" + left + ", r=" + right + "]";
        }
    }

    /**
     * an execQuery return value used by (non-scalable) range queries
     */
    @SuppressWarnings("serial")
    public static class ExecQueryReturn implements Serializable {
        public RemoteValue<?> key;
        public Link left;
        public Link right;

        @Override
        public String toString() {
            return "ExecQueryReturn[" + key + ", left=" + left + ", right="
                    + right + "]";
        }
    }

    @SuppressWarnings("serial")
    public static class RightNodeMismatch extends Exception {
        public Link curRight; // actual right link

        public RightNodeMismatch(Link curRight) {
            this.curRight = curRight;
        }

        @Override
        public String toString() {
            return "RightNodeMismatch(curRight=" + curRight + ")";
        }
    }
}
