/*
 * SGNode.java - SkipGraph nodes.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: SGNode.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.gtrans.ov.sg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ddll.Node.InsertionResult;
import org.piax.gtrans.ov.ddll.NodeObserver;
import org.piax.gtrans.ov.sg.SkipGraph.BestLink;
import org.piax.gtrans.ov.sg.SkipGraph.LvState;
import org.piax.gtrans.ov.sg.SkipGraph.QueryId;
import org.piax.gtrans.ov.sg.SkipGraph.SGNodeInfo;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a node in a skip graph.
 */
public class SGNode<E extends Endpoint> implements NodeObserver {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(SGNode.class);

    private static int MAX_RETRY = 10;
    private static int INSERTION_RETRY_PERIOD = 100;
    private static int MAX_INSERTION_RETRY_PERIOD = 15000;
    private static int TRAVERSAL_RETRY_PERIOD = 1000;
    
    /** mode of SGNode */
    static enum SGMode {
        /** not inserted */
        OUT,
        /** searching for a location to be inserted */
        TRAVERSING,
        /** waiting for next traversing */
        WAITING,
        /** inserted */
        INSERTED,
        /** deleting */
        DELETING
    };

    final SkipGraph<E> sg;
    /*
     * 経路表
     * 書き換える際は rtLockW()，読み込む際は rtLockR() を呼んでから処理．
     * 1物理ノード内に複数のSGNode(keyに対応)が存在する可能性がある．
     * Multi-Key SGでは，これら全てのSGNodeは，同じメンバシップベクトルを共有する．
     * このため，単純に実装すると経路表の高さが最大(=メンバシップベクトルの桁数)に達してしまう．
     * これを避けるために，各ノードの経路表では，同一物理ノード上のSGNodeしか存在しない
     * レベルまでしか管理しないことにする．
     * あるSGNodeで経路表の高さを増加させる必要がある場合は，INSERTEDなノード全てで増加させる．
     */
    final ArrayList<Tile> table = new ArrayList<Tile>();
    final Comparable<?> rawkey;
    final DdllKey key;
    private final MembershipVector mv;
    SGMode sgmode = SGMode.OUT;

    /**
     * # of nodes traversed so far for inserting this node.
     * this value is used for determining which node <em>wins</em> when
     * multiple nodes are being inserted simultaneously.
     */
    private int traversed = 0;

    final Set<QueryId> queryHistory = new HashSet<QueryId>();

    /**
     * create a SGNode instance.
     * 
     * @param sg        the SkipGraph instance that manages this node
     * @param mv        membership vector
     * @param rawkey    the key
     */
    public SGNode(SkipGraph<E> sg, MembershipVector mv, Comparable<?> rawkey) {
        this.sg = sg;
        this.mv = mv;
        this.rawkey = rawkey;
        this.key = new DdllKey(rawkey, new UniqId(sg.peerId));
        
        /* register instance for debug */
//        synchronized (SGNode.class) {
//            sgnodes.add(this);
//        }
    }
    
    /* instances for debug */
//    private static ArrayList<SGNode<?>> sgnodes = new ArrayList<SGNode<?>>();

    /* dump nodes for debug */
//    static public synchronized void dump() {
//        FileWriter fw = null;
//        String fn = "SGNode-dump-" + System.currentTimeMillis() + ".txt";
//        logger.warn("SGNode dump to{}", fn);
//        try {
//            fw = new FileWriter(fn);
//        } catch (IOException e) {
//            logger.error("file open", e);
//            return;
//        }
//        try {
//            for (SGNode<?> sgnode : sgnodes) {
//                fw.write("sgnode " + sgnode.rawkey + "\n");
//                sgnode.rtLockW();
//                for (Tile tile : sgnode.table) {
//                    fw.write(tile.toString() + "\n");
//                }
//                sgnode.rtUnlockW();
//            }
//        } catch (IOException e) {
//            logger.error("", e);
//        } finally {
//            try {
//                fw.close();
//            } catch (IOException e) {
//                logger.error("file close", e);
//            }
//        }
//    }

    /*
     * reader write locks
     */
    private void rtLockR() {
        sg.rtLockR();
    }

    private void rtUnlockR() {
        sg.rtUnlockR();
    }

    private void rtLockW() {
        sg.rtLockW();
    }

    private void rtUnlockW() {
        sg.rtUnlockW();
    }

    /*
     * ノードxがlevel lで挿入中(n.insert()実行中)に，別のノードvが，level l-1で
     * xを発見し，level lでxに接続しようとする可能性があることに注意．
     */
    
    /**
     * insert a key into a skip graph.
     * 
     * @param seed Skip graphに既に挿入済みのノード
     * @return 成功したらtrue
     * @throws UnavailableException seed nodeにkeyが存在しない
     * @throws IOException seed nodeとの通信でエラー
     */
    boolean addKey(E seed) throws UnavailableException, IOException {
        logger.trace("ENTRY:");
        logger.debug("insert key {} seed:{}", rawkey, seed);
        if (rawkey == null) {
            throw new IllegalArgumentException("null key specified");
        }
        rtLockW();
        assert sgmode == SGMode.OUT;
        sgmode = SGMode.TRAVERSING;
        rtUnlockW();

        long start = System.currentTimeMillis();

        int retryAll = 0;   // retry counter for all levels
        long retryWait = INSERTION_RETRY_PERIOD;
        int retryCounter = 0;
        // start inserting from level 0 
        int l = 0;
        /*
         * sgmode = INSERTING
         * while (true) {
         *   level=l-1 で走査し，level=l での挿入場所 p を見つける．
         *   if (circulated) break;
         *   if (成功) {
         *      p を使って level l で挿入
         *      if (成功) {
         *       l++
         *       continue
         *      }
         *    }
         *   sgmode = WAITING
         *   乱数時間待つ
         *   sgmode = INSERTING
         * }
         */
        outer: while (true) {
            Tile t = getDdllNode(l, LvState.INSERTING);
            Node n = t.node;
            long start0 = System.currentTimeMillis();
            try {
                InsertPoint p = getContact(seed, l);
                long end0 = System.currentTimeMillis();
                long travTime = end0 - start0;
                if (p == null) {    // circulated
                    // getContact終了時に rtLock を開放しているため，
                    // その後 getSGNodeInfo の呼び出しによって自ノードが負けている可能性
                    // がある．念のためにこれをチェックする．
                    rtLockW();
                    if (sgmode != SGMode.TRAVERSING) {
                        rtUnlockW();
                        throw new ConflictException("conflict");
                    }
                    n.insertAsInitialNode();
                    t.mode = LvState.INSERTED;
                    /*
                     * 構造が壊れているなどの何らかの理由で、
                     * ノードのレベルが異なっている場合は
                     * 同じレベルになるまで、続行する
                     */
                    if ((l+1) < sg.getHeight()) {
                        logger.warn("{}:cont l={} height={}",key,l,sg.getHeight());
                        l++;
                        retryCounter = 0;
                        rtUnlockW();
                        continue;
                    }
                    //rtLockW();
                    break;
                }
                logger.debug("{} is inserting at level {} between {}", rawkey,
                        l, p);
                assert t.mode == LvState.INSERTING;
                InsertionResult insres = null;
                if (t.node.isBetween(p.left.key, p.right.key)) {
                    insres = n.insert(p);
                } else {
                    // p.right に到達したときに，最初はなかったp.leftが挿入されていた場合，
                    // p.leftから挿入場所を探して挿入する．
                    logger.debug("{}: not ordered: {}", rawkey, p);
                    insres = n.insert(p.left, 1);
                }
                if (insres.success) {
                    t.mode = LvState.INSERTED;
                    logger.debug("{} is inserted at level {}, retry = {},"
                            + " travTime = {}msec: {}", rawkey, l,
                            retryCounter, travTime, n);
                    logger.debug(sg.toString());
                    l++;
                    retryCounter = 0;
                    if (n.getMyLink().addr.equals(n.getRight().addr)
                            && n.getMyLink().addr.equals(n.getLeft().addr)) {
                        logger.debug("leftaddr == myaddr == rightaddr,"
                                + " l={}, h={}", l, sg.getHeight());
                        // 高さを揃えるために，他のノードと同じレベルまで挿入を継続する
                        // SkipGraph#adjustHeight(int)と排他制御するためにロック獲得．
                        rtLockW();
                        if (l >= sg.getHeight()) {
                            // WriteLockを保持している間に(atomicに)経路表の高さを調べて，
                            // sgmode=INSERTEDにしなければならないことに注意．
                            break outer;
                        }
                        rtUnlockW();
                    }
                    continue;
                }
                logger.debug("{}: DDLL insertion failed at level {}, retry.",
                        rawkey, l);
                rtLockW();
                sgmode = SGMode.WAITING;
                rtUnlockW();
            } catch (ConflictException e) {
                long end0 = System.currentTimeMillis();
                long travTime = end0 - start0;
                logger.debug("addKey: got {}, travTime = {}msec, waiting...",
                        e, travTime);
                // fall through
            } catch (TemporaryIOException e) {
                sgmode = SGMode.WAITING;
                long end0 = System.currentTimeMillis();
                long travTime = end0 - start0;
                logger.debug("addKey: got {}, travTime = {}msec, waiting...",
                        e, travTime);
                // fall through
            } catch (UnavailableException e) {
                logger.debug("", e);
                if (l == 0) {
                    if (seed == null) {
                        // insert myself as the initial node!
                        n.insertAsInitialNode();
                        t.mode = LvState.INSERTED;
                        rtLockW();
                        sgmode = SGMode.INSERTED;
                        rtUnlockW();
                        logger.debug("inserted as the initial node");
                        return true;
                    }
                    // the seed peer has no key!
                    throw e;
                }
                throw new Error("addKey: got UnavailableException while l != 0");
            }
            rtLockR();
            //sgmode = SGMode.WAITING;
            assert sgmode == SGMode.WAITING; 
            rtUnlockR();
            retryAll++;
            retryCounter++;
            try {
            	/*
            	 * backoff retry-wait time
            	 */
                long waitTime = retryWait+(long)(retryWait*Math.random());
                retryWait <<= 1;
                if (retryWait > MAX_INSERTION_RETRY_PERIOD) {
                	retryWait = INSERTION_RETRY_PERIOD;
                  }
                logger.debug("{}: {}th retry after {}ms", rawkey, retryCounter,
                		waitTime);
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {}
            logger.debug("{}: waiting done at level {}", rawkey, l);
            // 再度level=lでの挿入場所を検索する
        }
        assert sg.rtlock.writeLock().isHeldByCurrentThread();
        sgmode = SGMode.INSERTED;
        rtUnlockW();
        long end = System.currentTimeMillis();
        logger.debug("addKey({}) took {} msec, total retry count = {}",
                rawkey, end-start, retryAll);
        return true;
    }
    
    /**
     * Traverse the skip graph to find the contact nodes (the immediate left
     * and right node) for inserting this node at level l.
     * If another node calls getSGNodeInfo() on this node while traversing,
     * ConflictionException may happen. 
     * 
     * @param seed   a seed node (only used for level 0)
     * @param l      level
     * @return a contact node.  null if circulated.
     * @throws NoSuchKeyException
     * @throws IOException  
     * @throws ConflictException
     */
    private InsertPoint getContact(E seed, int l)
            throws UnavailableException, IOException, ConflictException {
        if (l == 0) {
            InsertPoint p;
            try {
                // accurate = false because an inaccurate result can be 
                // detected later. 
                p = sg.find(seed, new DdllKey(rawkey, new UniqId(sg.peerId)), false);
                logger.debug("getContact: {} is between {} at level0", rawkey, p);
                return p;
            } catch (UnavailableException e) {
                logger.debug("", e);
                throw e;
            } catch (IOException e) {
                logger.debug("", e);
                throw e;
            }
        } else {
            InsertPoint p;
            try {
                // find a peer whose membership vector matches at least l bits
                p = findMatchingNode(l);
                logger.debug("getContact: {} returns {} at level {}", rawkey, p, l);
                return p;
            } catch (ConflictException e) {
                logger.debug("", e);
                throw e;
            } catch (NoSuchKeyException e) {
                logger.debug("", e);
                throw new Error("should not happen", e);
            } catch (IOException e) {
                logger.debug("", e);
                throw new Error("should not happen", e);
            }
        }
    }

    boolean removeKey() {
        // once sgmode is set to DELETING, table height will not be increased.
        // therefore, it is safe to call table.size() without locking later in
        // this method.
        rtLockW();
        if (sgmode != SGMode.INSERTED) {
            rtUnlockW();
            return false;
        }
        sgmode = SGMode.DELETING;
        rtUnlockW();
        for (int i = table.size() - 1; i >= 0; i--) {
            Node n = getTile(i).node;
            logger.debug("removeKey: {}, level {}", n, i);
            boolean rc = n.delete(MAX_RETRY);
            if (!rc) {
                logger.info("removeKey: ddll node deletion fails");
            }
            rtLockW();
            try {
                table.remove(i);
            } finally {
                rtUnlockW();
            }
        }
        rtLockW();
        sgmode = SGMode.OUT;
        rtUnlockW();
        return true;
    }

    /**
     * SGNodeの経路表の中から，指定されたkeyより小さい，最も大きいLinkを探し，
     * BestLinkのインスタンスとして返す．
     * 自ノードをnとする．レベル0で n.key <= key < n.right.key が成立するとき，
     * {@link BestLink#isImmedPred} が true になる． 
     * 完全に挿入処理が終わっていない状態でも呼び出される可能性があることに注意すること．
     *
     * @param key キー
     * @param accurate accurate or not.
     * @return 最も近いLinkを表すBestLinkのインスタンス．
     *         まだ経路表の準備ができていない，もしくは自ノードが削除済みの場合は null を返す．
     */
    @Deprecated
    BestLink findClosestLocal(DdllKey key, boolean accurate) {
        logger.debug("findClosestLocal: key {}", key);
        assert sg.rtlock.getReadHoldCount() > 0;
        if (sgmode == SGMode.OUT) {
            throw new Error("findClosestLocal is called while sgmode==OUT");
        }
        BestLink best = null;
        int l;
        for (l = table.size() - 1; l >= 0; l--) {
            Tile t = getTile(l);
            if (t == null) {
                continue;
            }
            final Node n = t.node;
            n.lock();
            try {
                if (n.getMode() != Node.Mode.IN
                        && n.getMode() != Node.Mode.DELWAIT) {
                    continue;
                }
                final Link leftLink = n.getLeft();
                final Link rightLink = n.getRight();
                final Link myLink = n.getMyLink();
                // [best---n.right---key] ==> replace best with n.right 
                if (best == null
                        || Node.isOrdered(best.link.key, rightLink.key, key)) {
                    best = new BestLink(rightLink, null, false);
                }
                // [best---n.left---key] ==> replace best with n.left
                if (Node.isOrdered(best.link.key, leftLink.key, key)) {
                    best = new BestLink(leftLink, null, false);
                }
                // [best---n---key] ==> replace best with n
                if (Node.isOrdered(best.link.key, myLink.key, key)) {
                    best = new BestLink(myLink, null, false);
                }
                if (Node.isOrdered(best.link.key, rightLink.key, key)) {
                    best = new BestLink(rightLink, null, false);
                }
                if (l == 0) {
                    // [n---key---n.right] at level 0 ==> n is the best
                    if (Node.isOrdered(myLink.key, key, rightLink.key)) {
                        best = new BestLink(myLink, rightLink, true);
                    } else if (Node.isOrdered(leftLink.key, key, myLink.key)
                            && !Node.isOrdered(leftLink.key, best.link.key,
                                    myLink.key)
                            // check because a left node may be inaccurate 
                    ) {
                        // [n.left---key---n] at level 0 ==> n.left is the best
                        best = new BestLink(leftLink, myLink, !accurate);
                    }
                }
            } finally {
                n.unlock();
            }
        }
        logger.debug("best {}", best);
        return best;
    }

    private InsertPoint findMatchingNode(int level)
    throws NoSuchKeyException, ConflictException, IOException {
        rtLockW();
        sgmode = SGMode.TRAVERSING;
        traversed = 0;
        rtUnlockW();
        try {
            InsertPoint p = findMatchingNode0(level);
            logger.debug("FMN: finished after traversing {} nodes", traversed);
            return p;
        } catch (ConflictException e) {
            rtLockW();
            sgmode = SGMode.WAITING;
            rtUnlockW();
            throw e;
        }
    }

    /**
     * Find a node whose membership vector matches at least `level' digits,
     * by traversing the (level - 1) level link rightward.
     * 
     * @param n     the node to send a query.
     *              should be null when starting a traversal.
     * @param level level
     * @return peer location.  if we could not find a such node (i.e., 
     *         reached to myself), return null.
     * @throws IOException
     * @throws NoSuchKeyException
     * @throws ConflictException found that another node is simultaneously 
     *                           traversing.
     */
    @SuppressWarnings("unchecked")
    private InsertPoint findMatchingNode0(int level)
    throws NoSuchKeyException, ConflictException, IOException {
        assert level > 0;
        // 自ノードの右ノードから始める．
        // 自ノードの右ノードが fail している場合，右リンクがいずれ書き換わるため，
        // 繰り返しごとに mine.getRight() を呼ぶ必要がある．
        // XXX: condition wait?
        Node mine = getDdllNode(level - 1, null).node;
        Link n = mine.getRight();
        while (true) {
            if (sgmode != SGMode.TRAVERSING) {
                logger.debug("FMN: seems conflicted. stop scanning");
                throw new ConflictException("i lost");
            }
            mine = getDdllNode(level - 1, null).node;
            logger.debug("FMN: n={}, mine={}, level={}", n, mine, level);
            if (mine.getMyLink().equals(n)) {
                // message circulated!
                logger.debug("findMatchingNode: circulated");
                return null;
            }
            SkipGraphIf<E> stub = sg.getStub((E) n.addr, SkipGraph.RPC_TIMEOUT);
            SGNodeInfo inf;
            try {
                logger.debug("findMatchingNode: calling getSGNodeInfo on {}", n);
                inf = stub.getSGNodeInfo(n.key.getPrimaryKey(), level - 1, mv,
                        traversed);
                if (inf == null) {
                    // nが削除中で対応するレベルが消えている場合．
                    // iterative routingなので，そのようなノードをたどる可能性はある．
                    logger.debug("findMatchingNode: inf==null, retry");
                    throw new ConflictException("findMatchingNode: inf==null");
                } else {
                    logger.debug("findMatchingNode: got {}", inf);
                }
            } catch (NoSuchKeyException e) {
                logger.debug("", e);
                // inf.right seems to be failed
                // let's retry from node n, expecting the right node of n
                // will soon be fixed!
                logger.debug("findMatchingNode: retry after "
                        + TRAVERSAL_RETRY_PERIOD + " msec");
                throw new ConflictException(e.toString());
            } catch (RPCException e) {
                logger.debug("RPCEx:", e.getCause());
                // communication error!
                throw new ConflictException("RPC to " + n + ", " + e.getCause());
            }
            if (inf.proceedRight()) {
                logger.debug("{}: move right {}", rawkey, inf);
            } else if (inf.foundInserted()) {
                logger.debug(
                        "findMatchingNode: found {} at level {} for key {}",
                        inf.me, level, rawkey);
                return new InsertPoint(inf.left, inf.me);
            } else {
                throw new ConflictException("conflicted with node " + n
                        + " and I lost");
            }
            // 右のノードへ
            rtLockW();
            traversed++;
            rtUnlockW();
            if (Node.isOrdered(n.key,key,inf.right.key)
                    && key.compareTo(inf.right.key) != 0) {
                // 自ノードを経由せずに一周している
                logger.warn("FMN: circulated without visiting myself");
                // これでも、新しいリングを作成する。
                return null;
            }
            n = inf.right;
            logger.debug("findMatchingNode: move to the right");
        }
    }

    /**
     * 指定した level に関連する{@link SGNodeInfo}を返す．
     * <p>
     * メンバシップベクタの上位ビットが同一であるノードを検索するために
     * {@link #findMatchingNode} からRPCで呼ばれる．
     * <p>
     * 自ノードも findMatchingNode実行中の場合，いままでにトラバースしたノード数
     * traversed が大きい側が勝つ．自ノードが負けた場合，findMatchingNode
     * を実行中のスレッドに通知する．
     * <p>
     * 返り値の SGNodeInfo には以下の情報が格納される．
     * <pre> 
     * ・me: (level + 1) の自分自身へのリンク
     * ・left: (level + 1) の左リンク
     * ・right: (level + 0) の右リンク．null if the caller cannot traverse
     *      rightward further.
     * </pre>
     * <p>
     * 自ノードが削除中の場合，指定されたlevelは削除済みの可能性がある．このときはnullを返す．
     * <p>
     * 自ノードが挿入中で level + 1 Linkがまだない場合，me = null を返す．
     * (level + 0は存在するので，メンバシップベクトルが一致しなければcaller側はrightを使って
     * 検索を継続できることに注意)
     * 
     * @param level     level
     * @param mv        the membership vector of the remote (caller) node
     * @param nTraversed the number of traversed node of the remote node
     * @return the SGNodeInfo
     */
    public SGNodeInfo getSGNodeInfo(int level, MembershipVector mv, int nTraversed) {
        logger.debug("getSGNodeInfo(this={}, level={}, nTraversed={})",
                this, level, nTraversed);
        rtLockW();
        try {
            Tile tile = getTile(level);
            if (tile == null) {
                logger.info("getSGNodeInfo: no such level ({})", level);
                return null;
            }
            Node n = tile.node;
            n.lock();
            Link r;
            try {
                if (n.getMode() == Node.Mode.OUT) {
                    logger.info("getSGNodeInfo: no such level ({})", level);
                    return null;
                }
                r = n.getRight();
            } finally {
                n.unlock();
            }

            // check if membership vector matches
            if (this.mv.commonPrefixLen(mv) < level + 1) {
                logger.debug("getSGNodeInfo: non-matching node");
                return new SGNodeInfo(null, null, r);
            }

            Link left = null;
            Link myLink = null;
            Link right = null;
            Tile uplevel = getTile(level + 1);
            if (uplevel == null && sgmode == SGMode.INSERTED) {
                // if we have finished inserting
                uplevel = getDdllNode(level + 1, LvState.INSERTED);
            }
            if (uplevel != null) {
                Node x = uplevel.node;
                // if we have (level + 1) level, return it
                x.lock();
                switch (uplevel.mode) {
                case INSERTED:
                    left = x.getLeft();
                    myLink = x.getMyLink();
                    break;
                case INSERTING:
                    switch (sgmode) {
                    case WAITING:
                        // when we are waiting in addKey()...
                        right = r;
                        break;
                    case TRAVERSING:
                        // if two nodes conflict, the node that have traversed
                        // more nodes wins.
                        boolean proceedOK = nTraversed > traversed;
                        logger.debug("getSGNodeInfo: conflicts. proceedOK = {}",
                                proceedOK);
                        if (proceedOK) {
                            right = r;
                            // this node lost.  let the thread executing
                            // findMatchingNode() know about this.
                            // note that rtLockW is taken here.
                            sgmode = SGMode.WAITING;
                        }
                        break;
                    default:
                        logger.info("getSGNodeInfo: sgmode = {}", sgmode);
                        break;
                    }
                    break;
                default:
                    logger.info("getSGNodeInfo: uplevel.mode = {}", uplevel.mode);
                    right = r;
                }
                x.unlock();
            } else {
                // if we do not have (level + 1) level,...
                // right = null means confliction
            }
            return new SGNodeInfo(myLink, left, right);
        } finally {
            rtUnlockW();
        }
    }

    public Tile getDdllNode(int level, LvState newstate) {
        rtLockW();
        try {
            if (level < table.size()) {
                return table.get(level); // getTile(level)
            }
            // 現在の経路表の高さよりも高いレベルが指定された場合，同一物理ノード内の
            // 全SGNodeの経路表を高くする．
            assert newstate != null;
            sg.adjustHeight(level);
            if (newstate == LvState.INSERTED && sgmode != SGMode.TRAVERSING) {
                // 挿入済みノードの経路表の高さを増加させる場合はここで終了
                // (経路表の高さは adjustHeight() で増加済み)
                return table.get(level);
            }
            Node n = createDdllNode(level);
            if (newstate == LvState.INSERTED && sgmode == SGMode.TRAVERSING) {
                // 初期ノードを挿入する場合
                n.insertAsInitialNode();
            }
            Tile t = new Tile(n, newstate);
            table.add(level, t);
            return t;
        } finally {
            rtUnlockW();
        }
    }

    Node createDdllNode(int level) {
        // we use membership vector as appData
        Node n = sg.manager.createNode(rawkey, ("L" + level), this, sg.mv);
        if (level == 0) {
            n.setCheckPeriod(SkipGraph.DDLL_CHECK_PERIOD_L0);
        } else {
            n.setCheckPeriod(SkipGraph.DDLL_CHECK_PERIOD_L1);
        }
        return n;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        Comparable<?> k = rawkey;
        buf.append("key=" + k);
        if (k != null) {
            buf.append(" (" + k.getClass().getCanonicalName() + ")\n");
        } else {
            buf.append("\n");
        }
        buf.append("sgmode=" + sgmode + "\n");
        if (table != null) {
            // ここで排他制御をいれるとデッドロックする可能性がある．
            //rtLockR();
            try {
                for (int i = table.size() - 1; i >= 0; i--) {
                    //buf.append(" lv" + i + ": " + getTile(i) + "\n");
                    try {
                        buf.append(" lv" + i + ": " + table.get(i) + "\n");
                    } catch (IndexOutOfBoundsException e) {
                        // 
                    }
                }
            } finally {
                //rtUnlockR();
            }
        }
        return buf.toString();
    }

    // XXX: make public just for testing.  
    // should be private.
    public Tile getTile(int level) {
        rtLockR();
        try {
            if (table.size() <= level) {
                return null;
            }
            return table.get(level);
        } finally {
            rtUnlockR();
        }
    }

    /**
     * 経路表でINSERTEDなエントリの高さを得る．
     * 
     * @return 高さ
     */
    int getInsertedHeight() {
        int l;
        assert sg.rtlock.getReadHoldCount() > 0;
        for (l = 0; l < table.size(); l++) {
            Tile t = table.get(l);
            if (t.mode != LvState.INSERTED) {
                break;
            }
        }
        return l;
    }

    /**
     * get my Link at level 0
     * 
     * @return  Link at level 0.  null if not inserted.
     */
    Link getMyLinkAtLevel0() {
        Tile t = getTile(0);
        if (t == null) {
            return null;
        }
        return t.node.getMyLink();
    }

    /*
     * For Scalable Range Queries
     */
    Link getRightAtLevel0() {
        return table.get(0).node.getRight(); // get cloned one
    }

    /**
     * get all links from all levels.
     */
    Set<Link> getAllLinks(boolean insertedOnly) {
        Set<Link> set = new HashSet<Link>();
        for (int i = table.size() - 1; i >= 0; i--) {
            Tile t = table.get(i);
            if (t.mode == LvState.INSERTED ||
                    (!insertedOnly && t.mode == LvState.INSERTING)) {
                Link left = t.node.getLeft();
                // request left non-null to exclude a node that has no valid
                // left and right links
                if (left != null) {
                    Link right = t.node.getRight();
                    Link me = t.node.getMyLink();
                    set.add(left);
                    if (right != null) {
                        set.add(right);
                    }
                    if (me != null) {
                        set.add(me);
                    }
                }
            }
        }
        return set;
    }

    /**
     * get all left links from all levels.
     */
    Set<Link> getAllLeftLinks() {
        Set<Link> set = new HashSet<Link>();
        for (int i = getInsertedHeight() - 1; i >= 0; i--) {
            Tile t = getTile(i);
            set.add(t.node.getLeft());
            set.add(t.node.getMyLink());
        }
        return set;
    }
    
    /**
     * get left link from specified level.
     */
    Link getLeftLink(int level) {
        Tile t = getTile(level);
        return t.node.getLeft();
    }
    
    /**
     * get right link from specified level.
     */
    Link getRightLink(int level) {
        Tile t = getTile(level);
        return t.node.getRight();
    }
    
    /**
     * get all right links from all levels.
     */
    Set<Link> getAllRightLinks() {
        Set<Link> set = new HashSet<Link>();
        for (int i = getInsertedHeight() - 1; i >= 0; i--) {
            Tile t = getTile(i);
            set.add(t.node.getRight());
            set.add(t.node.getMyLink());
        }
        return set;
    }

    /**
     * failedLink が故障しているものとして修復する．
     * また，failedLink の情報を右ノードに伝搬する．
     * 
     * @param failedLink the failed link.
     * @param failedLinks the failed links.
     * @param parentMsg the parent message.
     * @param failedRanges the failed ranges. 
     */
    void fixLeftLinks(Link failedLink, Collection<Link> failedLinks,
            RQMessage<E> parentMsg, Collection<Range<DdllKey>> failedRanges) {
        String h = "fixLeftLinks@" + sg.myLocator;
        logger.debug("{}: failedLink = {}, failedLinks = {}, failedRanges = {}",
                h, failedLink, failedLinks, failedRanges);
        RQMessage<E> msg = null;
        if (parentMsg != null && !failedRanges.isEmpty()) {
            msg = parentMsg.newChildInstance(failedRanges, "fixLeftLinks@"
                    + key);
        }
        rtLockR();
        try {
            for (int level = getInsertedHeight() - 1; level >= 0; level--) {
                Tile tile = getTile(level);
                Link left = tile.node.getLeft();
                if (left != null && failedLink.compareTo(left) == 0) {
                    if (level == 0 && parentMsg != null) {
                        if (!failedRanges.isEmpty()) {
                            logger.debug("{}: fix at L0, childmsg = {}", h, msg);
                            msg.prepareReceivingReply();
                            tile.node.startfix(failedLinks, msg);
                            msg = null;
                        } else {
                            logger.debug("{}: fix at L0, no childmsg", h);
                            tile.node.startFix(failedLinks);
                        }
                        // propagate fix request rightward
                        propagateRightward(left, failedLinks, key);
                    } else {
                        logger.debug("{}: fix at L{}", h, level);
                        tile.node.startFix(failedLinks);
                    }
                }
            }
        } finally {
            rtUnlockR();
        }
        if (msg != null) { // means the failed link is not found
            logger.info("{}: failed link not found: {}", h, msg);
        }
    }

    /**
     * failedLink が故障しているものとして修復する．
     * また，failedLink の情報を右ノードに伝搬する．
     * 
     * @param failedLink the failed link.
     * @param failedLinks the failed links.
     * @param rLimit the limit of key.
     */
    void fixAndPropagateRight(Link failedLink, Collection<Link> failedLinks,
            DdllKey rLimit) {
        String h = "fixAndPropagateRight@" + sg.myLocator;
        logger.debug("{}: failedLink = {}, failedLinks = {}, rLimit = {}",
                h, failedLink, failedLinks, rLimit);
        rtLockR();
        try {
            for (int level = getInsertedHeight() - 1; level >= 0; level--) {
                Tile tile = getTile(level);
                if (tile.mode != LvState.INSERTED) {
                    continue;
                }
                Link left = tile.node.getLeft();
                if (failedLink.compareTo(left) == 0) {
                    logger.debug("{}: fix at L{}", h, level);
                    tile.node.startFix(failedLinks);
                }
            }
            propagateRightward(failedLink, failedLinks, rLimit);
        } finally {
            rtUnlockR();
        }
    }

    /**
     * 故障ノード(failedLink)の情報を右方向のノードに通知する．
     * failedLinksはすべての故障ノードで，failedLinkを含む．
     * rLimit != null ならば，右ノードが rLimit に等しいか超えるところで通知を停止する．
     * 
     * @param failedLink    failed node
     * @param failedLinks   set of failed nodes
     * @param rLimit
     */
    @SuppressWarnings("unchecked")
    private void propagateRightward(Link failedLink,
            Collection<Link> failedLinks, DdllKey rLimit) {
        String h = "propagateRightward@" + sg.myLocator;
        MembershipVector mvec = (MembershipVector) failedLink.key.appData;
        int level = mvec.commonPrefixLen(sg.mv);
        Tile tile = getTile(level);
        if (tile == null) {
            logger.debug("{}: forward right stops at L{} (tile is null)",
                    h, level);
            return;
        }
        Link right = tile.node.getRight();
        if (right == null) {
            logger.debug("{}: forward right stops at L{} (right is null)",
                    h, level);
            return;
        }
        if (!failedLinks.contains(right) && right.key.compareTo(key) != 0) {
            if (rLimit != null && key.compareTo(rLimit) != 0
                    && Node.isOrdered(key, rLimit, right.key)) {
                logger.debug("rLimit restricted");
                return;
            }
            try {
                SkipGraphIf<E> stub = sg.getStub((E) right.addr);
                logger.debug("{}: forward right {} at L{}", h, right, level);
                stub.fixAndPropagateSingle(right.key.getPrimaryKey(), failedLink,
                        failedLinks, rLimit);
            } catch (RPCException e) {
                logger.info("", e);
            }
        } else {
            logger.debug("{}: forward right stops at L{}", h, level);
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void onRightNodeChange(Link prevRight, Link newRight, Object payload) {
        if (payload == null) {
            return;
        }
        logger.debug("{}: rightNodeChanged from {} to {}, {}", key, prevRight,
                newRight, payload);
        if (payload instanceof RQMessage) {
            RQMessage<E> msg = (RQMessage<E>) payload;
            msg.sgmf = sg.sgmf; // XXX: !!!

            // msg.subRanges から，[key, newRight) に重なるものを抽出
            List<Range<DdllKey>> subRanges = new ArrayList<Range<DdllKey>>();
            for (Range<DdllKey> r : msg.subRanges) {
                //     key---------newRight)
                //           [r---------------)
                // [r---------)
                if (RangeUtils.hasCommon(r, key, newRight.key)) {
                    subRanges.add(r);
                }
            }
            logger.debug("{}: rightNodeChanged: {} => {}", key, msg.subRanges,
                    subRanges);
            RQMessage<E> msg2 = msg.newInstanceSubrangesChanged(subRanges);
            sg.rqDisseminate(msg2);
        } else {
            logger.warn("{}: rightNodeChanged received illgal payload {}", key,
                    payload);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void payloadNotSent(Object payload) {
        if (payload instanceof RQMessage) {
            RQMessage<E> msg = (RQMessage<E>) payload;
            logger.debug("{}: payloadNotSent: {}", key, msg);
            sg.rqDisseminate(msg);
        } else {
            logger.warn("{}: payloadNotSent received illgal payload {}", key,
                    payload);
        }
    }

    @Override
    public boolean onNodeFailure(Collection<Link> failedLinks) {
        logger.debug("onNodeFailure: {}", failedLinks);
        for (Link flink : failedLinks) {
            fixAndPropagateRight(flink, failedLinks, key);
        }
        logger.debug("onNodeFailure finished");
        return false; // suppress DDLL left link fix
    }

    /**
     * a class representing single level of the routing table of a skip graph.
     */
    public static class Tile {
        // XXX: make public just for testing.
        // should be package scope.
        /** a DDLL node */
        public Node node;
        /** the state of the level */
        LvState mode;
    
        public Tile(Node node, LvState mode) {
            this.node = node;
            this.mode = mode;
        }

        @Override
        public String toString() {
            return "[" + mode + ", " + node + "]";
        }
    }

    /**
     * an exception thrown when detecting another node is trying to insert.
     */
    @SuppressWarnings("serial")
    static class ConflictException extends Exception {
        public ConflictException(String msg) {
            super(msg);
        }
    }

	@Override
	public List<Link> suppplyLeftCandidatesForFix() {
		// TODO Auto-generated method stub
		return null;
	}
}
