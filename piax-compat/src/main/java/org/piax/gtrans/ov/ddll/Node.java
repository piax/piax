/*
 * Node.java - Node implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Node.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.piax.ayame.ov.ddll.LinkSeq;
import org.piax.common.DdllKey;
import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.ov.Link;
import org.piax.util.KeyComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a DDLL node.
 * <p>
 * this class contains an implementation of DDLL protocol.
 */
public class Node {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    static String THREAD_NAME_PREFIX = "ddll-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    private static final int FIXREQNO = 0;
    public static int DEFAULT_TIMEOUT = 5000;
    public static int SETR_TIMEOUT = DEFAULT_TIMEOUT;
    public static int DELETE_OP_TIMEOUT = DEFAULT_TIMEOUT;
    public static int SEARCH_OP_TIMEOUT = DEFAULT_TIMEOUT;

    /**
     * timeout for a response of GetStat message in msec. if the left node does
     * not respond within this period, the left node is considered failed.
     *  
     * TODO: change timeout adaptively
     */
    public static int GETSTAT_OP_TIMEOUT = DEFAULT_TIMEOUT;
    public static final int INS_DEL_RETRY_INTERVAL_BASE = 200;

    /**
     * minimum interval of fixing
     * To avoid rush of fixing, make minimum interval between fixing.
     */
    public static int MIN_FIX_INTERVAL = 500;

    /**
     * fix処理を定期実行するための基準となる周期 (msec)
     */
    public static int DEFAULT_CHECK_PERIOD = 10000;

    /**
     * DDLL node state
     */
    public enum Mode {
        OUT, INS, INSWAIT, IN, DEL, DELWAIT, GRACE,
    };

    /**
     * the state of the link repair procedure.
     */
    static enum FIX_STATE {
        /** idle */
        WAITING,
        /** checking left links (sending getStat message) */
        CHECKING,
        /** fixing left links (sending SetR message, insertion type) */
        FIXING_BOTH,
        /** fixing left links (sending SetR message, non-insertion type) */
        FIXING_LEFTONLY,
    }

    private static KeyComparator keyComp = KeyComparator.getInstance();

    final NodeManager manager;
    final NodeObserver observer;
    boolean isOnline = true;
    /** a Link to myself */
    final Link me;
    final DdllKey key;
    volatile Mode mode = Mode.OUT;
    volatile Link left;
    volatile LinkSeq lNum;
    volatile Link right;
    volatile LinkSeq rNum;
    volatile int ref = 0;
    volatile boolean sendLastUnrefL = true;
    // the latest request number of the SetR message
    // (either for insertion or for deletion)
    int currentReqNo;
    boolean expectDelayedResponse = false;

    private final ReentrantReadWriteLock protoLock =
            new ReentrantReadWriteLock();
    private Condition protoCond = protoLock.writeLock().newCondition();

    private final FutureValues futures = new FutureValues();

    private final Timer stabilizeTimer;
    private volatile FIX_STATE fixState = FIX_STATE.WAITING;
    private TimerTask fixTask;
    /** the period for checking the left link */
    private int checkPeriod = DEFAULT_CHECK_PERIOD;

    /** neighbor node set */
    final NeighborSet leftNbrs;

    private final static boolean TOSTRING_EMBED_NBRS = true;

    /**
     * constructor
     * 
     * @param manager A NodeManager that controls this Node instance.
     * @param observer A NodeObserver that monitors particular events occurred
     *            in this instance (maybe null).
     * @param id A string to identify a linked-list. nodes for different
     *            linked-lists should have different IDs.
     * @param key The key for this DDLL node.
     * @param appData Application-specific data that is used as a part of DdllKey
     *            instance but not used for comparison.
     * @param timer A timer instance used for executing periodic tasks.
     */
    Node(NodeManager manager, NodeObserver observer, String id,
            Comparable<?> key, Object appData, Timer timer) {
        this(manager, observer, new DdllKey(key, manager.peerId,
                id, 0, appData), timer);
    }

    /**
     * constructor
     * 
     * @param manager A NodeManager that controls this Node instance.
     * @param observer A NodeObserver that monitors particular events occurred
     *            in this instance (maybe null).
     * @param ddllKey The key for this DDLL node.
     * @param timer A timer instance used for executing periodic tasks.
     */
    Node(NodeManager manager, NodeObserver observer, DdllKey ddllKey,
            Timer timer) {
        this.manager = manager;
        this.observer = observer;
        this.key = ddllKey;
        me = new Link(manager.getLocator(), this.key);
        // setLeft(me);
        setLeftNum(new LinkSeq(0, 0));
        // setRight(me);
        // setRightNum(new LinkSeq(0, 0));
        leftNbrs = new NeighborSet(me, manager);
        stabilizeTimer = timer;
        online();
    }

    /**
     * activate the protocol.
     */
    public void online() {
        // System.out.println(key + " online() is called");
        isOnline = true;
    }

    /**
     * inactivate the protocol.
     */
    public void offline() {
        //System.out.println(key + " offline() is called");
        protoLock.writeLock().lock();
        isOnline = false;
        fixState = FIX_STATE.WAITING;
        if (fixTask != null) {
            fixTask.cancel();
        }
        protoLock.writeLock().unlock();
    }

    public boolean isOnline() {
        // System.out.println(key + " isOnline = " + isOnline);
        return isOnline;
    }

    // XXX: potential dead lock?  Think!
    public void fin() {
        protoLock.writeLock().lock();
        try {
            offline();
            if (this.left != null) {
                manager.monitor.unregisterNode(this.left, this);
            }
        } finally {
            protoLock.writeLock().unlock();
        }
    }

    /*
     * Getters and Setters
     */
    public Link getMyLink() {
        return (Link) me.clone();
    }

    public DdllKey getKey() {
        return key;
    }

    public Mode getMode() {
        return mode;
    }

    protected void setMode(Mode mode) {
        this.mode = mode;
    }

    public void setCheckPeriod(int period) {
        this.checkPeriod = period;
    }

    protected void setLeft(Link left) {
        if (this.left != null) {
            manager.monitor.unregisterNode(this.left, this);
        }
        this.left = left;
        if (left != null) {
            manager.monitor.registerNode(left, this, checkPeriod);
        }
    }

    protected void setLeftNum(LinkSeq lNum) {
        this.lNum = lNum;
    }

    public Link getLeft() {
        protoLock.readLock().lock();
        try {
            if (left == null) {
                return null;
            }
            return (Link) left.clone();
        } finally {
            protoLock.readLock().unlock();
        }
    }

    public Link getRight() {
        protoLock.readLock().lock();
        try {
            if (right == null) {
                return null;
            }
            return (Link) right.clone();
        } finally {
            protoLock.readLock().unlock();
        }
    }

    protected void setRight(Link right) {
        this.right = right;
    }

    protected void setRightNum(LinkSeq rNum) {
        this.rNum = rNum;
    }

    protected void setRef(int r) {
        this.ref = r;
    }

    public List<Link> getNeighborSet() {
        List<Link> lst = new ArrayList<Link>(leftNbrs.getNeighbors());
        return lst;
    }

    @Override
    public String toString() {
        String s =
                "Node[key=" + key + ", left=" + left + ", right=" + right
                        + ", lNum=" + lNum + ", rNum=" + rNum + ", " + mode
                        + ", " + fixState + "]";
        if (TOSTRING_EMBED_NBRS) {
            /*return "Node[key=" + key + ", left=" + left + ", right=" + right
            + ", " + mode + "]" + ": lnbr=" + leftNbrs;*/
            return s + ": lnbr=" + leftNbrs;
        } else {
            return s;
        }
    }

    /**
     * forcibly delete this node without notifying other nodes
     */
    public void reset() {
        protoLock.writeLock().lock();
        try {
            setMode(Mode.OUT);
            setLeft(null);
            setRight(null);
            if (fixTask != null) {
                fixTask.cancel();
            }
            manager.unregisterNode(key);
        } finally {
            protoLock.writeLock().unlock();
        }
    }

    public void lock() {
        protoLock.readLock().lock();
    }

    public void unlock() {
        protoLock.readLock().unlock();
    }

    private NodeManagerIf getStub(Endpoint loc) throws OfflineSendException {
        if (isOnline()) {
            return (NodeManagerIf) manager.getStub(loc);
        }
        throw new OfflineSendException();
    }

    @Deprecated
    public static boolean isOrdered(Comparable<?> a, Comparable<?> b,
            Comparable<?> c) {
        return keyComp.isOrdered(a, b, c);
    }

    @Deprecated
    public static boolean isOrdered(Comparable<?> from, boolean fromInclusive,
            Comparable<?> val, Comparable<?> to, boolean toInclusive) {
        boolean rc = keyComp.isOrdered(from, val, to);
        if (rc) {
            if (keyComp.compare(from, val) == 0) {
                rc = fromInclusive;
            }
        }
        if (rc) {
            if (keyComp.compare(val, to) == 0) {
                rc = toInclusive;
            }
        }
        return rc;
    }

    public boolean isBetween(DdllKey a, DdllKey c) {
        return keyComp.isOrdered(a, key, c);
    }

    /**
     * Insert this node as the initial node. This node becomes the first node in
     * the network.
     */
    public void insertAsInitialNode() {
        logger.trace("ENTRY:");
        setLeft(me);
        //setLeftNum(new LinkSeq(0, 0));
        setRight(me);
        setRightNum(new LinkSeq(0, 0));
        setMode(Mode.IN);
        setRef(1);
        manager.registerNode(this);
        logger.trace("EXIT:");
    }

    public static void initializeConnectedNodes(List<Node> list) {
        logger.trace("ENTRY:");
        logger.debug("List = {}", list);
        for (int i = 0; i < list.size(); i++) {
            Node p = list.get(i);
            Node q = list.get((i + 1) % list.size());
            p.setRight(q.me);
            p.setRightNum(new LinkSeq(0, 0));
            q.setLeft(p.me);
            //q.setLeftNum(new LinkSeq(0, 0));
            p.setMode(Mode.IN);
            p.setRef(1);
            p.manager.registerNode(p);
            // 物理ノードと近隣ノード集合との関係をよく考える必要がある!
        }
        logger.trace("EXIT:");
    }

    /**
     * insert this node to a linked-list by specifying an introducer node, which
     * is an arbitrary node that has been inserted to the linked-list.
     * <p>
     * note that this method may take O(N) steps for finding proper insertion
     * point and thus, should not be used for large linked-list.
     * 
     * @param introducer some node that has been inserted to a linked-list
     * @param maxRetry max retry count
     * @return insertion result
     * @throws IllegalStateException thrown if the node is already inserted
     */
    public InsertionResult insert(Link introducer, int maxRetry)
            throws IllegalStateException {
        if (mode != Mode.OUT) {
            throw new IllegalStateException("already inserted");
        }
        manager.registerNode(this);
        InsertionResult insres = InsertionResult.getFailureInstance();
        try {
            for (int i = 0; i < maxRetry; i++) {
                if (i != 0) {
                    sleepForRetry(i);
                }
                InsertPoint p;
                try {
                    // XXX: magic number 50!
                    p = findInsertPoint(introducer, key, 50);
                } catch (OfflineSendException e) {
                    logger.info(
                            "insert: findInsertPoint failed as offline while "
                                    + "inserting {}", key);
                    return InsertionResult.getFailureInstance();
                }
                if (p == null) {
                    logger.info(
                            "insert: could not find the insertion point for inserting {}, seed={}",
                            key, introducer);
                    // simply return here because if we continue here and if the
                    // introducer has failed, we stuck.
                    return InsertionResult.getFailureInstance();
                }
                insres = insert0(p);
                if (insres.success) {
                    return insres;
                }
                logger.debug("insertion failed for some reason. retry!: {}",
                        this);
            }
        } finally {
            if (insres != null && !insres.success) {
                //manager.unregisterNode(key);
                reset();
            }
        }
        return insres;
    }

    /**
     * insert this node to a linked-list by specifying the immediate left and
     * right nodes.
     * <p>
     * note that this method does not retry insertion when failed.
     * 
     * @param pos the insert point, which contains the immediate left and right
     *            node of this node.
     * @return insertion result
     * @throws IllegalStateException thrown if the node is already inserted
     */
    public InsertionResult insert(InsertPoint pos) throws IllegalStateException {
        if (mode != Mode.OUT) {
            throw new IllegalStateException("already inserted");
        }
        InsertionResult rc = null;
        manager.registerNode(this);
        try {
            rc = insert0(pos);
        } finally {
            if (rc == null || !rc.success) {
                //manager.unregisterNode(key);
                reset();
            }
        }
        return rc;
    }

    /**
     * insert a node between two nodes specified by an {@link InsertPoint}
     * instance.
     * 
     * @param p insertion point
     * @return true if successfully inserted, false otherwise
     */
    protected InsertionResult insert0(InsertPoint p) {
        logger.trace("ENTRY:");
        logger.debug("insert0: InsertPoint={}", p);
        expectDelayedResponse = false;
        try {
            // XXX: この部分は論文で抜けている (k-abe)
            // XXX: Locatorの値が変化する場合，equalsだとまずいだろう．
            if (p.left.equals(me)) {
                logger.warn("it seems that i'm unknowningly inserted!");
                askRemoteNodeToRemoveMe(p.right);
                return InsertionResult.getFailureInstance();
            }
            protoLock.writeLock().lock();
            try {
                setLeft(p.left);
                // because the repair-count in the left link number may be
                // incremented after (previous) unsuccessful insertion,
                // we do not call setLeftNum(new LinkSeq(0, 0)) here.
                setRight(p.right);
                setRightNum(null);
                setMode(Mode.INS);
            } finally {
                protoLock.writeLock().unlock();
            }
            logger.debug("insert0: {} inserts between {}", key, p);
            // send SetR message to the left node
            int reqNo = futures.newFuture();
            currentReqNo = reqNo;
            try {
                NodeManagerIf stub = getStub(left.addr);
                stub.setR(left.key, me, reqNo, me, right, lNum,
                        NodeManagerIf.SETR_TYPE_NORMAL, null);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                logger.error("", e.getCause());
                return InsertionResult.getFailureInstance();
            }

            // wait for setRAck or setRNak
            Link curR = null;
            try {
                Object obj = futures.get(reqNo, SETR_TIMEOUT);
                if (obj instanceof Link) {
                    curR = (Link)obj;
                }
            } catch (TimeoutException e) {
                logger.info("{}: insertion timed-out", key);
            }

            protoLock.writeLock().lock();
            switch (mode) {
            case IN:
                // we have received SetRAck
                protoLock.writeLock().unlock();
                return InsertionResult.getSuccessInstance();
            case OUT:
                // we have received SetRNak
            case INS:
                // time-out waiting for SetRAck or SetRNak.
                // because the left node may have received the SetR message
                // we sent, and currently this node has insufficient information
                // (e.g., no neighbors, no rNum), try to remove this node.

                // before asking the right node to fix, set mode to OUT
                // because getLiveLeftNodeStat() returns a node whose mode
                // is INS.
                setMode(Mode.OUT);
                // also, increment the repair-count of the left link number. 
                setLeftNum(lNum.gnext());
                protoLock.writeLock().unlock();
                askRemoteNodeToRemoveMe(right);
                break;
            default:
                protoLock.writeLock().unlock();
                // empty
            }
            if (curR != null) {
                // DDLL-OPT: we have received a SetRNak message.
                return new InsertionResult(false,
                        new InsertPoint(p.left, curR));
            }
            return InsertionResult.getFailureInstance();
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * ask a remote node to remove myself (me). 
     * @param remote    the remote node to ask.
     */
    private void askRemoteNodeToRemoveMe(Link remote) {
        logger.debug("ask {} to remove me", remote);
        try {
            NodeManagerIf stub = getStub(remote.addr);
            stub.startFix(remote.key, me, true);
        } catch (RPCException e) {
            logger.error("", e.getCause());
        }
    }

    /**
     * delete this node from the linked-list.
     * if the DDLL protocol fails to delete this node after 
     * maxRetry retries, the node is forcibly deleted and returns false.
     * 
     * @param maxRetry max retry
     * @return true if successfully deleted
     * @throws IllegalStateException thrown if node is not inserted
     */
    public boolean delete(int maxRetry) throws IllegalStateException {
        if (mode != Mode.IN) {
            throw new IllegalStateException("already deleted");
        }
        boolean rc = false;
        try {
            for (int i = 0; i < maxRetry; i++) {
                if (i != 0) {
                    sleepForRetry(i);
                }
                logger.debug("trying to delete {}, i={}", this, i);
                if (rc = delete0()) {
                    break;
                    //manager.unregisterNode(key);
                    //manager.monitor.unregisterNode(left, this);
                    //return true;
                }
            }
        } catch (OfflineSendException e) {
            logger.info("delete failed as offline");
        } finally {
            reset();
        }
        return rc;
    }

    /**
     * delete this node from the linked-list.
     * 
     * @return true if successfully deleted
     * @throws OfflineSendException thrown if this node is offline
     */
    protected boolean delete0() throws OfflineSendException {
        logger.trace("ENTRY:");
        protoLock.writeLock().lock();
        try {
            // XXX: 論文のA8, A9では右ノードのみが等しいかをチェックしているが，
            // 左ノードもチェックする．
            // ノードuの左ノードが別のノードaを指しているとき，aはuにSetLを
            // 送ってくる可能性がある．このため，右ノードだけチェックして早々に
            // OUTにしてしまうと，SetLに対応できない．(k-abe)
            if (me.equals(right) && me.equals(left)) {
                reset();
                return true;
            }
            if (mode == Mode.DEL) {
                // retry because timed out
                // InsertPoint p = findLiveLeft();
                // if (p == null) {
                // logger.info("findLiveLeft could not find node");
                // return false;
                // }
                // protoLock.writeLock().lock();
                // left = p.left;
                // lNum = lNum.gnext();
                // protoLock.writeLock().unlock();
            } else {
                setMode(Mode.DEL);
            }

            // send SetR message to the left node
            int reqNo = futures.newFuture();
            currentReqNo = reqNo;
            try {
                NodeManagerIf stub = getStub(left.addr);
                stub.setR(left.key, me, reqNo, right, me, rNum.next(),
                        NodeManagerIf.SETR_TYPE_NORMAL, null);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
                return false;
            }
            logger.debug("waiting, reqNo={}", reqNo);
            protoLock.writeLock().unlock();
            // wait for setRAck or setRNak
            try {
                futures.get(reqNo, SETR_TIMEOUT);
            } catch (TimeoutException e) {
                logger.info("waiting for SetRAck/Nak timed-out: {}", key);
            }
            protoLock.writeLock().lock();
            switch (mode) {
            case OUT: // received a SetRAck and UnrefL
                return true;
            case GRACE: // received a SetRAck
                if (ref == 0) {
                    setMode(Mode.OUT);
                    logger.debug("ref is already 0. send unrefL to {}", left);
                    try {
                        NodeManagerIf stub = getStub(left.addr);
                        stub.unrefL(left.key, me);
                    } catch (RPCException e) {
                        if (e instanceof OfflineSendException) {
                            throw (OfflineSendException) e;
                        } else {
                            logger.error("", e.getCause());
                        }
                    }
                    return true;
                }
                // wait for UnrefL
                break;
            case DELWAIT: // received a SetRNak
                return false;
            case DEL: // timed-out (no SetRAck nor SetRNak)
                try {
                    sendSetL(right, left, rNum.next(), me, false);
                } catch (RPCException e2) {
                    if (e2 instanceof OfflineSendException) {
                        throw (OfflineSendException) e2;
                    } else {
                        logger.error("", e2.getCause());
                    }
                }
                setMode(Mode.GRACE);
                break;
            default:
            }
            // here, mode should be GRACE.  wait for a UnrefL message
            logger.debug("waiting for unrefL");
            try {
                protoCond.await(DELETE_OP_TIMEOUT, TimeUnit.MILLISECONDS);
                logger.debug("condwait done, mode={}", mode);
            } catch (InterruptedException e1) {
                logger.debug("condwait interrupted {}", mode);
            }
            if (mode != Mode.OUT) {
                // unrefL timed-out
                setMode(Mode.OUT);
                // unrefLを左ノードへ送る
                logger.debug("unrefL timed-out. send unrefL to {}", left);
                try {
                    NodeManagerIf stub = getStub(left.addr);
                    stub.unrefL(left.key, me);
                } catch (RPCException e) {
                    if (e instanceof OfflineSendException) {
                        throw (OfflineSendException) e;
                    } else {
                        logger.error("", e.getCause());
                    }
                }
            }
            return true;
        } finally {
            protoLock.writeLock().unlock();
            logger.trace("EXIT:");
        }
    }

    /**
     * insert/deleteのリトライの際の待ち時間を計算し、その間sleepする。
     * <p>
     * n 回目のリトライの際には、 [0, n * INS_DEL_RETRY_INTERVAL_BASE] の範囲の乱数値を待ち時間に使う。
     * 
     * @param n リトライ回数
     */
    private void sleepForRetry(int n) {
        protoLock.writeLock().lock();
        try {
            long t =
                    (long) ((n + Math.random() * 0.1) * INS_DEL_RETRY_INTERVAL_BASE);
            logger.debug("{}th retry after {}ms", n, t);
            protoCond.await(t, TimeUnit.MILLISECONDS); // XXX: Why?
        } catch (InterruptedException ignore) {
        }
        protoLock.writeLock().unlock();
    }

    /**
     * find the immediate left and right nodes of `searchKey', by contacting the
     * specified introducer node.
     * 
     * @param introducer introducer node
     * @param searchKey
     * @param maxHops max hops
     * @return insert point. null on errors.
     * @throws OfflineSendException
     */
    private InsertPoint findInsertPoint(Link introducer, DdllKey searchKey,
            int maxHops) throws OfflineSendException {
        Link next = introducer;
        Link prevKey = null;
        for (int i = 0; i < maxHops; i++) {
            logger.debug("findInsertPoint: [{}]: i = {}, next = {}", key, i,
                    next);
            int reqNo = futures.newFuture();
            try {
                NodeManagerIf stub = getStub(next.addr);
                stub.findNearest(next.key, me, reqNo, searchKey, prevKey);
            } catch (RPCException e) {
                futures.discardFuture(reqNo);
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
                return null;
            }

            try {
                Object result = futures.get(reqNo, SEARCH_OP_TIMEOUT);
                if (result instanceof InsertPoint) {
                    InsertPoint rc = (InsertPoint) result;
                    if (rc.left == null) {
                        // remote node is OUT
                        logger.debug("findInsertPoint: remote node is out: {}",
                                next);
                        return null;
                    }
                    return rc;
                } else if (result instanceof Object[]) {
                    next = (Link) ((Object[]) result)[0];
                    prevKey = (Link) ((Object[]) result)[1];
                } else {
                    logger.error("illegal result");
                    return null;
                }
            } catch (TimeoutException e) {
                logger.warn("{}: no response from {} for findNearest request",
                        this, next);
                return null;
            }
        }
        logger.warn("over max hops");
        return null;
    }

    /*
     * protocol message handlers
     */
    /**
     * SetR message handler.
     * 
     * <pre>
     * pseudo code:
     * (A2)
     * receive SetR(rnew , rcur , rnewnum , incr) from q →
     *   if ((s  ̸= in ∧ s  ̸= lwait) ∨ r  ̸= rcur) then send SetRNak() to q
     * else send SetRAck(rnum) to q; r, rnum, ref := rnew, rnewnum, ref + incr fi
     * 
     * </pre>
     * 
     * @param sender    the sender of this SetR message
     * @param reqNo     request number
     * @param rNew      the new right link number
     * @param rCur      the current right link that this node should have
     * @param rNewNum   the new right link
     * @param type      the type of SetR 
     * @param payload   the optional data transferred along with the SetR
     * message
     */
    public void setR(Link sender, int reqNo, Link rNew, Link rCur,
            LinkSeq rNewNum, int type, Object payload) {
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("receive SetR(rNew={}, rCur={}, rNewNum={}, type={}, payload={})",
                rNew, rCur, rNewNum, type, payload);
        logger.debug("node status {}", this);
        try {
            if (mode == Mode.OUT) {
                logger.warn("setR received while mode is OUT, ignored");
                // 応答を返さない
                return;
            }
            if ((mode != Mode.IN && mode != Mode.DELWAIT)
                    || !right.equals(rCur)) {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    Link r;
                    if (mode != Mode.IN && mode != Mode.DELWAIT) {
                        r = null;
                    } else {
                        r = right;
                    }
                    stub.setRNak(sender.key, me, reqNo, r);
                } catch (Throwable e) {
                    handleRPCException("setR", "setRNak", e);
                }
            } else {
                Link prevRight = right;
                LinkSeq oldNum = rNum;
                setRight(rNew);
                setRightNum(rNewNum);
                if (type != NodeManagerIf.SETR_TYPE_FIX_LEFTONLY) {
                    setRef(ref + 1);
                }
                boolean forInsertion = sender.equals(rNew);

                // adjust leftNbrs
                if (type != NodeManagerIf.SETR_TYPE_NORMAL) {
                    // 修復の場合，右ノードは故障しているはず
                    leftNbrs.removeNode(rCur);
                    leftNbrs.add(rNew);
                } else {
                    if (forInsertion) {
                        leftNbrs.add(rNew);
                    } else {
                        leftNbrs.removeNode(rCur);
                    }
                }
                // compute a neighbor node set to send to the new right node 
                Set<Link> nset = leftNbrs.computeNSForRight(rNew);
                leftNbrs.setPrevRightSet(rNew, nset);
                try {
                    // send a SetRAck message
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setRAck(sender.key, me, reqNo, oldNum, nset);
                } catch (Throwable e) {
                    handleRPCException("setR", "setRAck", e);
                }
                try {
                    // send a SetL message
                    if (forInsertion) {
                        if (type != NodeManagerIf.SETR_TYPE_FIX_LEFTONLY) {
                            NodeManagerIf stub = getStub(prevRight.addr);
                            Set<Link> nset2 =
                                    leftNbrs.computeNSForRight(prevRight);
                            logger.debug("sending SetL nset2={}", nset2);   
                            stub.setL(prevRight.key, rNew, oldNum.next(), me,
                                    nset2);
                        }
                    } else {
                        logger.debug("sending SetL nset={}", nset);
                        NodeManagerIf stub = getStub(rNew.addr);
                        stub.setL(rNew.key, me, rNewNum, prevRight, nset);
                    }
                } catch (Throwable e) {
                    handleRPCException("setR", "setL", e);
                }
                // if this SetR is for node deletion or recovery,
                // send my left neighbors to the new right node
                /*if (!sender.equals(rNew) || incr == 0) {
                    leftNbrs.sendRight(right, key);
                }*/
                if (observer != null) {
                    protoLock.writeLock().unlock();
                    // notify the application
                    observer.onRightNodeChange(rCur, rNew, payload);
                }
            }
        } finally {
            logger.trace("EXIT:");
            if (protoLock.isWriteLockedByCurrentThread()) {
                protoLock.writeLock().unlock();
            }
        }
    }

    /**
     * SetRAck message handler.
     * 
     * <pre>
     * pseudo code:
     * (A3) receive SetRAck(znum) from q →
     *   if (s = jng) then s, rnum, ref := in, znum.next(), 1; send SetL(u, rnum, l) to r
     *     elseif (s = lvg) then s := grace; send SetL(l, rnum.next(), u) to r
     *     elseif (s = fix) then s := in fi
     * </pre>
     * 
     * @param sender the sender of this SetRAck message
     * @param reqNo request number
     * @param zNum the previous right link number of the sender node
     * @param nbrs neighbor node set
     */
    public void setRAck(Link sender, int reqNo, LinkSeq zNum, Set<Link> nbrs) {
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("receive SetRAck(sender={}, reqNo={}, zNum={}, nbrs={}) from {}",
                sender, reqNo, zNum, nbrs, sender);
        try {
            if (!sender.equals(left)) {
                logger.info("sender node is not current left node, mode {}",
                        mode);
            }
            if (reqNo == FIXREQNO) {
                if (fixState == FIX_STATE.FIXING_BOTH
                        || fixState == FIX_STATE.FIXING_LEFTONLY) {
                    logger.info("setRAck received from {}, fixState={}", sender,
                            fixState);
                    if (fixState == FIX_STATE.FIXING_BOTH) {
                        setRightNum(zNum.next());
                        setRef(1);  // XXX: THINK!
                    }
                    fixDone();
                    leftNbrs.set(nbrs);
                    // nbrs do not contain the immediate left node
                    leftNbrs.add(left);
                    leftNbrs.sendRight(key, right, sender.key);
                } else {
                    logger.debug("staled setRAck for recovery. dropped");
                }
                return;
            }
            if (futures.expired(reqNo)) {
                logger.info("reqNo {} has been expired", reqNo);
                return;
            }
            if (reqNo != currentReqNo) {
                logger.debug(
                        "staled setRAck (reqNo={}, current={}). dropped",
                        reqNo, currentReqNo);
                return;
            }
            switch (mode) {
            case INS:
                setMode(Mode.IN);
                setRightNum(zNum.next());
                setRef(1);
                leftNbrs.set(nbrs);
                // nbrs does not contain the immediate left node
                leftNbrs.add(left);
                // insert callのawaitをはずす
                futures.set(reqNo);
                break;
            case DEL:
                setMode(Mode.GRACE);
                // delete callのawaitをはずす
                futures.set(reqNo);
                break;
            default:
                if (expectDelayedResponse) {
                    logger.debug("maybe received a delayed SetRAck");
                } else {
                    logger.warn("mode={} when SetRAck is received", mode);
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    /**
     * SetRNak message handler.
     * 
     * <pre>
     * pseudo code:
     * (A4)
     * receive SetRNak() from q →
     *   if (s = jng) then s := jwait
     *   elseif (s = lvg) then s := lwait
     *   elseif (fixing) then fixing := false fi
     * </pre>
     * 
     * @param sender the sender of this SetRNak message
     * @param reqNo request number
     * @param curR current right link of the sender node
     */
    public void setRNak(Link sender, int reqNo, Link curR) {
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("receive SetRNak(sender={}, reqNo={})", sender, reqNo);
        try {
            if (reqNo != FIXREQNO && futures.expired(reqNo)) {
                logger.info("setR is expired in setRNak");
                return;
            }
            if (fixState == FIX_STATE.FIXING_BOTH
                    || fixState == FIX_STATE.FIXING_LEFTONLY) {
                logger.info("setRNak received from {}, fixState={}", sender,
                        fixState);
                fixDone();
                return;
            }
            switch (mode) {
            case INS:
                if (!sender.equals(left)) {
                    logger.warn(
                            "received SetRNak from unexpected sender {}, ignored",
                            sender);
                    break;
                }
                setMode(Mode.INSWAIT);
                // insert callのawaitをはずす
                if (reqNo != 0)
                    futures.set(reqNo, curR);
                break;
            case DEL:
                setMode(Mode.DELWAIT);
                // delete callのawaitをはずす
                if (reqNo != 0)
                    futures.set(reqNo);
                protoCond.signalAll();
                break;
            default:
                if (expectDelayedResponse) {
                    logger.debug("maybe received a delayed SetRNak");
                } else {
                    logger.warn("illegal mode: {} in setRNak", mode);
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    public void setL(Link lNew, LinkSeq lNewNum, Link prevL, Set<Link> nbrs) {
        /*
         * (A5) receive SetL(lnew , lnewnum , d) from q → if (lnewnum>lnum) then
         * l, lnum := lnew, lnewnum fi send UnrefL() to d
         */
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("receive SetL(lNew={}, lNewNum={}, prevL={}, nbrs={})",
                lNew, lNewNum, prevL, nbrs);

        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in setL");
                // 応答を返さない
                return;
            }
            boolean leftChanged = false;
            if (lNum.compareTo(lNewNum) < 0) {
                setLeft(lNew);
                setLeftNum(lNewNum);
                leftChanged = true;
            } else {
                logger.info("SetL ignored (lNum={}, lNewNum={})", lNum, lNewNum);
            }
            try {
                NodeManagerIf stub = getStub(prevL.addr);
                stub.unrefL(prevL.key, me);
            } catch (Throwable e) {
                handleRPCException("setL", "unrefL", e);
            }

            if (!leftChanged) {
                return;
            }

            /*
             * 近隣ノード集合を置き換えて，右ノードに伝達．
             */
            nbrs.add(lNew);
            leftNbrs.set(nbrs);
            // 1 2 [3] 4
            // 自ノードが4とする．
            // [3]を挿入する場合，4がSetL受信．この場合のlimitは2 (lPrev=2, lNew=3)
            // [3]を削除する場合，4がSetL受信．この場合のlimitも2 (lPrev=3, lNew=2)
            if (Node.isOrdered(prevL.key, lNew.key, key)) {
                // this SetL is sent for inserting a node (lNew)
                leftNbrs.sendRight(key, right, prevL.key);
            } else {
                // this SetL is sent for deleting a node (prevL)
                leftNbrs.sendRight(key, right, lNew.key);
            }

            /*
             * Some optimization (論文には書いていない) k-abe
             */
            if (mode == Mode.INSWAIT || mode == Mode.DELWAIT) {
                logger.debug("wakeup insert or delete thread");
                protoCond.signalAll();
            }
            if (mode == Mode.DEL) {
                if (me.equals(right) && me.equals(left)) {
                    // 自ノードが削除のためにSetRを送信中に，SetLを受信し，それによって
                    // 自ノードの左ポインタが自ノードを指している．
                    // また，右ノードも自ノードを指しているので，自ノードは唯一のノード
                    // である．このため，自ノードはSetRAckを待たずにOUT状態に遷移する．
                    // このとき，SetRAck/Nakが後から到着する可能性があることを示すため，
                    // expectDelayedResponseをセットする．
                    logger.info("SetL received while mode=DEL, case l=u=r");
                    setMode(Mode.OUT);
                    setLeft(null);
                    setRight(null);
                    // unblock the thread blocked in delete0()
                    futures.set(currentReqNo);
                    expectDelayedResponse = true;
                    protoCond.signalAll();
                } else if (me.equals(left)) {
                    // XXX: この状況になることになる条件が不明!!!
                    logger.info("SetL received while mode is DEL (l == u)");
                    setMode(Mode.GRACE);
                    futures.set(currentReqNo);
                    expectDelayedResponse = true;
                    // unblock the thread blocked in delete0()
                    protoCond.signalAll();
                    try {
                        sendSetL(right, left, rNum.next(), me, true);
                    } catch (Throwable e) {
                        handleRPCException("setL", "setL", e);
                    }
                } else {
                    logger.info("SetL received while mode is DEL (l != u)");
                    setMode(Mode.DELWAIT);
                    futures.set(currentReqNo);
                    expectDelayedResponse = true;
                    // unblock the thread blocked in delete0()
                    protoCond.signalAll();
                }
            }
        } finally {
            logger.debug("SetL() finished: {}", this);
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    public void unrefL(Link sender) {
        /*
         * (A6) receive UnrefL() from q →
         *  ref := ref − 1
         *  if (ref = 0) then send UnrefL() to l; s := out fi
         */
        protoLock.writeLock().lock();
        logger.trace("ENTRY:");
        logger.debug("receives UnrefL from {}", sender);
        try {
            if (ref > 0) {
                setRef(ref - 1);
            }
            logger.debug("ref={}, mode={}", ref, mode);
            if (ref == 0) {
                if (mode == Mode.DEL) {
                    return;
                }
                if (mode != Mode.GRACE) {
                    logger.warn("ref = 0 but mode != {DEL, GRACE}");
                    return;
                }
                if (sendLastUnrefL) {
                    logger.debug("send the last unrefL to {}", left);
                    try {
                        NodeManagerIf stub = getStub(left.addr);
                        stub.unrefL(left.key, me);
                    } catch (Throwable e) {
                        handleRPCException("unrefL", "unrefL", e);
                    }
                }
                setMode(Mode.OUT);
                setLeft(null);
                setRight(null);
                protoCond.signalAll();
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.writeLock().unlock();
        }
    }

    public void setFindResult(int reqNo, Link left, Link right) {
        protoLock.readLock().lock();
        try {
            // insert callのawaitをはずす
            futures.set(reqNo, new InsertPoint(left, right));
        } finally {
            protoLock.readLock().unlock();
        }
    }

    public void setFindNext(int reqNo, Link next, Link prevKey) {
        protoLock.readLock().lock();
        try {
            // insert callのawaitをはずす
            futures.set(reqNo, new Object[] { next, prevKey });
        } finally {
            protoLock.readLock().unlock();
        }
    }

    // on hop nodes
    public void findNearest(Link sender, int reqNo, DdllKey searchKey,
            Link prevKey) {
        protoLock.readLock().lock();
        logger.trace("ENTRY:");
        logger.debug(
                "receive findNearest(sender={}, reqNo={}, searchKey={}, prevKey={})",
                sender, reqNo, searchKey, prevKey);

        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in findNearest ({}->{})", sender, me);
                // ここで応答を返さないと，Skip graphの挿入時にタイムアウト待ちに
                // なるノードがチェーン状に発生する可能性があるため，応答を返すことにする．
                // (k-abe)
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindResult(sender.key, reqNo, null, null);
                } catch (Throwable e) {
                    handleRPCException("findNearest", "setFindResult", e);
                }
                return;
            }
            if (prevKey == null) {
                prevKey = keyComp.compare(key, searchKey) < 0 ? left : right;
            }
            if (mode != Mode.GRACE && isOrdered(key, searchKey, right.key)) {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindResult(sender.key, reqNo, me, right);
                } catch (Throwable e) {
                    handleRPCException("findNearest", "setFindResult", e);
                }
            } else if (mode != Mode.GRACE
                    && isOrdered(key, searchKey, prevKey.key)) {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindNext(sender.key, reqNo, right, prevKey);
                } catch (Throwable e) {
                    handleRPCException("findNearest", "setFindNext", e);
                }
            } else {
                try {
                    NodeManagerIf stub = getStub(sender.addr);
                    stub.setFindNext(sender.key, reqNo, left, me);
                } catch (Throwable e) {
                    handleRPCException("findNearest", "setFindNext", e);
                }
            }
        } finally {
            logger.trace("EXIT:");
            protoLock.readLock().unlock();
        }
    }

    public void getStat(Link sender, int reqNo) {
        protoLock.readLock().lock();
        try {
            if (mode == Mode.OUT) {
                logger.warn("mode is OUT in getStat ({}->{})", sender, me);
                // 応答を返さない
                return;
            }
            // for debugging
            if (mode == Mode.IN && (left == null || right == null)) {
                logger.warn("getStat: something wrong: {}", me);
            }
            try {
                NodeManagerIf stub = getStub(sender.addr);
                stub.setStat(sender.key, reqNo, new Stat(mode, me, left, right,
                        rNum));
            } catch (Throwable e) {
                handleRPCException("getStat", "setStat", e);
            }
        } finally {
            protoLock.readLock().unlock();
        }
    }

    /**
     * returns the status of this node.
     * <p>
     * this method is called by
     * {@link NodeManager#getStatMulti(Endpoint, DdllKey[])} to gather status of
     * multiple Node instances.
     * 
     * @return status of this node
     */
    Stat getStatNew() {
        protoLock.readLock().lock();
        // logger.debug("getStatMulti is called at {}", me);
        try {
            return new Stat(mode, me, left, right, rNum);
        } finally {
            protoLock.readLock().unlock();
        }
    }

    void setStat(int reqNo, Stat stat) {
        protoLock.readLock().lock();
        try {
            // getStat callのawaitをはずす
            futures.set(reqNo, stat);
        } finally {
            protoLock.readLock().unlock();
        }
    }

    void propagateNeighbors(DdllKey src, Set<Link> newset, DdllKey limit) {
        protoLock.readLock().lock();
        logger.trace("ENTRY:");
        logger.debug("receive propagateNeighbors(src={}, newset={}, limit={})",
                src, newset, limit);
        try {
            leftNbrs.receiveNeighbors(src, newset, right, limit);
        } finally {
            protoLock.readLock().unlock();
        }
    }

    void sendSetL(Link dest, Link lNew, LinkSeq lNewNum, Link lPrev,
            boolean isInserted) throws RPCException {
        // compute the neighbor node set at the dest node.
        Set<Link> nset = leftNbrs.computeNSForRight(dest);
        // sending a SetL message is equivalent to sending a propagateNeighbors
        // message.  make leftNbrs know this fact. 
        leftNbrs.setPrevRightSet(dest, nset);
        NodeManagerIf stub = getStub(dest.addr);
        stub.setL(dest.key, lNew, lNewNum, lPrev, nset);
    }

    /*
     * link repairing (link fixing) related procedures
     */
    /**
     * called from {@link NodeMonitor.NodeMon#ping()} when a node failure is
     * detected.
     * 
     * @param failedLinks the failed links.
     */
    void onNodeFailure(final Collection<Link> failedLinks) {
        protoLock.readLock().lock();
        try {
            if (mode != Mode.IN && mode != Mode.DELWAIT) {
                logger.debug("onNodeFailure: not inserted");
                return;
            }
        } finally {
            protoLock.readLock().unlock();
        }
        if (observer != null) {
            boolean rc = observer.onNodeFailure(failedLinks);
            if (rc) {
                startFix(failedLinks);
            }
        } else {
            startFix(failedLinks);
        }
    }

    /**
     * start fixing a single failed link.
     * 
     * @param failed the failed link.
     * @param force     true if you want to check the left node even if it is
     *                  not equals to `failed'.
     */
    public void startFix(Link failed, boolean force) {
        startfix(Collections.singleton(failed), null, force);
    }

    public void startFix(Link failed) {
        startFix(failed, false);
    }

    /**
     * start fixing set of failed links.
     * 
     * @param failedLinks the failed links.
     */
    public void startFix(final Collection<Link> failedLinks) {
        startfix(failedLinks, null, false);
    }

    public void startfix(final Collection<Link> failedLinks, Object payload) {
        startfix(failedLinks, payload, false);
    }

    /**
     * start fixing `failedLinks'. `payload' is piggy-backed by SetR message if
     * it is non-null.
     * 
     * @param failedLinks the failed links.
     * @param payload the payload object.
     * @param force     true if you want to check the left node even if it is
     *                  not contained in `failedLinks'.
     */
    public void startfix(final Collection<Link> failedLinks,
            final Object payload, boolean force) {
        Link left = getLeft();
        if (left == null || (!force && !failedLinks.contains(left))) {
            logger.debug("startfix: no fix: {}", failedLinks);
            if (observer != null && payload != null) {
                observer.payloadNotSent(payload);
            }
            return;
        }
        leftNbrs.removeNodes(failedLinks);
        Thread th = new Thread(THREAD_NAME_PREFIX + thNum.getAndIncrement()) {
            @Override
            public void run() {
                try {
                    logger.debug("start fixing (failedKeys = {})", failedLinks);
                    fix(failedLinks, payload);
                } catch (OfflineSendException e) {
                }
            }
        };
        int num = fixCount.getAndIncrement();
        th.setName("FIX" + key + "(fail=" + failedLinks + ")#" + num);
        th.start();
        return;
    }

    private AtomicInteger fixCount = new AtomicInteger(0);

    /**
     * called when NodeMonitor receives a Stat message from the left node.
     * 
     * @param s Stat message just received
     */
    void statReceived(Stat s) {
        protoLock.readLock().lock();
        if ((mode == Mode.IN || mode == Mode.DELWAIT) && getLeft().equals(s.me)) {
            if (me.equals(s.right)) {
                logger.trace("ENTRY:");
            } else {
                logger.debug("statReceived: mismatch with left node: {}", s);
                Thread th =
                        new Thread(THREAD_NAME_PREFIX + thNum.getAndIncrement()) {
                            @Override
                            public void run() {
                                try {
                                    // TODO: fix()は，再度 getStat を要求してしまう．
                                    // sを使うように改善するべき．
                                    fix(null, null);
                                } catch (OfflineSendException e) {
                                }
                            }
                        };
                th.setName("FIX" + key + "(mismatch)" + thNum.getAndIncrement());
                th.start();
            }
        } else {
            logger.debug("statReceived: ignore: {}", s);
        }
        protoLock.readLock().unlock();
    }

    /*
     * State Transition Diagram:
     *
     *          fix()          left node NG
     * WAITING --------> CHECKING --------> FIXING
     *    |  left node OK   |                 |
     *    ^-----------------+                 |
     *    ^-----------------------------------'
     *   (SETR_OP_TIMEOUT, SetRAck, SetRNak, SetL)
     *
     * WAITING : 待機中．
     * CHECKING: fix()で左ノードのチェック中．
     * FIXING  : fix()で左ノードの故障を検出し，SetRメッセージを送信した状態．
     *
     * メモ:
     * ・状態遷移の排他制御には，writeLock を使う．
     * ・WAITINGへの遷移は notFixing() で行う．
     * ・変数 fixTask は，FIXING状態でSetRがタイムアウトしたかをチェックするTimerTaskを
     *   保持．
     * ・SetRAck, SetRNakで WAITING に遷移するときは，FixTask をキャンセル．
     * ・FIXINGの間にSetLメッセージを受信して左ノードが書き換わった場合，WAITING状態へ遷移．
     */

    private volatile long lastFix = 0;

    /**
     * check the left node and recover the left link if necessary.
     * <p>
     * this method returns without confirming the recovery of the left
     * link. however, if the recovery attempt fails, another thread is created
     * and keeps trying to recover the left link so eventually the left link is
     * recovered.
     * 
     * @param failed known failed links
     * @param payload an Object to be sent with SetR message. null is ok.
     * @throws OfflineSendException
     */
    private void fix(final Collection<Link> failed, Object payload)
            throws OfflineSendException {
        logger.debug("fix(failed={})", failed);
        logger.debug("node status: {}", this);
        boolean callObserver = false;
        protoLock.writeLock().lock();
        if (fixState != FIX_STATE.WAITING) {
            try {
                logger.debug("another thread is fixing: {}", fixState);
                if (observer != null && payload != null) {
                    // wait until the current fix procedure executed by
                    // another thread terminates and then
                    // call the observer#payloadNotSent() to retransmit the
                    // payload.
                    while (fixState != FIX_STATE.WAITING) {
                        logger.debug("waiting (fixState = {})", fixState);
                        try {
                            protoCond.await();
                        } catch (InterruptedException e) {
                        }
                        logger.debug("waiting done");
                    }
                    // call observer after unlocking
                    callObserver = true;
                }
            } finally {
                protoLock.writeLock().unlock();
            }
            if (callObserver) {
                observer.payloadNotSent(payload);
            }
            return;
        }
        try {
            /*
             * {Fix} [] (A11) (s = in ∨ s = delwait ∨ s = grace) ∧ ¬fixing →
             * {execute periodically} (v, vr, vrnum) := findLiveLeft() if (s
             * = grace) then if (l ̸= v) then l, lastUnrefL := v, false fi skip
             * fi if (v = l ∧ vr = u ∧ vrnum = lnum) then skip fi {left link is
             * consistent} l , lnum , fixing := v, lnum.gnext(), true; send
             * SetR(u, vr , lnum , 0) to l [] (A12) timeout fixing → fixing :=
             * false
             */
            fixState = FIX_STATE.CHECKING;
            if (mode != Mode.IN && mode != Mode.DELWAIT && mode != Mode.GRACE) {
                logger.debug("node is not fully inserted (1)");
                return;
            }
            // -- unlock region
            protoLock.writeLock().unlock();
            // To avoid rush of fixing, make minimum interval between fixing.
            if (System.currentTimeMillis() - lastFix < MIN_FIX_INTERVAL) {
                logger.debug("delay {}ms", MIN_FIX_INTERVAL);
                try {
                    Thread.sleep(MIN_FIX_INTERVAL);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            logger.debug("searching new left node candidates");
            Stat v;
            try {
                v = findLiveLeft(failed);
                logger.debug("fix(): findLiveLeft returns: {}", v);
                if (v == null) {
                    if (mode == Mode.IN || mode == Mode.DELWAIT) {
                        logger.warn("could not find any live node: {}",
                                leftNbrs);
                    }
                    return;
                }
            } finally {
                protoLock.writeLock().lock();
            }
            logger.debug("status: {}", this);
            // lockを外している間に状態が変更されている可能性があるので再度チェック
            if (mode != Mode.IN && mode != Mode.DELWAIT && mode != Mode.GRACE) {
                logger.debug("node is not fully inserted (2)");
                return;
            }
            // --
            if (mode == Mode.GRACE) {
                if (!left.equals(v.me)) {
                    setLeft(v.me);
                    sendLastUnrefL = false;
                    // XXX: callObserver?
                }
                return;
            }
            // note that v.rNum is null if v.s = INS
            if (v.me.equals(left) && v.right.equals(me) && lNum.equals(v.rNum)) {
                // left link is consistent
                if (payload != null) {
                    callObserver = true;
                    logger.debug("SetR not send payload");
                }
                return;
            }
            // let's fix
            setLeft(v.me);
            setLeftNum(lNum.gnext());
            int type;
            if (isOrdered(v.me.key, key, v.right.key)) {
                fixState = FIX_STATE.FIXING_BOTH;
                type = NodeManagerIf.SETR_TYPE_FIX_BOTH;
                setRight(v.right);
                // rNum は SetRAckを受信するまで値が古い． 
                logger.debug("fix between {} and {}", v.me, v.right);
            } else {
                fixState = FIX_STATE.FIXING_LEFTONLY;
                type = NodeManagerIf.SETR_TYPE_FIX_LEFTONLY;
                logger.debug("fix left link only");
            }
            logger.info("send SetR to {} for recovery", left);
            try {
                NodeManagerIf stub = getStub(left.addr);
                stub.setR(left.key, me, FIXREQNO, me, v.right, lNum,
                        type, payload);
            } catch (RPCException e) {
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
            }
            /*
             * 修復のために代替左ノードを発見して SetR メッセージを送信したので， 
             * そのノードからのSetRAck/SetRNakメッセージを待つ．
             */
            fixTask = new TimerTask() {
                // timer is expired (means the previous attempt to fix the left
                // link failed)
                @Override
                public void run() {
                    logger.debug("fixTask#run");
                    boolean retry = false;
                    protoLock.writeLock().lock();
                    if (fixState == FIX_STATE.FIXING_BOTH
                            || fixState == FIX_STATE.FIXING_LEFTONLY) {
                        // SetRAckもSetRNakも受信していないので，再修復を試みる．
                        logger.debug("no SetRAck/SetRNak is received "
                                + "within SETR_TIMEOUT");
                        fixTask = null;
                        retry = true;
                        fixDone();
                    }
                    protoLock.writeLock().unlock();
                    if (retry) {
                        // add the failed link to `failed'
                        Collection<Link> f = new HashSet<Link>();
                        if (failed != null) {
                            f.addAll(failed);
                        }
                        f.add(left);
                        startFix(f);
                    }
                }
            };
            stabilizeTimer.schedule(fixTask, Node.SETR_TIMEOUT);
        } finally {
            if (fixState == FIX_STATE.CHECKING) {
                // fixStateがCHECKINGのままということは，修復する必要がなかったということ．
                fixDone();
                assert fixState == FIX_STATE.WAITING;
            }
            lastFix = System.currentTimeMillis();
            protoLock.writeLock().unlock();
            if (callObserver && observer != null && payload != null) {
                observer.payloadNotSent(payload);
            }
            logger.debug("fix() finished");
        }
    }

    /**
     * FIXING (あるいは CHECKING) からWAITINGに移行する．
     */
    private void fixDone() {
        assert protoLock.isWriteLockedByCurrentThread();
        fixState = FIX_STATE.WAITING;
        logger.debug("fixDone: {}", this);
        protoCond.signalAll(); // wake up threads blocked in fix() if any
    }

    /**
     * search the live immediate left node.
     * 
     * @param failedLinks known failed links. the nodes listed in failedLinks
     *            are considered being failed without checking.
     * @return the status of the immediate live left node.
     * @throws OfflineSendException
     */
    private Stat findLiveLeft(Collection<Link> failedLinks)
            throws OfflineSendException {
        Stat s = findLiveLeft0(failedLinks);
        logger.debug("findLiveLeft1 returns {}", s);
        return s;
    }

    /**
     * search the live immediate left node by using leftNbrs.
     * 
     * @param failedLinks known failed links. the nodes listed in failedLinks
     *            are considered being failed without checking.
     * @return the status of the immediate live left node.
     * @throws OfflineSendException
     */
    private Stat findLiveLeft0(Collection<Link> failedLinks)
            throws OfflineSendException {
        Map<Link, Stat> stats = new HashMap<Link, Stat>();
        if (failedLinks != null) {
            // register the failed node so that we do not have to traverse
            // rightward from the left node of the failed node
            for (Link failed : failedLinks) {
                stats.put(failed, null);
            }
        }
        NeighborSet cands = new NeighborSet(me, manager, Integer.MAX_VALUE);
        cands.addAll(leftNbrs.getNeighbors());
        cands.add(me);
        /*
         * N10.l = N10, N10.r = N20
         * N20.r = N30
         * のような場合，N10の近隣ノードに右ノードが入っていないと修復できない．
         * このため right を入れておく．
         */
        cands.add(right);
        // 自ノードの他のキーを追加する．
        // キーのidが同じキーは論理的に同じ連結リスト上にあるという前提
        Set<Link> others = manager.getAllLinksById(key.id);
        others.remove(me);
        if (others.size() > 0) {
            logger.debug("add other keys: {}", others);
            cands.addAll(others);
        }
        // add external links not managed by DDLL
        if (observer != null) {
            List<Link> ext = observer.suppplyLeftCandidatesForFix();
            if (ext != null) {
                logger.debug("add external keys: {}", others);
                cands.addAll(ext);
            }
        }
        logger.info("stock left nodes: {}", cands);
        List<Link> nbrs = new ArrayList<Link>(cands.getNeighbors());

        // find the closest node in the left side whose state is IN or DELWAIT
        Link n = null;
        for (Link node : nbrs) {
            logger.debug("checking {}", node);
            // ignore the ghost of myself (XXX: THINK!)
            if (node.key.rawKey.equals(me.key.rawKey)
                    && node.addr.equals(me.addr) && !node.equals(me)) {
                continue;
            }
            Stat s = callGetStat(node);
            stats.put(node, s);
            logger.debug("results from {} is {}", node, s);
            if (s == null) {
                leftNbrs.removeNode(node);
                continue;
            }
            // note that it is okay here if the node is reinserted
            logger.debug("{}: findLiveLeft: got stat {}", me, s);
            if (s.mode == Mode.IN || s.mode == Mode.DELWAIT) {
                n = node;
                break;
            }
        }
        if (n == null) {
            // if we do not find any live node, return myself.
            if (nbrs.size() > 0) {
                logger.warn("{}: findLiveLeft could not find "
                        + "any IN nor DELWAIT node. (nbrs={})", me, nbrs);
            }
            protoLock.readLock().lock();
            try {
                // check my state (my right node might be null!) 
                if (mode != Mode.IN && mode != Mode.DELWAIT) {
                    logger.debug("{}: findLiveLeft1: noticed i've left", me);
                    return null;
                }
                n = me;
                // insert a fake entry
                stats.put(n, new Stat(mode, me, left, right, rNum));
                // return new Stat(mode, me, left, right, rNum);
            } finally {
                protoLock.readLock().unlock();
            }
        }
        // search rightward from node n
        logger.debug("traversing rightward from {}", n);
        while (true) {
            Stat nstat = stats.get(n);
            Link nr = nstat.right;
            if (nr.key.compareTo(key) == 0) {
                // if n = n.r, return n (mostly n = myself case)
                return nstat;
            }
            if (n.key.compareTo(key) != 0 && isOrdered(n.key, key, nr.key)) {
                // if n < u < n.r, return n
                // logger.info("findLiveLeft: loop {}", count);
                return nstat;
            }
            if (stats.containsKey(nr)) {
                if (stats.get(nr) == null) {
                    // nr is failed
                    return stats.get(n);
                }
                // move rightward
                n = nr;
                continue;
            }
            // nr is not listed in leftNbr and (n < nr < u)
            logger.info("{}: findLiveLeft found new node {}", me, nr);
            Stat nrstat = callGetStat(nr);
            if (nrstat == null) {
                // logger.info("findLiveLeft: loop {}", count);
                return stats.get(n);
            }
            logger.info("{}: findLiveLeft new node stat {}", me, nrstat);
            stats.put(nr, nrstat);
            leftNbrs.add(nr);
            n = nr;
        }
    }

    private void handleRPCException(String method, String rpc, Throwable e) {
        if (e instanceof OfflineSendException) {
            logger.warn("{}: calling RPC({}) failed (I'm offline)", method, rpc);
        } else if (e instanceof RPCException) {
            logger.error("", e.getCause());
        } else {
            logger.error("", e);
        }
    }

    private Stat callGetStat(Link node) throws OfflineSendException {
        logger.trace("ENTRY:");
        try {
            int reqNo = futures.newFuture();
            try {
                logger.debug("calling getStat() on node {}", node);
                NodeManagerIf stub = getStub(node.addr);
                stub.getStat(node.key, me, reqNo);
            } catch (RPCException e) {
                logger.debug("got RPCException {}", e.toString());
                futures.discardFuture(reqNo);
                if (e instanceof OfflineSendException) {
                    throw (OfflineSendException) e;
                } else {
                    logger.error("", e.getCause());
                }
                return null;
            }
            try {
                logger.debug("waiting on reqNo {}", reqNo);
                Object result = futures.get(reqNo, GETSTAT_OP_TIMEOUT);
                logger.debug("got result {}", result);
                if (result instanceof Stat) {
                    return (Stat) result;
                } else {
                    logger.error("illegal result");
                    return null;
                }
            } catch (TimeoutException e) {
                logger.warn("getStat to {} is timed out", node);
                return null;
            }
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * a class representing an insertion point in a linked-list.
     */
    public static class InsertPoint implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Link left;
        public final Link right;

        public InsertPoint(Link left, Link right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "[" + left + ", " + right + "]";
        }
    }

    /**
     * a class representing a result of insertion.
     */
    public static class InsertionResult {
        /** insertion succeeded? */
        public final boolean success;
        /** an InsertPoint that can be used on the next insertion retry */
        public final InsertPoint hint;
        private static InsertionResult successInstance = new InsertionResult(
                true);
        private static InsertionResult failureInstance = new InsertionResult(
                false);
        public static InsertionResult getSuccessInstance() {
            return successInstance; 
        }
        public static InsertionResult getFailureInstance() {
            return failureInstance;
        }
        private InsertionResult(boolean success) {
            this(success, null);
        }
        public InsertionResult(boolean success, InsertPoint hint) {
            this.success = success;
            this.hint = hint;
        }
        @Override
        public String toString() {
            return "[" + success + ", hint=" + hint + "]";
        }
    }
}
