/*
 * RingManager.java - A node entity of ring overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.subspace.CircularRange;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ddll.NodeManager;
import org.piax.gtrans.ov.ring.rq.RQMessage;
import org.piax.util.KeyComparator;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple ring network over DDLL protocol.
 * this class may be used as a base class of ring-based structured P2P
 * networks.  this class can handle multiple keys.
 * 
 * @param <E> the type of Endpoint in the underlying network.
 */
public class RingManager<E extends Endpoint> extends RPCInvoker<RingIf, E>
        implements RingIf {
    /*--- logger ---*/
    protected static final Logger logger = LoggerFactory
            .getLogger(RingManager.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("ring");
    public final TransportId transId;

    protected static final KeyComparator keyComp = KeyComparator.getInstance();

    /** DDLL node manager */
    protected NodeManager manager;
    protected E myLocator;

    protected final PeerId peerId;
    private ReentrantReadWriteLock rtlock = new ReentrantReadWriteLock();

    protected NavigableMap<Comparable<?>, RingVNode<E>> keyHash =
            new ConcurrentSkipListMap<Comparable<?>, RingVNode<E>>(keyComp);
    protected Random rand = new MersenneTwister();

    protected final MessagingFramework msgframe;
    protected final StatManager statman;

    private final static int NTHREADS_IN_POOL = 2;
    private final MyThreadPool pool;


    /** DDLL's left node check period for level 0 (msec) */
    public static int DDLL_CHECK_PERIOD_L0 = 10 * 1000;

    public static int RPC_TIMEOUT = 20000;

    /**
     * create a Chord# instance.
     * 
     * @param transId
     *            transportId
     * @param trans
     *            underlying transport
     * @throws IdConflictException the exception for id confliction.
     * @throws IOException the exeption for I/O error.
     */
    public RingManager(TransportId transId, ChannelTransport<E> trans)
            throws IdConflictException, IOException {
        super(transId, trans);
        this.transId = transId;
        this.myLocator = trans.getEndpoint();
        this.peerId = trans.getPeerId();
        this.pool = new MyThreadPool(NTHREADS_IN_POOL, "RingPool",
                getPeerId().toString());
        this.manager =
                new NodeManager(new TransportId(transId.toString() + "-ddll"),
                        trans);
        this.msgframe = new MessagingFramework(this);
        this.statman = new StatManager(this);

        logger.debug("DdllRing: transId={}", trans.getTransportId());
    }

    @SuppressWarnings("deprecation")
    @Override
    public synchronized void online() {
        if (isOnline())
            return;
        super.online();
        manager.online();
    }

    @SuppressWarnings("deprecation")
    @Override
    public synchronized void offline() {
        if (!isOnline())
            return;
        manager.offline();
        super.offline();
    }

    @Override
    public synchronized void fin() {
        manager.fin();
        pool.shutdown();
        super.fin();
    }

    public boolean isActive() {
        return super.isActive;
    }

    /* reader-writer locks */
    public void rtLockR() {
        rtlock.readLock().lock();
    }

    public void rtUnlockR() {
        rtlock.readLock().unlock();
    }

    public void rtLockW() {
        rtlock.writeLock().lock();
    }

    public void rtUnlockW() {
        rtlock.writeLock().unlock();
    }

    public Condition newCondition() {
        return rtlock.writeLock().newCondition();
    }

    public void checkLocked() {
        assert (rtlock.getReadLockCount() > 0
                || rtlock.isWriteLockedByCurrentThread());
    }

    @Override
    public RingIf getStub(E addr, int rpcTimeout) {
        return super.getStub(addr, rpcTimeout);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RingIf getStub(Endpoint dst) {
        return super.getStub((E) dst);
    }

    @Deprecated
    public void schedule(TimerTask task, long delay) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay) {
        return pool.schedule(task, delay);
    }

    @Deprecated
    public void schedule(TimerTask task, long delay, long period) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay, long period) {
        return pool.schedule(task, delay, period);
    }

    /**
     * get PeerId
     * 
     * @return PeerId
     */
    public PeerId getPeerId() {
        return peerId;
    }

    /**
     * get all RingVNode instances
     * 
     * @return collection of RingVNode
     */
    public Collection<? extends RingVNode<E>> allVNodes() {
        return keyHash.values();
    }

    /**
     * get all inserted RingVNode instances
     * 
     * @return collection of RingVNode
     */
    public List<? extends RingVNode<E>> allValidVNodes() {
        List<RingVNode<E>> rc = new ArrayList<RingVNode<E>>();
        for (RingVNode<E> vn : keyHash.values()) {
            if (vn.isInserted()) {
                rc.add(vn);
            }
        }
        return rc;
    }

    /**
     * get RingVNode instance (for debugging only)
     * 
     * @param rawkey the raw key.
     * @return RingVNode the ring node.
     */
    public RingVNode<E> getVNode(Comparable<?> rawkey) {
        RingVNode<E> snode = keyHash.get(rawkey);
        return snode;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        //        if (!isOnline()) {
        //            buf.append("(offline)\n");
        //        }
        buf.append("PeerId: " + peerId + "\n");
        for (RingVNode<E> snode : keyHash.values()) {
            buf.append(snode);
        }
        return buf.toString();
    }

    public String toStringShort() {
        return keyHash.keySet().toString();
    }

    /**
     * @param rawkey the rawkey
     * @param params parameters.
     * @return the RingVNode object.
     */
    protected RingVNode<E> newVNode(Comparable<?> rawkey, Object... params) {
        return new RingVNode<E>(this, rawkey);
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
     * gather all links from all virtual nodes.
     * 
     * @return all links from all SGNodes.
     */
    protected NavigableMap<DdllKey, Link> getAvailableLinks() {
        checkLocked();
        NavigableMap<DdllKey, Link> allLinks =
                new ConcurrentSkipListMap<DdllKey, Link>();
        for (RingVNode<E> n : keyHash.values()) {
            for (Link link : n.getAllLinks()) {
                allLinks.put(link.key, link);
            }
        }
        return allLinks;
    }

    @Override
    public Link[] getLocalLinks() {
        List<Link> list = new ArrayList<Link>();
        rtLockR();
        try {
            for (RingVNode<E> snode : keyHash.values()) {
                if (snode.isInserted()) {
                    Link link = snode.getLocalLink();
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
     * add a key to the ring.
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
    protected boolean addKey(Comparable<?> rawkey) throws IOException,
            UnavailableException {
        return addKey(null, rawkey);
    }

    /**
     * add a key to the ring.
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
    public boolean addKey(E introducer, Comparable<?> rawkey)
            throws UnavailableException, IOException {
        return addKey(introducer, rawkey, (Object[]) null);
    }

    protected synchronized boolean addKey(E introducer, Comparable<?> rawkey,
            Object... vnodeParams) throws UnavailableException, IOException {
        if (myLocator.equals(introducer)) {
            introducer = null;
        }
        rtLockW();
        if (keyHash.containsKey(rawkey)) {
            rtUnlockW();
            logger.debug("addKey: already registered: {}", rawkey);
            return false;
        }
        RingVNode<E> n = newVNode(rawkey, vnodeParams);
        keyHash.put(rawkey, n);
        rtUnlockW();
        try {
            boolean rc = n.addKey(introducer);
            if (!rc) {
                logger.error("addKey failed for {}", rawkey);
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
     * remove a key from the ring.
     * 
     * @param rawkey
     *            the key
     * @return true if the key was successfully removed
     * @throws IOException
     *             thrown if some I/O error occurred.
     */
    public boolean removeKey(Comparable<?> rawkey) throws IOException {
        logger.debug("removeKey key={}\n{}", rawkey, this);
        RingVNode<E> snode = keyHash.get(rawkey);
        if (snode == null) {
            return false;
        }
        // removeKeyは時間がかかるため，keyHashからkeyを削除するのは後で行う．
        boolean rc = snode.removeKey();
        if (rc) {
            rtLockW();
            keyHash.remove(rawkey);
            rtUnlockW();
        }
        return rc;
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
     * @param query
     * 			  non-null if NO_RESPONSE
     * @param opts
     *            the transport options.
     * @return the insertion point for `key'
     * @throws UnavailableException
     *             自ノードあるいはseedにkeyが登録されていない
     * @throws IOException
     *             communication error
     */
    public InsertPoint find(E introducer, DdllKey key, boolean accurate, Object query, TransOptions opts)
            throws UnavailableException, IOException {
    		InsertPoint insp = null;
    		try {
    			logger.debug("query={}", query);
    			insp = findImmedNeighbors(introducer, key, query, opts);
    		}
    		catch (TemporaryIOException e) { // get insert point failed by timeout.
    			throw new IOException(e.getCause());
    		}
    		return insp;
    }

    /**
     * Find the contact nodes (the immediate left and right node)
     * for inserting a node.
     * 
     * @param introducer   an introducer node
     * @param key          a DDLL key to be searched
     * @param query        the query object.
     * @param opts         the transport options.
     * @return an InsertPoint.  null if there's no existing node.
     * @throws IOException the exception thrown when an I/O error occurred.
     * @throws UnavailableException the exception thrown when the insert point is unavailable.
     */
    @SuppressWarnings("unchecked")
    public InsertPoint findImmedNeighbors(E introducer, DdllKey key, Object query, TransOptions opts)
            throws UnavailableException, IOException {
        E p = introducer;
        if (p == null) {
            p = myLocator;
            if (getLocalLinks().length == 0) {
                logger.debug("findImmedNeighbors: no local links are available");
                return null;
            }
        }
        while (true) {
            RingIf stub = getStub(p);
            try {
                Link[] links = stub.getClosestLinks(key);
                logger.debug("getClosestLinks({}) at {} returns {}", key, p,
                        Arrays.toString(links));
                if (links[0].addr.equals(p)) {
                    InsertPoint ins = new InsertPoint(links[0], links[1]);
                    return ins;
                }
                p = (E) links[0].addr;
            } catch (RPCException e) {
                throw new IOException(e.getCause());
            } catch (UnavailableException e) {
                throw e;
            }
        }
    }

    /**
     * get the closest left and right links of `key' in the local routing
     * table.
     *
     * @param key  the key
     * @return the immediate left and right links of `key'
     * @throws UnavailableException
     *              thrown if no key is available at the local routing table
     */
    @Override
    public Link[] getClosestLinks(DdllKey key) throws UnavailableException {
        logger.debug("getClosestLinks({}) is called", key);
        //logger.debug(this.toString());
        rtLockR();
        NavigableMap<DdllKey, Link> allLinks = getAvailableLinks();
        rtUnlockR();
        Map.Entry<DdllKey, Link> lent = allLinks.floorEntry(key);
        if (lent == null) {
            lent = allLinks.lastEntry();
            if (lent == null) {
                throw new UnavailableException("Peer " + peerId + " has no key");
            }
        }
        Map.Entry<DdllKey, Link> rent = allLinks.ceilingEntry(key);
        if (rent == null) {
            rent = allLinks.firstEntry();
            if (rent == null) {
                throw new UnavailableException("Peer " + peerId + " has no key");
            }
        }
        Link[] links = new Link[2];
        links[0] = lent.getValue();
        links[1] = rent.getValue();
        logger.debug("getClosestLinks({}) returns [{}, {}]", key, links[0],
                links[1]);
        return links;
    }

    /**
     * find the closest left link of `key' from the local routing table.
     *
     * @param key  the key
     * @return the immediate left link of `key'
     * @throws UnavailableException
     *              thrown if no key is available at the local routing table
     */
    public Link findLeftLink(DdllKey key) throws UnavailableException {
        logger.debug("findLeftLink({}) is called", key);
        Link[] links = getClosestLinks(key);
        return links[0];
    }

    public void fixRoutingTables(Collection<Link> failedNodes,
            RQMessage parentMsg, Collection<CircularRange<DdllKey>> ranges) {
        logger.debug(
                "fixRoutingTables: failedNodes={}, parentMsg={}, ranges={}",
                failedNodes, parentMsg, ranges);
    }

    public RequestMessage getRequestMessageById(int replyId) {
        return msgframe.getRequestMessageById(replyId);
    }

    /**
     * one-way RPCs
     */
    @Override
    public void requestMsgReceived(RequestMessage sgMessage) {
        msgframe.requestMsgReceived(sgMessage);
    }

    @Override
    public void replyMsgReceived(ReplyMessage sgReplyMessage) {
        msgframe.replyMsgReceived(sgReplyMessage);
    }

    @Override
    public void ackReceived(int msgId, AckMessage ackMessage) {
        msgframe.ackMsgReceived(msgId, ackMessage);
    }

    /**
     * A class used for passing a link to the closest node.
     */
    @SuppressWarnings("serial")
    public static class LinkContainer implements Serializable {
        // link to the closest node
        public final Link link;
        // the right link of the closest node (if known).
        public final Link rightLink;
        // true if the node pointed by `link' is the immediate predecessor of
        // the specified key
        public final boolean isImmedPred;

        public LinkContainer(Link link, Link rightLink, boolean isImmedPred) {
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
