/*
 * Suzaku.java - A suzaku overlay implementation.
 * 
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MChordSharp.java 1153 2015-02-22 04:29:30Z teranisi $
 */

package org.piax.gtrans.ov.async.suzaku;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransportListener;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.EventSender;
import org.piax.gtrans.async.EventSender.EventSenderNet;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.RoutingTableAccessor;
import org.piax.gtrans.ov.async.rq.RQAdapter;
import org.piax.gtrans.ov.async.rq.RQStrategy;
import org.piax.gtrans.ov.async.rq.RQStrategy.RQNodeFactory;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Suzaku<D extends Destination, K extends ComparableKey<?>>
        extends OverlayImpl<D, K> implements RoutingTableAccessor {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(Suzaku.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("suzaku");
    RQNodeFactory factory;
    EventSender sender;
    Map<K,LocalNode> nodes;
    

    public Suzaku(ChannelTransport<?> lowerTrans) throws IdConflictException,
            IOException {
        this(DEFAULT_TRANSPORT_ID, lowerTrans);
    }

    @SuppressWarnings("unchecked")
    public Suzaku(TransportId transId, ChannelTransport<?> lowerTrans)
            throws IdConflictException, IOException {
        super(lowerTrans.getPeer(), transId, lowerTrans);
        peer.registerBaseOverlay(transIdPath);
        SuzakuNodeFactory base = new SuzakuNodeFactory(3);
        factory = new RQNodeFactory(base);
        sender = new EventSenderNet(transId, lowerTrans);
        nodes = new HashMap<>();
        // XXX 
        EventExecutor.startExecutorThread();
    }

    @Override
    public synchronized void fin() {
        
        super.fin();
    }

    public Endpoint getEndpoint() {
        return peerId;
    }

    public FutureQueue<?> request1(ObjectId sender, ObjectId receiver,
            K dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        return request3(sender, receiver, new KeyRanges<K>(dst), msg, opts);
    }

    public FutureQueue<?> request2(ObjectId sender, ObjectId receiver,
            KeyRange<K> dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        return request3(sender, receiver, new KeyRanges<K>(dst), msg, opts);
    }
    
    public static class ExecQueryAdapter extends RQAdapter<FutureQueue<Object>> {
        public NestedMessage nmsg;
        transient public Suzaku szk;
        public ExecQueryAdapter(ObjectId objId, NestedMessage nmsg, Consumer<RemoteValue<FutureQueue<Object>>> resultsReceiver) {
            super(resultsReceiver);
            this.nmsg = nmsg;
        }
        public ExecQueryAdapter(Suzaku szk) { // for executor side.
            super(null);
            this.szk = szk;
        }
        @Override
        public CompletableFuture<FutureQueue<Object>> get(RQAdapter<FutureQueue<Object>> received, DdllKey key) {
            //return CompletableFuture.completedFuture(szk.onReceiveRequest(key, ((ExecQueryAdapter)received).nmsg));
            return CompletableFuture.supplyAsync(()->{return szk.onReceiveRequest(key, ((ExecQueryAdapter)received).nmsg);});
        }
    }
    
    LocalNode getEntryPoint() {
        // XXX the first entry.
        return nodes.entrySet().iterator().next().getValue();
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public FutureQueue<?> request3(ObjectId sender, ObjectId receiver,
            KeyRanges<K> dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        if (opts == null) {
            opts = new TransOptions();
        }
        Collection<KeyRange<K>> ranges = dst.getRanges();
        if (msg != null && msg instanceof NestedMessage
                && ((NestedMessage) msg).passthrough == SpecialKey.WILDCARD) {
            ranges.add(new KeyRange(SpecialKey.WILDCARD));
        }
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId, msg);
        
        FutureQueue<Object> fq = new FutureQueue<>();
        getEntryPoint()
        .rangeQueryAsync(ranges, new ExecQueryAdapter(receiver, nmsg, (ret)-> {
            try {
                if (ret == null) {
                    fq.setEOFuture();
                }
                else {
                    FutureQueue<Object> o = ret.get();
                    for (RemoteValue<Object> r : o) {
                        fq.add(r);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }), opts);
        return fq;
    }

    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            Destination dst, Object msg, int timeout)
            throws ProtocolUnsupportedException, IOException {
    		return request(sender, receiver, dst, msg, new TransOptions(timeout));
    }
    
    @Override @SuppressWarnings("unchecked")
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            Destination dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        logger.debug("peer:{} dst:{} msg:{}", peerId, dst, msg);
        if (!isJoined) {
            throw new IllegalStateException("Not joined to the network yet.");
        }
        if (dst instanceof KeyRanges) {
            return request3(sender, receiver, (KeyRanges<K>) dst, msg, opts);
        } else if (dst instanceof KeyRange) {
            return request2(sender, receiver, (KeyRange<K>) dst, msg, opts);
        } else if (dst instanceof ComparableKey) {
            return request1(sender, receiver, (K) dst, msg, opts);
            // XXX Not implemented yet.
        } else if (dst instanceof LowerUpper) {
            return forwardQueryToMaxLessThan(sender, receiver, (LowerUpper) dst, msg, opts);
        } else {
            throw new ProtocolUnsupportedException(
                    "chord sharp only supports ranges");
        }
    }
    
    /*
     * for MaxLessEq key query
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FutureQueue<?> forwardQueryToMaxLessThan(ObjectId sender,
            ObjectId receiver, LowerUpper lu, Object msg, TransOptions opts)
            throws IllegalStateException {
        logger.trace("ENTRY:");
        if (!isJoined) {
            throw new IllegalStateException("Not joined to the network yet.");
        }
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId, msg);

               
        //Collection<RemoteValue<?>> ret = chordSharp.forwardQuery(lu.isPlusDir(),
//                lu.getRange(), lu.getMaxNum(), nmsg, opts);
        
        //FutureQueue<?> fq = new FutureQueue(ret);
        //fq.setEOFuture();
        //return fq;
        return null; // XXX not implemented yet.
    }

    /*
     * Invokes LL-Net, ALM-type execQuery
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FutureQueue<?> onReceiveRequest(DdllKey key, NestedMessage nmsg) {
        logger.trace("ENTRY:");
        Collection<K> matchedKeys = Collections.<K>singleton((K) key.getRawKey());
        
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);
        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<K> keys = getKeys(nmsg.receiver);
        keys.retainAll(matchedKeys);

        OverlayReceivedMessage<K> rcvMsg = 
                new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());

        TransportListener<D> listener = getListener0(nmsg.receiver);
        if (listener == null) {
            logger.info("onReceiveRequest data purged as no such listener {}",
                    nmsg.receiver);
            return FutureQueue.emptyQueue();
        }
        if (listener instanceof OverlayListener) {
            return selectOnReceive((OverlayListener) listener, this, rcvMsg);
        } else if (listener instanceof RequestTransportListener) {
            return selectOnReceive((RequestTransportListener) listener, this, rcvMsg);
        } else {
            Object inn = checkAndClearIsEasySend(rcvMsg.getMessage());
            rcvMsg.setMessage(inn);
            //listener.onReceive((Transport<D>) getLowerTransport(), rcvMsg);
            listener.onReceive(this, rcvMsg);
            return FutureQueue.emptyQueue();
        }
    }

    private boolean meansRoot(Endpoint seed) {
        return lowerTrans.getEndpoint().equals(seed);
        
        //return getEndpoint().equals(seed);
    }

    /*
     * TODO IOExceptionが返る時、keyRegisterのすべてのkeyをaddKeyしない状態の
     * 場合がある。本来はこの状態管理をkey毎にしておく必要がある
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean join(Collection<? extends Endpoint> seeds) throws IOException {
        synchronized (keyRegister) {
            logger.trace("ENTRY:");
            if (isJoined) {
                return false;
            }
            if (seeds == null || seeds.size() == 0) {
                throw new IllegalArgumentException("invalied specified seeds");
            }

            // join時に必ず、peerIdを登録する
            registerKey((K) this.peerId);
            // if root peer
            if (seeds.size() == 1) {
                for (Endpoint peerLocator : seeds) {
                    if (meansRoot(peerLocator)) {
                        for (K key : keyRegister.keySet()) {
                            szAddKey(null, key, true);
                        }
                        isJoined = true;
                        // this.seeds = seeds;
                        return true;
                    }
                }
            }
            // if not root peer
            // 先頭のkeyを使って、seed指定のaddKeyを行う
            Iterator<K> it = keyRegister.keySet().iterator();
            K firstKey = it.next();
            for (Endpoint seed : seeds) {
                if (meansRoot(seed))
                    continue;
                szAddKey(seed, firstKey, false);
                break;
            }
            // for all rest of keys
            while (it.hasNext()) {
                K key = it.next();
                szAddKey(null, key, false);
            }
            isJoined = true;
            // this.seeds = seeds;
            return true;
        }
    }

    /*
     * TODO IOExceptionが返る時、keyRegisterのすべてのkeyをaddKeyしない状態の
     * 場合がある。本来はこの状態管理をkey毎にしておく必要がある
     */
    @Override
    public boolean leave() throws IOException {
        synchronized (keyRegister) {
            logger.trace("ENTRY:");
            if (!isJoined) {
                return false;
            }
            for (K key : keyRegister.keySet()) {
                szRemoveKey(key);
            }
            isJoined = false;
            return true;
        }
    }

    @Override
    public Class<?> getAvailableKeyType() {
        return Comparable.class;
    }
    
    private void szAddKey(Endpoint seed, K key, boolean initial) throws IOException {
        logger.trace("ENTRY:");
        try {
            LocalNode node = new LocalNode(sender, new DdllKey(key, peer.getPeerId(), "", null));
            factory.setupNode(node);
            RQStrategy s = (RQStrategy)node.getTopStrategy();
            s.registerAdapter(new ExecQueryAdapter(this));

            if (initial) {
                node.joinInitialNode();
            }
            else {
                node.addKey(seed != null ? seed : node.addr);
            }
            nodes.put(key, node);
        } catch (InterruptedException e) {
            logger.error("", e);
            throw new IOException(e);
        }
    }

    @Override
    protected void lowerAddKey(K key) throws IOException {
        if (!isJoined) return; 
        szAddKey(null, key, false);
    }

    @Override
    public boolean addKey(ObjectId upper, K key) throws IOException {
        logger.trace("ENTRY:");
        logger.debug("upper:{} key:{}", upper, key);
        if (key == null) {
            throw new IllegalArgumentException("null key specified");
        }
        return super.addKey(upper, key);
    }

    private void szRemoveKey(K key) throws IOException {
        logger.trace("ENTRY:");
        for (LocalNode n : nodes.values()) {
            CompletableFuture<Boolean> f = n.leaveAsync();
            f.whenComplete((r, e)->{
                if (e != null) {
                    logger.warn("removing key {}:{}", key, e);
                }
                if (r) {
                    logger.debug("leave completed for key {}", key);
                    nodes.remove(key);
                }
            });
        }
    }

    @Override
    protected void lowerRemoveKey(K key) throws IOException {
        if (!isJoined) return; 
        szRemoveKey(key);
    }

    @Override
    public boolean removeKey(ObjectId upper, K key) throws IOException {
        logger.trace("ENTRY:");
        logger.debug("upper:{} key:{}", upper, key);
        if (key == null) {
            throw new IllegalArgumentException("null key specified");
        }
        return super.removeKey(upper, key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<K> getKeys(ObjectId upper) {
        Set<K> keys = super.getKeys(upper);
        keys.add((K) peerId);
        return keys;
    }
    
    // RoutingTableAccessor implementations.
    @Override
    public Link[] getAll() {
        return null; // XXX not implemented yet.
    }

    @Override
    public Link getLocal(Comparable<?> key) {
        return null; // XXX not implemented yet.
    }

    @Override
    public Link getRight(Comparable<?> key) {
        return getRight(key, 0);
    }

    @Override
    public Link getLeft(Comparable<?> key) {
        return getLeft(key, 0);
    }

    @Override
    public Link getRight(Comparable<?> key, int level) {
        return getRights(key, level)[0];
    }

    @Override
    public Link getLeft(Comparable<?> key, int level) {
        return getLefts(key, level)[0];
    }

    @Override
    public Link[] getRights(Comparable<?> key) {
        return getRights(key, 0);
    }

    @Override
    public Link[] getLefts(Comparable<?> key) {
        return getLefts(key, 0);
    }

    @Override
    public Link[] getRights(Comparable<?> key, int level) {
        return null; // XXX not implemented yet.
    }

    @Override
    public Link[] getLefts(Comparable<?> key, int level) {
        return null; // XXX not implemented yet.
    }
    
    @Override
    public int getHeight(Comparable<?> key) {
        return -1; // XXX not implemented yet.
    }
}
