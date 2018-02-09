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

package org.piax.gtrans.ov.suzaku;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.piax.ayame.EventSender;
import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQStrategy;
import org.piax.ayame.ov.rq.RQStrategy.RQNodeFactory;
import org.piax.ayame.ov.suzaku.SuzakuStrategy;
import org.piax.ayame.ov.suzaku.SuzakuStrategy.SuzakuNodeFactory;
import org.piax.common.ComparableKey;
import org.piax.common.ComparableKey.SpecialKey;
import org.piax.common.DdllKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.Option.BooleanOption;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransportListener;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.netty.kryo.KryoUtil;
import org.piax.gtrans.ov.Link;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.RoutingTableAccessor;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Suzaku<D extends Destination, K extends ComparableKey<?>>
        extends OverlayImpl<D, K> implements RoutingTableAccessor {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(Suzaku.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("suzaku");
    public static final int DEFAULT_SUZAKU_TYPE = 3; // Suzaku algorithm.
    public static final BooleanOption EXEC_ASYNC = new BooleanOption(true, "-exec-async"); // exec get asynchronously 

    RQNodeFactory factory;
    @SuppressWarnings("rawtypes")
    EventSender sender;
    Map<K,LocalNode> nodes;
    
    static {
        // ayame related classes
        // XXX need to think where these registration should be located
        KryoUtil.register(org.piax.ayame.Event.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.DdllKeyRange.class);
        KryoUtil.register(org.piax.ayame.ov.rq.DKRangeRValue.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.LinkSeq.class);
        KryoUtil.register(org.piax.ayame.Node.class, new NodeSerializer());
        KryoUtil.register(org.piax.ayame.LocalNode.class, new NodeSerializer());
        KryoUtil.register(org.piax.ayame.FTEntry.class);
        KryoUtil.register(org.piax.ayame.FTEntry[].class);
        KryoUtil.register(org.piax.ayame.Event.Lookup.class);
        KryoUtil.register(org.piax.ayame.Event.AckEvent.class);
        KryoUtil.register(org.piax.ayame.Event.LookupDone.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.DdllEvent.SetRAck.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.DdllEvent.SetR.class);
        KryoUtil.register(org.piax.ayame.ov.rq.RQAdapter.InsertionPointAdapter.class);
        KryoUtil.register(org.piax.ayame.ov.rq.RQAdapter.KeyAdapter.class);
        KryoUtil.register(org.piax.ayame.ov.rq.RQRange.class);
        KryoUtil.register(org.piax.ayame.ov.rq.RQReply.class);
        KryoUtil.register(org.piax.ayame.ov.rq.RQRequest.class); 
        KryoUtil.register(org.piax.ayame.ov.suzaku.SuzakuEvent.GetEntReply.class);
        KryoUtil.register(org.piax.ayame.ov.suzaku.SuzakuStrategy.FTEntrySet.class);
        KryoUtil.register(org.piax.gtrans.ov.suzaku.Suzaku.ExecQueryAdapter.class);
        KryoUtil.register(org.piax.ayame.ov.suzaku.SuzakuEvent.GetEntRequest.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.DdllEvent.GetCandidates.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.DdllEvent.GetCandidatesReply.class);
        KryoUtil.register(org.piax.ayame.ov.ddll.DdllEvent.SetL.class);
    }

    public Suzaku() throws IdConflictException, IOException {
        this(Overlay.DEFAULT_ENDPOINT.value());
    }
    
    public Suzaku(String spec) throws IdConflictException, IOException {
        this(DEFAULT_TRANSPORT_ID,
                Peer.getInstance(PeerId.newId()).newBaseChannelTransport(Endpoint.newEndpoint(spec)),
                DEFAULT_SUZAKU_TYPE);
    }

    public Suzaku(ChannelTransport<?> lowerTrans) throws IdConflictException,
            IOException {
        this(DEFAULT_TRANSPORT_ID, lowerTrans, DEFAULT_SUZAKU_TYPE);
    }

    public Suzaku(ChannelTransport<?> lowerTrans, int suzakuType) throws IdConflictException,
    IOException {
        this(DEFAULT_TRANSPORT_ID, lowerTrans, suzakuType);
    }

    public Suzaku(TransportId transId, ChannelTransport<?> lowerTrans)
            throws IdConflictException, IOException {
        this(transId, lowerTrans, DEFAULT_SUZAKU_TYPE);
    }

    public Suzaku(TransportId transId, ChannelTransport<?> lowerTrans, int suzakuType)
            throws IdConflictException, IOException {
        super(lowerTrans.getPeer(), transId, lowerTrans);
        peer.registerBaseOverlay(transIdPath);
        SuzakuNodeFactory base = new SuzakuNodeFactory(suzakuType);
        factory = new RQNodeFactory(base);
        //sender = new EventSenderNet(transId, lowerTrans);
        sender = new NetEventSender<>(transId, lowerTrans);
        logger.debug("EventSender started:" + lowerTrans.getEndpoint());
        nodes = new HashMap<>();
    }

    @Override
    public synchronized void fin() {
        try {
            leave();
        } catch (IOException e) {
            e.printStackTrace();
        }
        super.fin();
        if (sender instanceof NetEventSender) {
            ((NetEventSender)sender).fin();
        }
        peer.fin();
    }

    public Endpoint getEndpoint() {
        Endpoint ep = sender.getEndpoint();
        if (ep instanceof PrimaryKey) { 
            return ep;
        }
        else {
            return peerId;
        }
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
    
    public static class ExecQueryAdapter extends RQAdapter<Object> {
        private static final long serialVersionUID = -2672546268071889814L;
        public NestedMessage nmsg;
        @SuppressWarnings("rawtypes")
        transient public Suzaku szk;
        public ExecQueryAdapter(ObjectId objId, NestedMessage nmsg, Consumer<RemoteValue<Object>> resultsReceiver) {
            super(resultsReceiver);
            this.nmsg = nmsg;
        }
        @SuppressWarnings("rawtypes")
        public ExecQueryAdapter(Suzaku szk) { // for executor side.
            super(null);
            this.szk = szk;
        }

        @Override
        public CompletableFuture<Object> get(RQAdapter<Object> received, DdllKey key) {
            if (EXEC_ASYNC.value()) {
                return CompletableFuture.supplyAsync(()->szk.onReceiveRequest(key, ((ExecQueryAdapter)received).nmsg));
            }
            else {
                return CompletableFuture.completedFuture(szk.onReceiveRequest(key, ((ExecQueryAdapter)received).nmsg));
            }
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
                    if (ret instanceof RemoteValue<?> &&
                            ((RemoteValue<Object>)ret).getValue() instanceof FutureQueue<?>) {
                        Object val = ret.get();
                        for (RemoteValue<Object> r : (FutureQueue<Object>)val) {
                            fq.add(r);
                        }
                    }
                    else {
                        fq.add(ret);
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
    @SuppressWarnings("unchecked")
    @Override
    public void requestAsync(ObjectId sender, ObjectId receiver, D d,
            Object msg, BiConsumer<Object, Exception> responseReceiver,
            TransOptions opts) {
        if (sender == null) {
            sender = getDefaultAppId();
        }
        if (receiver == null) {
            receiver = getDefaultAppId();
        }
        if (opts == null) {
            opts = new TransOptions();
        }
        Collection<KeyRange<K>> ranges = null;
        
        if (d instanceof ComparableKey) {
            KeyRanges<K> dst = new KeyRanges<K>((K)d);
            ranges = dst.getRanges();
        }
        else if (d instanceof KeyRanges) {
            KeyRanges<K> dst = (KeyRanges<K>) d;
            ranges = dst.getRanges();
        }
        else if (d instanceof KeyRange) {
            ranges = (Collection<KeyRange<K>>) Collections.singleton(d);
        }
        else if (d instanceof LowerUpper) {
            forwardQueryToMaxLessThanAsync(sender, receiver, (LowerUpper) d, msg,
                    responseReceiver,
                    opts);
            return;
        }
        else {
            logger.warn("unknown destination type:" + d); // XXX oops, LowerUpper 
        }
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId, msg);
        
        getEntryPoint()
        .rangeQueryAsync(ranges, new ExecQueryAdapter(receiver, nmsg, (ret)-> {
            try {
                if (ret == null) {
                    responseReceiver.accept(Response.EOR, null); // End of response.
                }
                else {
                    if (ret.getValue().equals(Response.EMPTY)) {
                        responseReceiver.accept(null, (Exception)ret.getException());
                    }
                    else {
                        responseReceiver.accept(ret.getValue(), (Exception)ret.getException());
                    }
                }
            }
            catch (Exception e) {
                responseReceiver.accept(null, e);
            }
        }), opts);
    }

    public void forwardQueryToMaxLessThanAsync(ObjectId sender,
            ObjectId receiver, LowerUpper lu, Object msg,
            BiConsumer<Object, Exception> responseReceiver, TransOptions opts)
            throws IllegalStateException {
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId, msg);
        getEntryPoint().forwardQueryLeftAsync(lu.getRange(), lu.getMaxNum(),
                new ExecQueryAdapter(receiver, nmsg, (ret)-> {
                    try {
                        if (ret == null) {
                            responseReceiver.accept(Response.EOR, null); // End of response.
                        }
                        else {
                            responseReceiver.accept(ret.getValue(), (Exception)ret.getException());
                        }
                    }
                    catch (Exception e) {
                        responseReceiver.accept(null, e);
                    }
                })
                , opts);
    }

    /*
     * for MaxLessEq key query
     */
    @SuppressWarnings("unchecked")
    public FutureQueue<?> forwardQueryToMaxLessThan(ObjectId sender,
            ObjectId receiver, LowerUpper lu, Object msg, TransOptions opts)
            throws IllegalStateException {
        logger.debug("ENTRY: {}", msg);
        if (!isJoined) {
            throw new IllegalStateException("Not joined to the network yet.");
        }
        if (opts == null) {
            opts = new TransOptions();
        }
        logger.debug("opts: {}", opts);
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId, msg);
        FutureQueue<Object> fq = new FutureQueue<>();
        getEntryPoint().forwardQueryLeftAsync(lu.getRange(), lu.getMaxNum(),
                new ExecQueryAdapter(receiver, nmsg, (ret)-> {
                    try {
                        if (ret == null) {
                            fq.setEOFuture();
                        }
                        else if (ret instanceof RemoteValue<?> &&
                                ((RemoteValue<Object>)ret).getValue() instanceof FutureQueue<?>) {
                            Object val = ret.get();
                            for (RemoteValue<Object> r : (FutureQueue<Object>)val) {
                                fq.add(r);
                            }
                        }
                        else {
                            fq.add(ret);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                })
                , opts);
        return fq;
    }

    /*
     * Invokes LL-Net, ALM-type execQuery
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object onReceiveRequest(DdllKey key, NestedMessage nmsg) {
        logger.trace("ENTRY:");
        Collection<K> matchedKeys = Collections.<K>singleton((K) key.getRawKey());
        
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);

        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<K> keys = getKeys(nmsg.receiver);
        keys.retainAll(matchedKeys);
        // if keys.size == 0, the listener should not be called? 

        OverlayReceivedMessage<K> rcvMsg = 
                new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());

        TransportListener<D> listener = getListener0(nmsg.receiver);
        if (listener == null) {
            logger.debug("onReceiveRequest data purged as no such listener {}",
                    nmsg.receiver);
            return Response.EMPTY; // not a valid response.
        }
        if (listener instanceof OverlayListener) {
            Object ret = selectOnReceive((OverlayListener) listener, this, rcvMsg);
            if (ret instanceof FutureQueue<?>) { // just for compatibility.
                return ((FutureQueue<Object>)ret).poll().getValue();
            }
            return ret;
        } else if (listener instanceof RequestTransportListener) {
            Object ret = selectOnReceive((RequestTransportListener) listener, this, rcvMsg);
            if (ret instanceof FutureQueue<?>) {
                return ((FutureQueue<Object>)ret).poll().getValue();
            }
            return ret;
        } else {
            Object inn = checkAndClearIsEasySend(rcvMsg.getMessage());
            rcvMsg.setMessage(inn);
            //listener.onReceive((Transport<D>) getLowerTransport(), rcvMsg);
            listener.onReceive(this, rcvMsg);
            return Response.EMPTY; // thrown away: FutureQueue.emptyQueue();
        }
    }
    /*
    // toplevel api.
    public void setOverlayListener(ObjectId appId, OverlayListener<D,K> listener) {
        listenersByUpper.put(appId, listener);
    }*/

    public void setRequestListener(ObjectId appId, RequestTransportListener<D> listener) {
        listenersByUpper.put(appId, listener);
    }
    
    public void setRequestListener(String appIdStr, RequestTransportListener<D> listener) {
        listenersByUpper.put(new ObjectId(appIdStr), listener);
    }
    
    public void setRequestListener(RequestTransportListener<D> listener) {
        listenersByUpper.put(getDefaultAppId(), listener);
    }

    private boolean meansRoot(Endpoint seed) {
        return lowerTrans.getEndpoint().equals(seed);
        
        //return getEndpoint().equals(seed);
    }

    public boolean join() throws ProtocolUnsupportedException, IOException {
        return join(lowerTrans.getEndpoint().newSameTypeEndpoint(Overlay.DEFAULT_SEED.value()));
    }

    public boolean join(String spec) throws ProtocolUnsupportedException, IOException {
        return join(lowerTrans.getEndpoint().newSameTypeEndpoint(spec));
    }

    /*
     * TODO IOExceptionが返る時、keyRegisterのすべてのkeyをaddKeyしない状態の
     * 場合がある。本来はこの状態管理をkey毎にしておく必要がある
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean join(Collection<? extends Endpoint> seeds) throws IOException {
            logger.trace("ENTRY: seeds={}", seeds);
            if (isJoined) {
                return false;
            }
            if (seeds == null || seeds.size() == 0) {
                throw new IllegalArgumentException("invalied specified seeds");
            }

            K primaryKey;
            if (sender.getEndpoint() instanceof PrimaryKey) { // IdChannelTransport
                primaryKey = (K)(((PrimaryKey)sender.getEndpoint()).getRawKey());
            }
            else {
                primaryKey = (K)this.peerId;
            }
            // join時に必ず、peerIdを登録する
            //registerKey((K) this.peerId);
            registerKey(primaryKey);
            // if root peer
            if (seeds.size() == 1) {
                for (Endpoint peerLocator : seeds) {
                    if (meansRoot(peerLocator)) {
                        boolean first = true;
                        for (K key : keyRegister.keySet()) {
                            szAddKey(null, key, first);
                            first = false;
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

    /*
     * TODO IOExceptionが返る時、keyRegisterのすべてのkeyをaddKeyしない状態の
     * 場合がある。本来はこの状態管理をkey毎にしておく必要がある
     */
    @Override
    public boolean leave() throws IOException {
            logger.trace("ENTRY:");
            if (!isJoined) {
                return false;
            }
            for (K key : keyRegister.keySet()) {
                if (sender instanceof NetEventSender && ((NetEventSender)sender).isRunning() || 
                        !(sender instanceof NetEventSender)) {
                    szRemoveKey(key);
                }
            }
            isJoined = false;
            return true;
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
                logger.debug("initial=" + node.key + "self=" + node.addr);
                node.joinInitialNode();
            }
            else {
                logger.debug("seed=" + (seed == null ? node.addr : seed) + ","+ node.key + "self=" + node.addr);
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
        boolean exists = false;
        synchronized(keyRegister) {
            exists = keyRegister.containsKey(key);
        }
        if (!exists) {
            lowerAddKey(key);
        }
        synchronized(keyRegister) {
            if (!keyRegister.containsKey(key)) {
                super.registerKey(upper, key);
            }
        }
        return true;
    }

    private void szRemoveKey(K key) throws IOException {
        logger.trace("ENTRY:");
        logger.debug("szRemoveKey:" + key);
        LocalNode n = nodes.get(key);
        CompletableFuture<Boolean> f = n.leaveAsync();
        try {
            f.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("leave failed: {}" + e);
        }
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
        if (key instanceof PrimaryKey || key instanceof PeerId) {
            throw new IllegalArgumentException("Primary key or Peer Id cannot be removed (leave instead)");
        }
        this.checkActive();
        synchronized (keyRegister) {
            if (!keyRegister.containsKey(key)) {
                return false;
            }
        }
        // if this key is single, do remove from overlay
        if (numOfRegisteredKey(key) == 1) {
            lowerRemoveKey(key);
        }
        synchronized (keyRegister) {
            if (keyRegister.containsKey(key)) {
                unregisterKey(upper, key);
            }
        }
        return true;
    }

    private Link[] nodes2Links(List<Node> nodes) {
        return nodes.stream().map(node->{
            return new Link(node.addr, node.key);
        }).toArray(count->{
            return new Link[count];
        });
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
        List<Link>ret = new ArrayList<>();
        nodes.values().forEach((n) -> {
            Stream<FTEntry> s = SuzakuStrategy.getSuzakuStrategy(n).getFTEntryStream();
            if (s != null){
                s.forEach((e) -> {
                    e.allNodes().stream().forEach((node) -> {
                        ret.add(new Link(node.addr, node.key));
                    });
                });
            }
        });
        return ret.toArray(new Link[0]);
    }

    @Override
    public Link getLocal(Comparable<?> key) {
        Node node = nodes.get(key);
        return new Link(node.addr, node.key);
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
        SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(nodes.get(key));
        FTEntry ent = szk.getFingerTableEntry(false, level);
        return ent != null? nodes2Links(ent.allNodes()) : new Link[0];
    }

    @Override
    public Link[] getLefts(Comparable<?> key, int level) {
        SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(nodes.get(key));
        FTEntry ent = szk.getFingerTableEntry(true, level);
        return ent != null? nodes2Links(ent.allNodes()) : new Link[0];
    }
    
    @Override
    public int getHeight(Comparable<?> key) {
        SuzakuStrategy szk = SuzakuStrategy.getSuzakuStrategy(nodes.get(key));
        return szk.getFingerTableSize();
    }
}
