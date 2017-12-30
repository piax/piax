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

package org.piax.gtrans.ov.szk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransportListener;
import org.piax.gtrans.ReturnValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.RoutingTableAccessor;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.piax.gtrans.ov.ring.UnavailableException;
import org.piax.gtrans.ov.ring.rq.RQExecQueryCallback;
import org.piax.gtrans.ov.ring.rq.RQResults;
import org.piax.gtrans.ov.ring.rq.RQReturn.MVal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Suzaku<D extends Destination, K extends ComparableKey<?>>
        extends OverlayImpl<D, K> implements RQExecQueryCallback, RoutingTableAccessor {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(Suzaku.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("szk");

    ChordSharp<Endpoint> chordSharp;

    public Suzaku() throws IdConflictException, IOException {
        this(Overlay.DEFAULT_ENDPOINT.value());
    }
    
    public Suzaku(String spec) throws IdConflictException, IOException {
        this(DEFAULT_TRANSPORT_ID,
                Peer.getInstance(PeerId.newId()).newBaseChannelTransport(Endpoint.newEndpoint(spec)));
    }
    
    public Suzaku(ChannelTransport<?> lowerTrans) throws IdConflictException,
            IOException {
        this(DEFAULT_TRANSPORT_ID, lowerTrans);
    }

    @SuppressWarnings("unchecked")
    public Suzaku(TransportId transId, ChannelTransport<?> lowerTrans)
            throws IdConflictException, IOException {
        super(lowerTrans.getPeer(), transId, lowerTrans);
        peer.registerBaseOverlay(transIdPath);
        chordSharp = new ChordSharp<Endpoint>(new TransportId(transId + "x"), 
                (ChannelTransport<Endpoint>) lowerTrans, this);
    }

    @Override
    public synchronized void fin() {
        chordSharp.fin();
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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public FutureQueue<?> request3(ObjectId sender, ObjectId receiver,
            KeyRanges<K> dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        Collection<KeyRange<K>> ranges = dst.getRanges();
        if (msg != null && msg instanceof NestedMessage
                && ((NestedMessage) msg).passthrough == SpecialKey.WILDCARD) {
            ranges.add(new KeyRange(SpecialKey.WILDCARD));
        }
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId, msg);
        RQResults results = chordSharp.scalableRangeQueryPro(ranges, nmsg, opts);

        //if (TransOptions.responseType(opts) == ResponseType.NO_RESPONSE) {
        //		return FutureQueue.emptyQueue();
        //}
        return results.getFutureQueue();
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
        NestedMessage nmsg = new NestedMessage(sender, receiver, null, peerId,
                msg);
        Collection<RemoteValue<?>> ret = chordSharp.forwardQuery(lu.isPlusDir(),
                lu.getRange(), lu.getMaxNum(), nmsg, opts);

        List<RemoteValue<?>> ret2 = new ArrayList<RemoteValue<?>>();
        if (ret != null) {
            // ddllSG.forwardQuery がerrによって、nullを返すことがあるため
            for (RemoteValue<?> rv : ret) {
                if (rv != null && rv.getValue() instanceof MVal) {
                    MVal mval = (MVal) rv.getValue();
                    for (ReturnValue<Object> o : mval.vals) {
                        ret2.add(new RemoteValue<Object>(rv.getPeer(),
                                o.getValue(),o.getException()));
                    }
                } else {
                    ret2.add(rv);
                }
            }
        }
        FutureQueue<?> fq = new FutureQueue(ret2);
        fq.setEOFuture();
        return fq;
    }
/*
    static class MVal implements Serializable {
        private static final long serialVersionUID = 1L;
        List<ReturnValue<Object>> vals = new ArrayList<ReturnValue<Object>>();
    }
*/
    /*
     * rqExecQuery 
     */
    @SuppressWarnings("unchecked")
    public RemoteValue<?> rqExecQuery(Comparable<?> key, Object msg) {
        logger.trace("ENTRY:");
        if (!isJoined) {
            throw new IllegalStateException("Not joined to the network yet.");
        }
        FutureQueue<?> rets = onReceiveRequest(
                Collections.<K>singleton((K) key), (NestedMessage) msg);
        /*
         * TODO: Dirty code for multiple value.
         * Original MSkipGraph also has something to do here according to the original comment.
         */
        MVal mval = new MVal();
        for (RemoteValue<?> x : rets) {
            mval.vals.add(new ReturnValue(x.getValue(),x.getException()));
        }
        return new RemoteValue<Object>(this.peerId, mval);
    }

    /*
     * Invokes LL-Net, ALM-type execQuery
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FutureQueue<?> onReceiveRequest(Collection<K> matchedKeys,
            NestedMessage nmsg) {
        logger.trace("ENTRY:");
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);
        // gatewayのための処理　（BaseOverlayの役割）
        if (matchedKeys.contains(SpecialKey.WILDCARD)) {
            nmsg.setPassthrough(SpecialKey.WILDCARD);
        } else {
            nmsg.setPassthrough(null);
        }
        
        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<K> keys = getKeys(nmsg.receiver);
        keys.retainAll(matchedKeys);
        if (keys.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return FutureQueue.emptyQueue();
            }
        }
        OverlayReceivedMessage<K> rcvMsg = 
                new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());
//       	OverlayListener<D, K> listener = getListener(nmsg.receiver);
        TransportListener<D> listener = getListener0(nmsg.receiver);
       	if (listener == null) {
            logger.info("onReceiveRequest data purged as no such listener {}",
                    nmsg.receiver);
       		return FutureQueue.emptyQueue();
       	}
       	if (listener instanceof OverlayListener) {
            return (FutureQueue<?>)selectOnReceive((OverlayListener) listener, this, rcvMsg);
       	} else if (listener instanceof RequestTransportListener) {
            return (FutureQueue<?>)selectOnReceive((RequestTransportListener) listener, this, rcvMsg);
        } else {
            Object inn = checkAndClearIsEasySend(rcvMsg.getMessage());
            rcvMsg.setMessage(inn);
            //listener.onReceive((Transport<D>) getLowerTransport(), rcvMsg);
            listener.onReceive(this, rcvMsg);
            return FutureQueue.emptyQueue();
        }
    }

    private boolean meansRoot(Endpoint seed) {
        return getEndpoint().equals(seed);
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
                            szAddKey(null, key);
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
                szAddKey(seed, firstKey);
                break;
            }
            // for all rest of keys
            while (it.hasNext()) {
                K key = it.next();
                szAddKey(null, key);
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
    
    private void szAddKey(Endpoint seed, K key) throws IOException {
        logger.trace("ENTRY:");
        try {
            chordSharp.addKey(seed, key);
        } catch (UnavailableException e) {
            logger.error("", e);
            throw new IOException(e);
        }
    }

    @Override
    protected void lowerAddKey(K key) throws IOException {
        if (!isJoined) return; 
        szAddKey(null, key);
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
        chordSharp.removeKey(key);
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
    
    public void scheduleFingerTableUpdate(int delay, int interval){
		chordSharp.scheduleFingerTableUpdate(delay, interval);
    }

    // RoutingTableAccessor implementations.
    @Override
    public Link[] getAll() {
        return chordSharp.getNeighbors(null, false, 0);
    }

    @Override
    public Link getLocal(Comparable<?> key) {
        return chordSharp.getLocal(key);
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
        return chordSharp.getNeighbors(key, true, level);
    }

    @Override
    public Link[] getLefts(Comparable<?> key, int level) {
        return chordSharp.getNeighbors(key, false, level);
    }
    
    @Override
    public int getHeight(Comparable<?> key) {
        return chordSharp.getHeight(key);
    }
}
