/*
 * DOLR.java - Decentralized object location and routing implementation on PIAX
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DOLR.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.dolr;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.wrapper.ConvertedComparableKey;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decentralized object location and routing implementation on PIAX
 */
public class DOLR<K extends Key> extends OverlayImpl<K, K> implements
        OverlayListener<ConvertedComparableKey<K>, ConvertedComparableKey<K>> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(DOLR.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("dolr");

    final Overlay<ConvertedComparableKey<K>, ConvertedComparableKey<K>> sg;

    public DOLR(
            Overlay<? super ConvertedComparableKey<K>, ? super ConvertedComparableKey<K>> sg)
            throws IdConflictException, IOException {
        this(DEFAULT_TRANSPORT_ID, sg);
    }

    @SuppressWarnings("unchecked")
    public DOLR(
            TransportId transId,
            Overlay<? super ConvertedComparableKey<K>, ? super ConvertedComparableKey<K>> sg)
            throws IdConflictException, IOException {
        super(sg.getPeer(), transId, sg);
        this.sg = (Overlay<ConvertedComparableKey<K>, ConvertedComparableKey<K>>) sg;
        this.sg.setListener(transId, this);
    }

    @Override
    public synchronized void fin() {
        sg.setListener(transId, null);
        super.fin();
    }

    @Override
    public Endpoint getEndpoint() {
        return sg.getEndpoint();
    }

    @Override
    public Class<?> getAvailableKeyType() {
        return Comparable.class;
    }

    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            K dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        logger.debug("peer:{} dst:{} msg:{}", peerId, dst, msg);
        try {
            if (!(dst instanceof Key)) {
                throw new ProtocolUnsupportedException(
                        "DOLR only supports Key destination");
            }
            NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                    getEndpoint(), msg);
            return sg.request(transId, new ConvertedComparableKey<K>((K) dst),
                    nmsg, opts);
        } finally {
            logger.trace("EXIT:");
        }
    }

    public void onReceive(Overlay<ConvertedComparableKey<K>, ConvertedComparableKey<K>> trans,
            OverlayReceivedMessage<ConvertedComparableKey<K>> rmsg) {
        logger.trace("ENTRY:");
        Set<K> matched = new HashSet<K>();
        for (ConvertedComparableKey<K> k : rmsg.getMatchedKeys()) {
            matched.add(k.key);
        }
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matched, nmsg);
        
        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<K> keys = getKeys(nmsg.receiver);
        keys.retainAll(matched);
        if (keys.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return;
            }
        }
        OverlayListener<K, K> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
            return;
        }
        OverlayReceivedMessage<K> rcvMsg = new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());
        ovl.onReceive(this, rcvMsg);
    }

    public FutureQueue<?> onReceiveRequest(
            Overlay<ConvertedComparableKey<K>, ConvertedComparableKey<K>> trans,
            OverlayReceivedMessage<ConvertedComparableKey<K>> rmsg) {
        logger.trace("ENTRY:");
        Set<K> matched = new HashSet<K>();
        for (ConvertedComparableKey<K> k : rmsg.getMatchedKeys()) {
            matched.add(k.key);
        }
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matched, nmsg);
        
        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<K> keys = getKeys(nmsg.receiver);
        keys.retainAll(matched);
        if (keys.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return FutureQueue.emptyQueue();
            }
        }
        OverlayListener<K, K> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
            return FutureQueue.emptyQueue();
        }
        OverlayReceivedMessage<K> rcvMsg = new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());
        return ovl.onReceiveRequest(this, rcvMsg);
    }

    @Override
    public boolean join(Collection<? extends Endpoint> seeds)
            throws IOException {
        logger.trace("ENTRY:");
        if (sg.isJoined()) return true;
        return sg.join(seeds);
    }

    @Override
    public boolean leave() throws IOException {
        logger.trace("ENTRY:");
        if (!sg.isJoined()) return true;
        return sg.leave();
    }
    
    @Override
    public boolean isJoined() {
        return sg.isJoined();
    }
    
    @Override
    protected void lowerAddKey(K key) throws IOException {
        logger.debug("lower addKey:{}", key);
        sg.addKey(transId, new ConvertedComparableKey<K>(key));
    }

    @Override
    protected void lowerRemoveKey(K key) throws IOException {
        logger.debug("lower removeKey:{}", key);
        sg.removeKey(transId, new ConvertedComparableKey<K>(key));
    }

    @Override
    public Overlay<ConvertedComparableKey<K>, ConvertedComparableKey<K>> getLowerTransport() {
        return sg;
    }

    //-- unnecessary but need to be defined methods by Java8
    public void onReceive(Transport<ConvertedComparableKey<K>> trans, ReceivedMessage rmsg) {
    }
    public void onReceive(RequestTransport<ConvertedComparableKey<K>> trans,
            ReceivedMessage rmsg) {
    }
    public FutureQueue<?> onReceiveRequest(RequestTransport<ConvertedComparableKey<K>> trans,
            ReceivedMessage rmsg) {
        return null;
    }
}
