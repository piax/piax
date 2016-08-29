/*
 * SimpleFlooding.java - An implementation of simple flooding.
 * 
 * Copyright (c) 2012-2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: SimpleFlooding.java 1171 2015-05-18 14:07:32Z teranisi $
 */

package org.piax.gtrans.ov.flood;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of simple flooding.
 */
public class SimpleFlooding<D extends Destination, K extends Key> extends
        OverlayImpl<D, K> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(SimpleFlooding.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("flood");
    
    public static int MAX_HOPS = 7;
    public static int MAX_LINKS = 30;

    final FloodingNode<D, K> flood;

    public SimpleFlooding(ChannelTransport<?> trans) throws IdConflictException,
            IOException {
        this(DEFAULT_TRANSPORT_ID, trans);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public SimpleFlooding(TransportId transId, ChannelTransport<?> trans)
            throws IdConflictException, IOException {
        super(trans.getPeer(), transId, trans);
        Peer.getInstance(peerId).registerBaseOverlay(transIdPath);
        flood = new FloodingNode(this, new TransportId(transId + "$"), trans);
    }

    @Override
    public synchronized void fin() {
        flood.fin();
        super.fin();
    }

    public Endpoint getEndpoint() {
        return lowerTrans.getEndpoint();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            Destination dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        logger.debug("peer:{} dst:{} msg:{}", peerId, dst, msg);
        // TODO Need to implement opts.
        if (!(dst instanceof GeoRegion || dst instanceof KeyRanges
                || dst instanceof KeyRange || dst instanceof Key)) {
            throw new ProtocolUnsupportedException(
                    "flooding only supports region, key, range or ranges destination");
        }
        NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                getEndpoint(), msg);
        List<RemoteValue<?>> rlist = flood.request(
                Collections.singletonList((Endpoint) getEndpoint()), dst, nmsg);
        FutureQueue<?> fq = new FutureQueue(rlist);
        fq.setEOFuture();
        return fq;
    }

    public FutureQueue<?> onReceiveRequest(Collection<K> matchedKeys,
            NestedMessage nmsg) {
        logger.trace("ENTRY:");
        logger.debug("peerId:{} matchedKeys:{} nmsg:{}", peerId, matchedKeys, nmsg);
        OverlayListener<D, K> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest purged data as no such listener");
            return FutureQueue.emptyQueue();
        }
        OverlayReceivedMessage<K> rcvMsg = new OverlayReceivedMessage<K>(
                nmsg.sender, nmsg.src, matchedKeys, nmsg.getInner());
        return selectOnReceive(ovl, this, rcvMsg);
    }
    
    @Override
    public boolean addKey(ObjectId upper, K key) throws IOException {
        logger.debug("peerId:{} upper:{}, key:{}", peerId, upper, key);
        return super.addKey(upper, key);
    }
    
    @Override
    public synchronized boolean join(Collection<? extends Endpoint> seeds)
            throws IOException {
        if (isJoined()) {
            return false;
        }
        isJoined = true;
        flood.link(seeds);
        return true;
    }

    @Override
    public boolean leave() throws IOException {
        if (!isJoined()) {
            return false;
        }
        isJoined = false;
        return true;
    }

    public String showTable() {
        StringBuilder sb = new StringBuilder();
        Set<Endpoint> locs = flood.getLinks();
        for (Endpoint loc : locs) {
            sb.append("   " + loc + "\n");
        }
        return sb.toString();
    }
}
