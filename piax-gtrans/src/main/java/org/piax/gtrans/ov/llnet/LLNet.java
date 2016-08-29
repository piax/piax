/*
 * LLNet.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: LLNet.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.ov.llnet;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.common.Location;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.subspace.GeoEllipse;
import org.piax.common.subspace.GeoRectangle;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
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
 * 
 */
public class LLNet extends OverlayImpl<GeoRegion, Location> implements
        OverlayListener<KeyRanges<LocationId>, LocationId> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(LLNet.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("llnet");

    final Overlay<KeyRanges<LocationId>, LocationId> sg;

    public LLNet(Overlay<? super KeyRanges<LocationId>, ? super LocationId> sg)
            throws IdConflictException, IOException {
        this(DEFAULT_TRANSPORT_ID, sg);
    }

    @SuppressWarnings("unchecked")
    public LLNet(TransportId transId,
            Overlay<? super KeyRanges<LocationId>, ? super LocationId> sg)
            throws IdConflictException, IOException {
        super(sg.getPeer(), transId, sg);
        this.sg = (Overlay<KeyRanges<LocationId>, LocationId>) sg;
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
        return Location.class;
    }

    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            GeoRegion dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        logger.debug("peer:{} dst:{} msg:{}", peerId, dst, msg);
        try {
            GeoRectangle rect = null;
            if (dst instanceof GeoRectangle) {
                rect = (GeoRectangle) dst;
            } else if (dst instanceof GeoEllipse) {
                Rectangle2D r = ((GeoEllipse) dst).getBounds2D();
                rect = new GeoRectangle(r.getX(), r.getY(), r.getWidth(),
                        r.getHeight());
            } else {
                throw new ProtocolUnsupportedException(
                        "LLNet only supports GeoRegion");
            }
            AreaId[] areaIds = AreaId.getAreaIds(rect);
            List<KeyRange<LocationId>> llranges = new ArrayList<KeyRange<LocationId>>();
            for (int i = 0; i < areaIds.length; i++) {
                llranges.add(new KeyRange<LocationId>(areaIds[i].startLocId(),
                        areaIds[i].endLocId()));
            }

            NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                    getEndpoint(), 0, dst, msg);
            return sg.request(transId, new KeyRanges<LocationId>(llranges), nmsg, opts);
        } finally {
            logger.trace("EXIT:");
        }
    }

    public void onReceive(Overlay<KeyRanges<LocationId>, LocationId> trans,
            OverlayReceivedMessage<LocationId> rmsg) {
        logger.trace("ENTRY:");
        Collection<?> matchedKeys = rmsg.getMatchedKeys();
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);

        GeoRegion region = (GeoRegion) nmsg.option;
        Set<Location> registerKeys = getKeys(nmsg.receiver);
        /*
         * TODO LocationIdが一致するが、元のLocationが異なるケース
         * LocationIdにLocationをbindする方法がうまくいかないので妥協
         */
        Set<Location> matchedLocs = new HashSet<Location>();
        for (Location loc : registerKeys) {
            if (region.contains(loc) && matchedKeys.contains(new LocationId(loc))) {
                matchedLocs.add(loc);
            }
        }
        // matchedKeys から locationのセットを求める
        // 但し、regionに含まれ、かつ、registerKeysの要素でないといけない
        // これがうまくいかない。下位の層のせいで、locがnullになる
//        for (Key mk : matchedKeys) {
//            Location loc = (Location) ((WrappedComparableKey<?>) mk).option;
//            if (region.contains(loc) && registerKeys.contains(loc)) {
//                matchedLocs.add(loc);
//            }
//        }
        if (matchedLocs.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return;
            }
        }
        OverlayListener<GeoRegion, Location> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
            return;
        }
        OverlayReceivedMessage<Location> rcvMsg = new OverlayReceivedMessage<Location>(
                nmsg.sender, nmsg.src, matchedLocs, nmsg.getInner());
        ovl.onReceive(this, rcvMsg);
    }

    @Override
    public FutureQueue<?> onReceiveRequest(
            Overlay<KeyRanges<LocationId>, LocationId> trans,
            OverlayReceivedMessage<LocationId> rmsg) {
        logger.trace("ENTRY:");
        Collection<?> matchedKeys = rmsg.getMatchedKeys();
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);

        GeoRegion region = (GeoRegion) nmsg.option;
        Set<Location> registerKeys = getKeys(nmsg.receiver);
        /*
         * TODO LocationIdが一致するが、元のLocationが異なるケース
         * LocationIdにLocationをbindする方法がうまくいかないので妥協
         */
        Set<Location> matchedLocs = new HashSet<Location>();
        for (Location loc : registerKeys) {
            if (region.contains(loc) && matchedKeys.contains(new LocationId(loc))) {
                matchedLocs.add(loc);
            }
        }
        logger.debug("matchedLocs:{}", matchedLocs);
        // matchedKeys から locationのセットを求める
        // 但し、regionに含まれ、かつ、registerKeysの要素でないといけない
        // これがうまくいかない。下位の層のせいで、locがnullになる
//        for (Key mk : matchedKeys) {
//            Location loc = (Location) ((WrappedComparableKey<?>) mk).option;
//            if (region.contains(loc) && registerKeys.contains(loc)) {
//                matchedLocs.add(loc);
//            }
//        }
        if (matchedLocs.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return FutureQueue.emptyQueue();
            }
        }
        OverlayListener<GeoRegion, Location> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
            return FutureQueue.emptyQueue();
        }
        OverlayReceivedMessage<Location> rcvMsg = new OverlayReceivedMessage<Location>(
                nmsg.sender, nmsg.src, matchedLocs, nmsg.getInner());
        return ovl.onReceiveRequest(this, rcvMsg);
    }
    
    @Override
    public boolean join(Collection<? extends Endpoint> seeds)
            throws IOException {
        if (sg.isJoined()) return true;
        return sg.join(seeds);
    }

    @Override
    public boolean leave() throws IOException {
        if (!sg.isJoined()) return true;
        return sg.leave();
    }

    @Override
    protected void lowerAddKey(Location key) throws IOException {
        logger.debug("lower addKey:{}", key);
        sg.addKey(transId, new LocationId(key));
    }

    @Override
    protected void lowerRemoveKey(Location key) throws IOException {
        logger.debug("lower removeKey:{}", key);
        sg.removeKey(transId, new LocationId(key));
    }
    
    @Override
    public boolean isJoined() {
        return sg.isJoined();
    }

    @Override
    public  Overlay<KeyRanges<LocationId>, LocationId> getLowerTransport() {
        return sg;
    }

    //-- unnecessary but need to be defined methods by Java8
    public void onReceive(Transport<KeyRanges<LocationId>> trans, ReceivedMessage rmsg) {
    }
    public void onReceive(RequestTransport<KeyRanges<LocationId>> trans,
            ReceivedMessage rmsg) {
    }
    public FutureQueue<?> onReceiveRequest(RequestTransport<KeyRanges<LocationId>> trans,
            ReceivedMessage rmsg) {
        return null;
    }
}
