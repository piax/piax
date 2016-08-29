/*
 * FloodingNode.java - An implementation of flooding node object.
 * 
 * Copyright (c) 2012-2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: FloodingNode.java 1171 2015-05-18 14:07:32Z teranisi $
 */

package org.piax.gtrans.ov.flood;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.Location;
import org.piax.common.TransportId;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of flooding node object.
 */
public class FloodingNode<D extends Destination, K extends Key> extends
        RPCInvoker<FloodingNodeIf<D>, Endpoint> implements FloodingNodeIf<D> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(FloodingNode.class);

    private final SimpleFlooding<D, K> mother;
    private final Set<Endpoint> forwardTable = new HashSet<Endpoint>();

    @SuppressWarnings("unchecked")
    public FloodingNode(SimpleFlooding<D, K> mother, TransportId transId,
            ChannelTransport<?> trans) throws IdConflictException, IOException {
        super(transId, (ChannelTransport<Endpoint>) trans);
        this.mother = mother;
    }

    private void removeTooMuchLinks() {
        if (forwardTable.size() <= SimpleFlooding.MAX_LINKS) {
            return;
        }
        // select locator randomly
        // TODO not efficient
        List<Endpoint> locs = new ArrayList<Endpoint>(forwardTable);
        Collections.shuffle(locs);
        forwardTable.clear();
        forwardTable.addAll(locs.subList(0, SimpleFlooding.MAX_LINKS));
    }
    
    synchronized void link(Collection<? extends Endpoint> peers) {
        logger.trace("ENTRY:");
        try {
            if (peers.size() == 0) return;
            forwardTable.addAll(peers);
            forwardTable.remove(this.getEndpoint());
            removeTooMuchLinks();
        } finally {
            logger.trace("EXIT:");
        }
    }

    synchronized void unlink(Endpoint peer) {
        forwardTable.remove(peer);
    }

    synchronized Set<Endpoint> getLinks() {
        return new HashSet<Endpoint>(forwardTable);
    }
    
    private List<K> matchedKeys(D dst, NestedMessage nmsg) {
        logger.trace("ENTRY:");
        logger.debug("peerId:{} dst:{} msg:{}", trans.getPeerId(), dst, nmsg);
        List<K> matched = new ArrayList<K>();
        GeoRegion region = (dst instanceof GeoRegion) ? (GeoRegion) dst : null;
        KeyRanges<?> ranges = (dst instanceof KeyRanges) ? (KeyRanges<?>) dst : null;
        KeyRange<?> range = (dst instanceof KeyRange) ? (KeyRange<?>) dst : null;
        Key key = (dst instanceof Key) ? (Key) dst : null;
        boolean hasWILDCARD = nmsg.passthrough == SpecialKey.WILDCARD;
        Set<K> kset = mother.getKeys(nmsg.receiver);
        logger.debug("getKey:{} upper:{}", kset, nmsg.receiver);
        for (K k : mother.getKeys(nmsg.receiver)) {
            if (region != null && k instanceof Location) {
                if (region.contains((Location) k)) {
                    matched.add(k);
                }
            } else if (ranges != null && k instanceof ComparableKey<?>) {
                if (ranges.contains((ComparableKey<?>) k)) {
                    matched.add(k);
                }
            } else if (range != null && k instanceof ComparableKey<?>) {
                if (range.contains((ComparableKey<?>) k)) {
                    matched.add(k);
                }
            } else if (key != null) {
                if (key.equals(k)) {
                    matched.add(k);
                }
            }
//            if (hasWILDCARD && key == SpecialKey.WILDCARD) {
//                matched.add(key);
//            }
        }
        if (hasWILDCARD && mother.getKeys().contains(SpecialKey.WILDCARD)) {
//            matched.add(SpecialKey.WILDCARD);
            nmsg.setPassthrough(SpecialKey.WILDCARD);
        } else {
            nmsg.setPassthrough(null);
        }
        return matched;
    }
    
    public List<RemoteValue<?>> request(List<Endpoint> visited, Destination dst,
            NestedMessage nmsg) {
        logger.trace("ENTRY:");
        logger.debug("peerId:{} visited {}", trans.getPeerId(), visited);
        try {
            List<RemoteValue<?>> rets = new ArrayList<RemoteValue<?>>();
            Set<Endpoint> nexts = getLinks();
            nexts.removeAll(visited);
            List<Endpoint> _visited = new ArrayList<Endpoint>(visited);
            _visited.addAll(nexts);
            
            // local execution
            List<K> keys = matchedKeys((D) dst, nmsg);
            if (!keys.isEmpty() || nmsg.passthrough == SpecialKey.WILDCARD) {
                // gatewayのための処理
                FutureQueue<?> fq = mother.onReceiveRequest(keys, nmsg);
                for (RemoteValue<?> rv : fq) {
                    rets.add(rv);
                }
            }

            // cut the flooding
            if (visited.size() < SimpleFlooding.MAX_HOPS) {
                for (Endpoint nbr : nexts) {
                    try {
                        FloodingNodeIf<D> stub = getStub(nbr);
                        List<RemoteValue<?>> rset = stub.request(_visited, dst,
                                nmsg);
                        if (rset != null) {
                            rets.addAll(rset);
                        }
                    } catch (RPCException e) {
                        logger.info("peer down {}", nbr);
                        unlink(nbr);
                    }
                }
            }
            // learn more link
            link(visited);
            return rets;
        } catch (Exception e) {
            logger.error("", e);
            return null;
        } finally {
            logger.trace("EXIT:");
        }
    }
}
