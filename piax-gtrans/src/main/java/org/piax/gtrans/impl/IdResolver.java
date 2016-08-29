/*
 * IdResolver.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: IdResolver.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.util.Register;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class IdResolver {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(IdResolver.class);

    final PeerId myId;
    private final Register<PeerId, PeerLocator> map;
    
    public IdResolver(PeerId myId) {
        this.myId = myId;
        map = new Register<PeerId, PeerLocator>();
    }

    public synchronized boolean contains(PeerId peerId) {
        return map.containsKey(peerId);
    }
    
    public synchronized void set(PeerId peerId, PeerLocator... locs) {
        if (myId.equals(peerId)) {
            logger.info("specified peerId is mine: set");
            return;
        }
        map.remove(peerId);
        for (PeerLocator loc : locs) {
            map.add(peerId, loc);
        }
    }

    public synchronized boolean add(PeerId peerId, PeerLocator loc) {
        if (myId.equals(peerId)) {
            logger.info("specified peerId is mine: add");
            return false;
        }
        return map.add(peerId, loc);
    }

    public synchronized boolean remove(PeerId peerId, PeerLocator loc) {
        if (myId.equals(peerId)) {
            logger.info("specified peerId is mine: remove");
            return false;
        }
        return map.remove(peerId, loc);
    }

    public synchronized void putAll(Map<? extends PeerId, 
            ? extends PeerLocator> map) {
        for (Map.Entry<? extends PeerId, ? extends PeerLocator> entry : 
            map.entrySet()) {
            set(entry.getKey(), entry.getValue());
        }
    }

    public synchronized void removeAll(PeerId peerId) {
        map.remove(peerId);
    }
    
    public synchronized List<PeerId> getPeerIds() {
        return new ArrayList<PeerId>(map.keySet());
    }

    public synchronized List<PeerLocator> getLocators(PeerId peerId) {
        return new ArrayList<PeerLocator>(map.getValues(peerId));
    }
    
    @Override
    public synchronized String toString() {
        return map.toString();
    }
}
