/*
 * PeerInfo.java - A pair of PeerId and Endpoint
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: PeerInfo.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import java.io.Serializable;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;

/**
 * A pair of PeerId and Endpoint.
 */
public class PeerInfo<E extends Endpoint> implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final PeerId peerId;
    protected final E endpoint;
    
    public PeerInfo(PeerId peerId, E endpoint) {
        this.peerId = peerId;
        this.endpoint = endpoint;
    }
    
    public PeerId getPeerId() {
        return peerId;
    }

    public E getEndpoint() {
        return endpoint;
    }
    
    @Override
    public int hashCode() {
        return endpoint.hashCode() ^ peerId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof PeerInfo))
            return false;
        @SuppressWarnings("unchecked")
        PeerInfo<E> _obj = (PeerInfo<E>) obj;
        boolean i = (peerId == null) ? (_obj.peerId == null) :
            peerId.equals(_obj.peerId);
        if (!i) return false;
        boolean o = (endpoint == null) ? (_obj.endpoint == null) :
            endpoint.equals(_obj.endpoint);
        return o;
    }
    
    @Override
    public String toString() {
        return peerId + "#" + endpoint;
    }
}
