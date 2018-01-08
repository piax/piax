/*
 * PeerIdWithLocator.java - A pair of Peer Identifier and Locator.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: PeerIdWithLocator.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

/**
 * A class to hold a pair of Peer Identifier and Locator.
 */
public class PeerIdWithLocator extends PeerId {
    private static final long serialVersionUID = 1L;
    
    protected final PeerLocator locator; 
    
    public PeerIdWithLocator(PeerId peerId, PeerLocator locator) {
        super(peerId._getBytes());
        this.locator = locator;
    }
    
    public PeerId getPeerId() {
        return new PeerId(bytes);
    }
    
    public PeerLocator getLocator() {
        return locator;
    }
}
