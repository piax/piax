/*
 * PeerId.java - An identifier of a peer.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: PeerId.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

/**
 * A class which represents Peer Id.
 */
public class PeerId extends Id implements Endpoint, ComparableKey<Id> {
    private static final long serialVersionUID = 1L;
    public static final int DEFAULT_BYTE_LENGTH = 16;

    public static PeerId newId() {
        return new PeerId(newRandomBytes(DEFAULT_BYTE_LENGTH));
    }

    public PeerId(byte[] bytes) {
        super(bytes);
    }

    public PeerId(String str) {
        super(str);
    }
}
