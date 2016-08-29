/*
 * TransportId.java - An identifier of a Transport.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: TransportId.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

/**
 * A class of a identifier of a Transport.
 */
public class TransportId extends ServiceId {
    private static final long serialVersionUID = 1L;
    
    /**
     * 無名のTransportIdの指定に用いる
     */
    public static final TransportId NULL_ID = new TransportId("");
    
    public TransportId(byte[] bytes) {
        super(bytes);
    }

    public TransportId(String str) {
        super(str);
    }
}
