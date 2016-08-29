/*
 * NoSuchPeerException.java - An exception occurs when there's no peer.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: NoSuchPeerException.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import java.io.IOException;

/**
 * 通信時に相手先として指定したピアが存在しないことを示す例外。
 */
public class NoSuchPeerException extends IOException {
    private static final long serialVersionUID = 1L;

    public NoSuchPeerException() {
    }

    /**
     * @param message
     */
    public NoSuchPeerException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public NoSuchPeerException(Throwable cause) {
        super(cause);
    }
}
