/*
 * NoSuchPeerException.java - An exception occurs when there's no remote object.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: NoSuchRemoteObjectException.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

import java.io.IOException;

/**
 * 通信時に相手先として指定したピアが存在しないことを示す例外。
 * An exception occurs when there's no remote object.
 */
public class NoSuchRemoteObjectException extends IOException {
    private static final long serialVersionUID = 1L;

    public NoSuchRemoteObjectException() {
    }

    /**
     * @param message
     */
    public NoSuchRemoteObjectException(String message) {
        super(message);
    }
}
