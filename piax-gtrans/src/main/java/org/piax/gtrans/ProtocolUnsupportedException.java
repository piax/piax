/*
 * ProtocolUnsupportedException.java - An exception occurs when a protocol
 * is not supported.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ProtocolUnsupportedException.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import java.io.IOException;

/**
 * An exception occurs when a protocol is not supported.
 */
public class ProtocolUnsupportedException extends IOException {
    private static final long serialVersionUID = 1L;

    public ProtocolUnsupportedException() {
    }

    /**
     * @param message
     */
    public ProtocolUnsupportedException(String message) {
        super(message);
    }

    public ProtocolUnsupportedException(Throwable cause) {
        super(cause);
    }
}
