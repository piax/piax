/*
 * RPCException.java - An exception occurs while RPCs.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RPCException.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

/**
 * An exception occurs while RPCs.
 */
public class RPCException extends Exception {
    private static final long serialVersionUID = 1L;

    public RPCException() {
    }

    /**
     * @param message
     */
    public RPCException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public RPCException(Throwable cause) {
        super(cause);
    }
}
