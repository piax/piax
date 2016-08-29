/*
 * OfflineSendException.java - An exception occurs when offline send.
 *
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: OfflineSendException.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ddll;

import org.piax.gtrans.RPCException;

/**
 * An exception occurs when offline send.
 */
public class OfflineSendException extends RPCException {
    private static final long serialVersionUID = 1L;
    
    public OfflineSendException() {
    }

    public OfflineSendException(String message) {
        super(message);
    }
}
