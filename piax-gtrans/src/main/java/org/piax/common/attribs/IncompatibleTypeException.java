/*
 * IncompatibleTypeException.java - An exception for incompatible type.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: IncompatibleTypeException.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.attribs;

/**
 * An exception for incompatible type.
 */
public class IncompatibleTypeException extends Exception {
    private static final long serialVersionUID = 1L;

    public IncompatibleTypeException() {
    }

    /**
     * @param message
     */
    public IncompatibleTypeException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IncompatibleTypeException(Throwable cause) {
        super(cause);
    }
}
