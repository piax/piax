/*
 * InvalidMessageException.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: InvalidMessageException.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.impl;

/**
 * NestedMessageのmagicが不正。
 * 
 * 
 */
public class InvalidMessageException extends Exception {
    private static final long serialVersionUID = 1L;

    public InvalidMessageException() {
    }

    public InvalidMessageException(String message) {
        super(message);
    }
}
