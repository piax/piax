/*
 * IdConflictException.java - An exception for Id conflictions
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: IdConflictException.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

/**
 * Idの衝突の検知を示す例外
 */
public class IdConflictException extends Exception {
    private static final long serialVersionUID = 1L;

    public IdConflictException() {
    }

    /**
     * @param message
     */
    public IdConflictException(String message) {
        super(message);
    }
}
