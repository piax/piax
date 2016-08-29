/*
 * NoSuchKeyException.java - NoSuchKeyException implementation of SkipGraph.
 * 
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.gtrans.ov.ring;

@SuppressWarnings("serial")
public class NoSuchKeyException extends Exception {
    public NoSuchKeyException(String msg) {
        super(msg);
    }
}
