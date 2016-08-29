/*
 * UnavailableException.java - An exception thrown if no key is available
 * on a node.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: UnavailableException.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.ov.ring;

/**
 * An exception thrown if no key is available on a node, which means
 * the node has not inserted into an overlay network.
 */
@SuppressWarnings("serial")
public class UnavailableException extends Exception {
    public UnavailableException(String msg) {
        super(msg);
    }
}
