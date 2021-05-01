/*
 * IntegerKey.java - A key wrapper of integer type.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: IntegerKey.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

/**
 * A key wrapper of integer type.
 */
public class IntegerKey extends NumberKey<Integer> {
    private static final long serialVersionUID = 1L;

    public IntegerKey(Integer key) {
        super(key);
    }
}
