/*
 * LongKey.java - A key wrapper of long type.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: LongKey.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

/**
 * A key wrapper of long type.
 */
public class LongKey extends NumberKey<Long> {
    private static final long serialVersionUID = 1L;

    public LongKey(Long key) {
        super(key);
    }
}
