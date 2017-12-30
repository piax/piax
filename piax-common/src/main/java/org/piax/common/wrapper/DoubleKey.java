/*
 * DoubleKey.java - A key wrapper of double type.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DoubleKey.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

/**
 * A key wrapper of double type.
 */
public class DoubleKey extends NumberKey<Double> {
    private static final long serialVersionUID = 1L;

    public DoubleKey(Double key) {
        super(key);
    }
}
