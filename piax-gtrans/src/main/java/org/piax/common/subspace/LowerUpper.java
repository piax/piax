/*
 * LowerUpper.java - The max value that is not larger than specified value.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: LowerUpper.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.subspace;

import org.piax.common.Destination;

/**
 * The max value that is not larger than specified value.
 */
public class LowerUpper implements Destination {
    private static final long serialVersionUID = 1L;

    protected final KeyRange<?> range;
    protected final boolean isPlusDir;
    protected final int maxNum;

    public LowerUpper(KeyRange<?> range, boolean isPlusDir, int maxNum) {
        this.range = range;
        this.isPlusDir = isPlusDir;
        this.maxNum = maxNum;
    }

    public KeyRange<?> getRange() {
        return range;
    }
    
    public boolean isPlusDir() {
        return isPlusDir;
    }

    public int getMaxNum() {
        return maxNum;
    }

    @Override
    public String toString() {
        String sign = isPlusDir ? "+" : "-";
        return range + "<" + sign + maxNum + ">";
    }
}
