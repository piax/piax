/*
 * RQResults.java - A class for accessing the details of range query
 * results.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.ayame.ov.rq;

/**
 * a class for accessing the details of range query results.
 * (not implemented in ayame version)
 * 
 * @param <T> a type for range query results.
 */
public class RQResults<T> {
    protected final RQRequest<T>.RQCatcher catcher;

    public RQResults(RQRequest<T>.RQCatcher catcher) {
        this.catcher = catcher;
    }
}
