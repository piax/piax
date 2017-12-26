/*
 * RQIf.java - A range query overlay interface.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ring.NoSuchKeyException;
import org.piax.gtrans.ov.ring.RingIf;
import org.piax.gtrans.ov.ring.RingManager.ExecQueryReturn;
import org.piax.gtrans.ov.ring.RingManager.RightNodeMismatch;
import org.piax.gtrans.ov.ring.UnavailableException;

/**
 * a virtual node of RangeQuerable P2P network.
 */

public interface RQIf<E extends Endpoint> extends RingIf {
    @RemoteCallable
    public ExecQueryReturn invokeExecQuery(Comparable<?> target, Link curRight,
            QueryId qid, boolean doAction, Object query, TransOptions opts)
            throws NoSuchKeyException, RightNodeMismatch, RPCException;

    @RemoteCallable
    public InsertPoint findImmedNeighbors(E introducer, DdllKey key, Object query, TransOptions opts)
            throws UnavailableException, IOException, RPCException;
}
