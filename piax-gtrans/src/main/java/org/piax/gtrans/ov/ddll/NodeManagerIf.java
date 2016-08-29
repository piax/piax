/*
 * NodeManagerIf.java - NodeManager interfaces.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: NodeManagerIf.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;

/**
 * an interface for RPCs of {@link NodeManager}.
 */
public interface NodeManagerIf extends RPCIf {
    public static final int SETR_TYPE_NORMAL = 0;
    public static final int SETR_TYPE_FIX_LEFTONLY = 1;
    public static final int SETR_TYPE_FIX_BOTH = 2;
    @RemoteCallable(Type.ONEWAY)
    void setR(DdllKey target, Link sender, int reqNo, Link rNew, Link rCur,
            LinkNum rNewNum, int type, Object payload)
                    throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setRAck(DdllKey target, Link sender, int reqNo, LinkNum val,
            Set<Link> nbrs) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setRNak(DdllKey target, Link sender, int reqNo, Link curR)
            throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setL(DdllKey target, Link lNew, LinkNum lNewNum, Link d,
            Set<Link> nbrs) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void unrefL(DdllKey target, Link sender) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void findNearest(DdllKey target, Link sender, int reqNo, DdllKey searchKey,
            Link prevKey) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setFindResult(DdllKey target, int reqNo, Link left, Link right)
            throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setFindNext(DdllKey target, int reqNo, Link next, Link prevKey)
            throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void getStat(DdllKey target, Link sender, int reqNo) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setStat(DdllKey target, int reqNo, Stat stat) throws RPCException;

    // for NodeMonitor
    @RemoteCallable(Type.ONEWAY)
    void getStatMulti(Endpoint sender, DdllKey[] targets) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void setStatMulti(Endpoint sender, Stat[] stats) throws RPCException;

    // managing neighbor node set
    @RemoteCallable(Type.ONEWAY)
    void propagateNeighbors(DdllKey src, DdllKey key, Set<Link> newset, DdllKey limit)
            throws RPCException;

    // fast link fixing (experimental)
    @RemoteCallable(Type.ONEWAY)
    void startFix(DdllKey target, Link failedNode, boolean force)
            throws RPCException;
}
