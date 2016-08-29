/*
 * SkipGraphIf.java - A RPC interface of SkipGraph.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: SkipGraphIf.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.ov.sg;

import java.util.Collection;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.sg.SGMessagingFramework.SGReplyMessage;
import org.piax.gtrans.ov.sg.SGMessagingFramework.SGRequestMessage;
import org.piax.gtrans.ov.sg.SkipGraph.BestLink;
import org.piax.gtrans.ov.sg.SkipGraph.ExecQueryReturn;
import org.piax.gtrans.ov.sg.SkipGraph.QueryId;
import org.piax.gtrans.ov.sg.SkipGraph.RightNodeMismatch;
import org.piax.gtrans.ov.sg.SkipGraph.SGNodeInfo;

/**
 * Skip Graph RPC interface
 */
public interface SkipGraphIf<E extends Endpoint> extends RPCIf {
    @RemoteCallable
    public SGNodeInfo getSGNodeInfo(Comparable<?> target, int level,
            MembershipVector mv, int nTraversed) throws NoSuchKeyException,
            RPCException;

    @RemoteCallable
    @Deprecated
    public BestLink findClosestLocal(DdllKey target, boolean accurate)
            throws UnavailableException, RPCException;

    @RemoteCallable
    public ExecQueryReturn invokeExecQuery(Comparable<?> target, Link curRight,
            QueryId qid, boolean doAction, Object query)
            throws NoSuchKeyException, RightNodeMismatch, RPCException;

    @RemoteCallable
    public Link[] getLocalLinks() throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void requestMsgReceived(SGRequestMessage<E> sgMessage) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void replyMsgReceived(SGReplyMessage<E> sgReplyMessage) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void ackReceived(int msgId) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void fixAndPropagateSingle(Comparable<?> primaryKey, Link failedLink,
            Collection<Link> failedLinks, DdllKey rLimit) throws RPCException;
}
