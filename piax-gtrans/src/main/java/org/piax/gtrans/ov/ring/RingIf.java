/*
 * RingIf.java - RingIf implementation of ring overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.gtrans.ov.ring;

import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteCallable.Type;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;

/**
 * RPC interface of simple ring network.
 */
public interface RingIf extends RPCIf {
    /*@RemoteCallable
    @Deprecated
    public BestLink findClosestLocal(DdllKey target, boolean accurate)
            throws UnavailableException, RPCException;*/

    /*@RemoteCallable
    public ExecQueryReturn invokeExecQuery(Comparable<?> target, Link curRight,
            QueryId qid, boolean doAction, Object query)
            throws NoSuchKeyException, RightNodeMismatch, RPCException;*/

    @RemoteCallable
    public Link[] getLocalLinks() throws RPCException;

    @RemoteCallable
    public Link[] getClosestLinks(DdllKey key) throws RPCException,
            UnavailableException;
    
    @RemoteCallable(Type.ONEWAY)
    void requestMsgReceived(RequestMessage sgMessage) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void replyMsgReceived(ReplyMessage sgReplyMessage) throws RPCException;

    @RemoteCallable(Type.ONEWAY)
    void ackReceived(int msgId, AckMessage ackMessage) throws RPCException;
}
