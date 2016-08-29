/*
 * BaseTransportGenerator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BaseTransportGenerator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.impl;

import java.io.IOException;

import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;

/**
 * 
 */
public abstract class BaseTransportGenerator {

    private BaseTransportGenerator next = null;
    protected final Peer peer;
    
    protected BaseTransportGenerator(Peer peer) {
        this.peer = peer;
    }
    
    protected Peer getPeer() {
        return peer;
    }
    
    public final synchronized void addNext(BaseTransportGenerator gen) {
        next = gen;
    }
    
    public final synchronized void addLast(BaseTransportGenerator gen) {
        if (this.next == null) {
            this.next = gen;
        } else {
            this.next.addLast(gen);
        }
    }

    public final synchronized <E extends PeerLocator> Transport<E> newBaseTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        Transport<E> trans = _newBaseTransport(desc, transId, loc);
        if (trans != null) {
            return trans;
        }
        if (next == null) return null;
        return next.newBaseTransport(desc, transId, loc);
    }
    
    public final synchronized <E extends PeerLocator> ChannelTransport<E> newBaseChannelTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        ChannelTransport<E> trans = _newBaseChannelTransport(desc, transId, loc);
        if (trans != null) {
            return trans;
        }
        if (next == null) return null;
        return next.newBaseChannelTransport(desc, transId, loc);
    }

    public abstract <E extends PeerLocator> ChannelTransport<E> _newBaseChannelTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException;

    public abstract <E extends PeerLocator> Transport<E> _newBaseTransport(
            String desc, TransportId transId, E loc)
            throws IdConflictException, IOException;
}
