/*
 * RawTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RawTransport.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.Transport;

/**
 * 
 */
public abstract class RawTransport<E extends PeerLocator> extends
        MonoTransportImpl<E> {
    
    protected final E peerLocator;
    
    protected RawTransport(PeerId peerId, E peerLocator, boolean supportsDuplex) {
        super(peerId, supportsDuplex);
        this.peerLocator = peerLocator;
    }
    
    @Override
    public void fin() {
        if (!isActive) return;
        super.fin();
        this.peer.getBaseTransportMgr().onFadeout(peerLocator, true);
    }

    public E getEndpoint() {
        return peerLocator;
    }

    @Override
    public Transport<?> getLowerTransport() {
        return null;
    }

    public boolean canSend(PeerLocator target) {
        if (target == null) return false;
        return peerLocator.getClass().equals(target.getClass());
    }
    
    public boolean canSendNormalObject() {
        return false;
    }

    @Override
    public void send(E dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
        if (!canSendNormalObject() && !(msg instanceof ByteBuffer)) {
            throw new IllegalArgumentException("msg type should be ByteBuffer");
        }
        send(dst, (ByteBuffer) msg);
    }

    @Override
    public abstract RawChannel<E> newChannel(E dst, boolean isDuplex, 
            int timeout) throws IOException;
}
