/*
 * MonoTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: MonoTransport.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.raw;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.TransportListener;

/**
 * 
 */
public interface MonoTransport<E extends Endpoint> extends ChannelTransport<E> {
    
    void setListener(TransportListener<E> listener);
    TransportListener<E> getListener();
    void setChannelListener(ChannelListener<E> listener);
    ChannelListener<E> getChannelListener();

    // for connection less communication
    void send(E dst, Object msg) throws ProtocolUnsupportedException,
            IOException;

    // for connection based communication
    Channel<E> newChannel(E dst) throws ProtocolUnsupportedException,
            IOException;

    Channel<E> newChannel(E dst, int timeout)
            throws ProtocolUnsupportedException, IOException;

    Channel<E> newChannel(E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException;

    Channel<E> newChannel(E dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException;
}
