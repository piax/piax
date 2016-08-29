/*
 * ChannelTransport.java - Transport for continuous message exchange.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: ChannelTransport.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;

/**
 * Transport for continuous message exchange.
 */
public interface ChannelTransport<E extends Endpoint> extends Transport<E> {
    
    /**
     * 
     * @return
     */
    boolean supportsDuplex();

    /**
     * 
     * @param upper
     * @param listener
     */
    void setChannelListener(ObjectId upper, ChannelListener<E> listener);
    
    /**
     * 
     * @param upper
     * @return
     */
    ChannelListener<E> getChannelListener(ObjectId upper);
    
    /**
     * @param listener
     */
    void setChannelListener(ChannelListener<E> listener);
    
    /**
     * 
     * @return
     */
    ChannelListener<E> getChannelListener();
    
    E getEndpoint();

    // ** for channel based communication
    // for application
    /**
     * 
     * @param sender
     * @param receiver
     * @param dst
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst)
            throws ProtocolUnsupportedException, IOException;

    /**
     * 
     * @param sender
     * @param receiver
     * @param dst
     * @param timeout
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            int timeout) throws ProtocolUnsupportedException, IOException;

    /**
     * 
     * @param sender
     * @param receiver
     * @param dst
     * @param isDuplex
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            boolean isDuplex) throws ProtocolUnsupportedException, IOException;

    /**
     * 
     * @param sender
     * @param receiver
     * @param dst
     * @param isDuplex
     * @param timeout
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            boolean isDuplex, int timeout) throws ProtocolUnsupportedException,
            IOException;

    // Reduced versions.
    Channel<E> newChannel(ObjectId appId, E dst)
            throws ProtocolUnsupportedException, IOException;
    
    Channel<E> newChannel(E dst)
            throws ProtocolUnsupportedException, IOException;
    
    Channel<E> newChannel(ObjectId appId, E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException;
    
    Channel<E> newChannel(E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException;
    
    Channel<E> newChannel(ObjectId appId, E dst, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
    Channel<E> newChannel(E dst, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            boolean isDuplex, TransOptions opts) throws ProtocolUnsupportedException,
            IOException;
    
    // for Transport
    /**
     * 
     * @param upperTrans
     * @param dst
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(TransportId upperTrans, E dst)
            throws ProtocolUnsupportedException, IOException;

    /**
     * 
     * @param upperTrans
     * @param dst
     * @param timeout
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(TransportId upperTrans, E dst, int timeout)
            throws ProtocolUnsupportedException, IOException;

    /**
     * 
     * @param upperTrans
     * @param dst
     * @param isDuplex
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(TransportId upperTrans, E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException;

    /**
     * 
     * @param upperTrans
     * @param dst
     * @param isDuplex
     * @param timeout
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    Channel<E> newChannel(TransportId upperTrans, E dst, boolean isDuplex,
            int timeout) throws ProtocolUnsupportedException, IOException;
    
    // opts version.
    Channel<E> newChannel(TransportId upperTrans, E dst, boolean isDuplex, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
}
