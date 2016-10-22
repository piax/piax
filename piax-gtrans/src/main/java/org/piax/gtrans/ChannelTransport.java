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
     * @return true when the channel supports duplex communication.
     */
    boolean supportsDuplex();

    /**
     * Set channel listener.
     * 
     * @param upper the object ID of the transport object to set listener.
     * @param listener the channel listener to set.
     */
    void setChannelListener(ObjectId upper, ChannelListener<E> listener);
    
    /**
     * Get channel listener.
     * @param upper the object ID
     * @return the channel listener.
     */
    ChannelListener<E> getChannelListener(ObjectId upper);
    
    /**
     * Set channel listener for the object which has default object ID.
     * 
     * @param listener the channel listner to set.
     */
    void setChannelListener(ChannelListener<E> listener);
    
    /**
     * Get channel listener for the object which has default object ID.
     * @return the channel listener.
     */
    ChannelListener<E> getChannelListener();
    
    E getEndpoint();

    // ** for channel based communication
    // for application
    /**
     * Create a new channel.
     * 
     * @param sender object ID of the sender. 
     * @param receiver object ID of the receiver. 
     * @param dst the endpoint of the destination to create channel. 
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst)
            throws ProtocolUnsupportedException, IOException;

    /**
     * Create a new channel with timeout.
     * 
     * @param sender object ID of the sender. 
     * @param receiver object ID of the receiver. 
     * @param dst the endpoint of the destination to create channel.
     * @param timeout the timeout length.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            int timeout) throws ProtocolUnsupportedException, IOException;

    /**
     * Create a new channel with isDuplex.
     * 
     * @param sender object ID of the sender. 
     * @param receiver object ID of the receiver. 
     * @param dst the endpoint of the destination to create channel.
     * @param isDuplex whether the connection is duplex or not.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            boolean isDuplex) throws ProtocolUnsupportedException, IOException;

    /**
     * Create a new channel with isDuplex and timeout.
     * 
     * @param sender object ID of the sender. 
     * @param receiver object ID of the receiver. 
     * @param dst the endpoint of the destination to create channel.
     * @param isDuplex whether the connection is duplex or not.
     * @param timeout the timeout length.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
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
     * Create a new channel for a transport object.
     * @param upperTrans the transport object.
     * @param dst the endpoint of the destination to create channel.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(TransportId upperTrans, E dst)
            throws ProtocolUnsupportedException, IOException;

    /**
     * Create a new channel for a transport object with timeout.
     * @param upperTrans the transport object.
     * @param dst the endpoint of the destination to create channel.
     * @param timeout timeout the timeout length.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(TransportId upperTrans, E dst, int timeout)
            throws ProtocolUnsupportedException, IOException;

    /**
     * Create a new channel for a transport object with isDuplex.
     * @param upperTrans the transport object.
     * @param dst the endpoint of the destination to create channel.
     * @param isDuplex whether the connection is duplex or not.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(TransportId upperTrans, E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException;

    /**
     * Create a new channel for a transport object with isDuplex and timeout.
     * @param upperTrans the transport object.
     * @param dst the endpoint of the destination to create channel.
     * @param isDuplex whether the connection is duplex or not.
     * @param timeout timeout the timeout length.
     * @return a new instance of channel.
     * @throws ProtocolUnsupportedException an exception thrown when the protocol is not supported.
     * @throws IOException an exception when thrown when an I/O error occurred.
     */
    Channel<E> newChannel(TransportId upperTrans, E dst, boolean isDuplex,
            int timeout) throws ProtocolUnsupportedException, IOException;
    
    // opts version.
    Channel<E> newChannel(TransportId upperTrans, E dst, boolean isDuplex, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
}
