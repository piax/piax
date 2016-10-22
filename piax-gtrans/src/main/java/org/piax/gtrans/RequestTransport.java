/*
 * RequestTransport.java - A Transport with Request/Response
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RequestTransport.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import java.io.IOException;

import org.piax.common.Destination;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;

/**
 * A Transport with Request/Response
 */
public interface RequestTransport<D extends Destination> extends Transport<D> {
    /*
    void setListener(ObjectId upper, RequestTransportListener<D> listener);
    RequestTransportListener<D> getListener(ObjectId upper);
    
    void setListener(RequestTransportListener<D> listener);
    RequestTransportListener<D> getListener();
	*/
    // for application
    /**
     * Send a request message. FutureQueue object is returned to access arrived responses.
     * @param sender the object ID of the sender.
     * @param receiver the object ID of the receiver.
     * @param dst the destination.
     * @param msg the request message.
     * @param timeout the timeout.
     * @return the future queue to access responses.
     * @throws ProtocolUnsupportedException thrown when the protocol is not supported.
     * @throws IOException thrown when an I/O error occurs.
     */
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
            Object msg, int timeout) throws ProtocolUnsupportedException,
            IOException;
    
    /**
     * Send a request message. FutureQueue object is returned to access arrived responses.
     * @param sender the object ID of the sender.
     * @param receiver the object ID of the receiver.
     * @param dst the destination.
     * @param msg the request message.
     * @param opts the options.
     * @return the future queue to access responses.
     * @throws ProtocolUnsupportedException thrown when the protocol is not supported.
     * @throws IOException thrown when an I/O error occurs.
     */
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
            Object msg, TransOptions opts) throws ProtocolUnsupportedException,
            IOException;
    
    /**
     * Send a request message. FutureQueue object is returned to access arrived responses.
     * @param sender the object ID of the sender.
     * @param receiver the object ID of the receiver.
     * @param dst the destination.
     * @param msg the request message.
     * @return the future queue to access responses.
     * @throws ProtocolUnsupportedException thrown when the protocol is not supported.
     * @throws IOException thrown when an I/O error occurs.
     */
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst, Object msg)
    			throws ProtocolUnsupportedException, IOException;

    /* Reduced argument versions of request */
    FutureQueue<?> request(ObjectId appId, D dst, Object msg)
			throws ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(ObjectId appId, D dst, Object msg, TransOptions opts)
			throws ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(D dst, Object msg)
			throws ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(D dst, Object msg, TransOptions opts)
			throws ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(D dst, Object msg, int timeout)
			throws ProtocolUnsupportedException, IOException;
    
    // for Transport
    /**
     * Send a request message. FutureQueue object is returned to access arrived responses.
     * @param upperTrans the transport ID of the transport.
     * @param dst the destination.
     * @param msg the request message.
     * @param timeout the timeout.
     * @return the future queue to access responses.
     * @throws ProtocolUnsupportedException thrown when the protocol is not supported.
     * @throws IOException thrown when an I/O error occurs.
     */
    FutureQueue<?> request(TransportId upperTrans, D dst, Object msg,
            int timeout) throws ProtocolUnsupportedException, IOException;
    
    /**
     * Send a request message. FutureQueue object is returned to access arrived responses.
     * @param upperTrans the transport ID of the transport.
     * @param dst the destination.
     * @param msg the request message.
     * @param opts the options.
     * @return the future queue to access responses.
     * @throws ProtocolUnsupportedException thrown when the protocol is not supported.
     * @throws IOException thrown when an I/O error occurs.
     */
    FutureQueue<?> request(TransportId upperTrans, D dst, Object msg,
            TransOptions opts) throws ProtocolUnsupportedException, IOException;
    
    /**
     * Send a request message. FutureQueue object is returned to access arrived responses.
     * @param upperTrans the transport ID of the transport.
     * @param dst the destination.
     * @param msg the request message.
     * @return the future queue to access responses.
     * @throws ProtocolUnsupportedException thrown when the protocol is not supported.
     * @throws IOException thrown when an I/O error occurs.
     */
    FutureQueue<?> request(TransportId upperTrans, D dst, Object msg) throws ProtocolUnsupportedException, IOException;
}
