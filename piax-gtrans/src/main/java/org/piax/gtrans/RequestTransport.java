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
     * 
     * @param sender
     * @param receiver
     * @param dst
     * @param msg
     * @param timeout
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
            Object msg, int timeout) throws ProtocolUnsupportedException,
            IOException;
    
    /**
     * 
     * @param sender
     * @param receiver
     * @param dst
     * @param msg
     * @param opts
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
            Object msg, TransOptions opts) throws ProtocolUnsupportedException,
            IOException;
    
    /**
     * The constructor with default options.
     * @param sender
     * @param receiver
     * @param dst
     * @param msg
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
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
     * 
     * @param upperTrans
     * @param dst
     * @param msg
     * @param timeout
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    FutureQueue<?> request(TransportId upperTrans, D dst, Object msg,
            int timeout) throws ProtocolUnsupportedException, IOException;
    
    /**
     * 
     * @param upperTrans
     * @param dst
     * @param msg
     * @param opts
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    FutureQueue<?> request(TransportId upperTrans, D dst, Object msg,
            TransOptions opts) throws ProtocolUnsupportedException, IOException;
    
    /**
     * The constructor with default options.
     * @param upperTrans
     * @param dst
     * @param msg
     * @return
     * @throws ProtocolUnsupportedException
     * @throws IOException
     */
    FutureQueue<?> request(TransportId upperTrans, D dst, Object msg) throws ProtocolUnsupportedException, IOException;
}
