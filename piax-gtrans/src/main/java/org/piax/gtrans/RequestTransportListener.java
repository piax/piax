/*
 * RequestTransportListener.java - A Listener for RequestTransport.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RequestTransportListener.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import org.piax.common.Destination;


/**
 * A Listener for RequestTransport.
 */
public interface RequestTransportListener<D extends Destination> extends
        TransportListener<D> {

    void onReceive(RequestTransport<D> trans, ReceivedMessage rmsg);
    
    /**
     * 
     * @param trans the request transport.
     * @param rmsg the received message.
     * @return future queue.
     */
    FutureQueue<?> onReceiveRequest(RequestTransport<D> trans,
            ReceivedMessage rmsg);
    
    // valid on Java 8 API
    default public void onReceive(Transport<D> trans, ReceivedMessage rmsg) {
    }
}
