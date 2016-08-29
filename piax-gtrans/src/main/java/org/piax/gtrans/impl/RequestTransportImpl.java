/*
 * RequestTransportImpl.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RequestTransportImpl.java 719 2013-07-08 01:58:25Z yos $
 */

package org.piax.gtrans.impl;

import java.io.IOException;
import java.io.Serializable;

import org.piax.common.Destination;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.RequestTransportListener;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public abstract class RequestTransportImpl<D extends Destination> extends
        TransportImpl<D> implements RequestTransport<D> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RequestTransportImpl.class);

    /**
     * @param peerId
     * @param transId
     * @throws IdConflictException
     */
    protected RequestTransportImpl(Peer peer, TransportId transId, Transport<?> lowerTrans)
            throws IdConflictException {
        super(peer, transId, lowerTrans);
    }

    /*
    public void setListener(ObjectId upper, RequestTransportListener<D> listener) {
        super.setListener(upper, listener);
    }
    
    public void setListener(RequestTransportListener<D> listener) {
        super.setListener(getDefaultAppId(), listener);
    }
    
    @Override
    public RequestTransportListener<D> getListener(ObjectId upper) {
        return (RequestTransportListener<D>) super.getListener(upper);
    }

    public RequestTransportListener<D> getListener() {
        return (RequestTransportListener<D>) super.getListener(getDefaultAppId());
    }
    */
    
    protected TransportListener<D> getListener0(ObjectId upper) {
        return super.getListener(upper);
    }
    
    public FutureQueue<?> request(ObjectId appId, D dst, Object msg)
			throws ProtocolUnsupportedException, IOException {
    	return request(appId, appId, dst, msg);
    }
    
    public FutureQueue<?> request(ObjectId appId, D dst, Object msg, TransOptions opts)
			throws ProtocolUnsupportedException, IOException {
    		return request(appId, appId, dst, msg, opts);
    }
    
    public FutureQueue<?> request(D dst, Object msg)
			throws ProtocolUnsupportedException, IOException {
    		return request(getDefaultAppId(), dst, msg);
    }
    
    public FutureQueue<?> request(D dst, Object msg, TransOptions opts)
			throws ProtocolUnsupportedException, IOException {
    		return request(getDefaultAppId(), dst, msg, opts);
    }
    
    public FutureQueue<?> request(D dst, Object msg, int timeout)
			throws ProtocolUnsupportedException, IOException {
    		return request(getDefaultAppId(), getDefaultAppId(), dst, msg, timeout);
    }
    
    
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst, Object msg)
    		throws ProtocolUnsupportedException, IOException {
    		return request(sender, receiver, dst, msg, null);
    }
    
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst, Object msg, int timeout)
    		throws ProtocolUnsupportedException, IOException {
    		return request(sender, receiver, dst, msg, new TransOptions(timeout));
    }
    
    public FutureQueue<?> request(TransportId upperTrans, D dst, Object msg, int timeout)
    		throws ProtocolUnsupportedException,
            IOException {
        return request(upperTrans, upperTrans, dst, msg, timeout);
    }
    
    public FutureQueue<?> request(TransportId upperTrans, D dst, Object msg) throws ProtocolUnsupportedException,
            IOException {
        return request(upperTrans, upperTrans, dst, msg, null);
    }
    
    public FutureQueue<?> request(TransportId upperTrans, D dst, Object msg, TransOptions opts)
    		throws ProtocolUnsupportedException,
    IOException {
    		return request(upperTrans, upperTrans, dst, msg, opts);
    }
    
    static class IsEasySend implements Serializable {
        private static final long serialVersionUID = 1L;
        final Object msg;
        public IsEasySend(Object msg) {
            this.msg = msg;
        }
    }

    public void send(ObjectId sender, ObjectId receiver, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        // Default implementation is to use request as a send.
        TransOptions newOpts = new TransOptions(opts);
        newOpts.setResponseType(ResponseType.NO_RESPONSE);
        request(sender, receiver, dst, new IsEasySend(msg), newOpts);
    }
    
    public void send(ObjectId sender, ObjectId receiver, D dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
    		send(sender, receiver, dst, msg, null);
    }

    protected static final Object NON = new Object();
    protected Object checkAndClearIsEasySend(Object msg) {
        if (msg instanceof IsEasySend) {
            return ((IsEasySend) msg).msg;
        } else if (msg instanceof NestedMessage) {
            NestedMessage nmsg = (NestedMessage) msg;
            Object inn = checkAndClearIsEasySend(nmsg.getInner());
            if (NON == inn) {
                return NON;
            }
            return new NestedMessage(nmsg, inn);
        }
        return NON;
    }
    
    protected FutureQueue<?> selectOnReceive(RequestTransportListener<D> listener,
            RequestTransport<D> trans, ReceivedMessage rmsg) {
        logger.trace("ENTRY:");
        Object msg = rmsg.getMessage();
        logger.debug("msg {}", msg);
        Object inn = checkAndClearIsEasySend(msg);
        if (NON != inn) {
            rmsg.setMessage(inn);
            logger.debug("select onReceive: trans:{}", trans.getTransportId());
            listener.onReceive(trans, rmsg);
            return FutureQueue.emptyQueue();
        } else {
            return listener.onReceiveRequest(trans, rmsg);
        }
    }
}
