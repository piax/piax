/*
 * ChannelTransportImpl.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ChannelTransportImpl.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public abstract class ChannelTransportImpl<E extends Endpoint> extends
        TransportImpl<E> implements ChannelTransport<E> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChannelTransportImpl.class);

    protected final boolean supportsDuplex;

    protected final Map<ObjectId, ChannelListener<E>> chListenersByUpper =
            new ConcurrentHashMap<ObjectId, ChannelListener<E>>();

    protected ChannelTransportImpl(Peer peer, TransportId transId,
            Transport<?> lowerTrans, boolean supportsDuplex)
            throws IdConflictException {
        super(peer, transId, lowerTrans);
        this.supportsDuplex = supportsDuplex;
    }

    /**
     * RawTransportのように、ServiceIdを持たないTransportを生成する。
     * 
     * @param peerId
     * @param supportsDuplex
     */
    protected ChannelTransportImpl(Peer peer, boolean supportsDuplex) {
        super(peer);
        this.supportsDuplex = supportsDuplex;
    }

    @Override
    public void fin() {
        super.fin();
        chListenersByUpper.clear();
    }

    public boolean supportsDuplex() {
        return supportsDuplex;
    }

    public void setChannelListener(ObjectId upper, ChannelListener<E> listener) {
        logger.trace("ENTRY:");
        logger.debug("transId {}", transId);
        logger.debug("receiver {}", upper);
        if (listener == null) {
            chListenersByUpper.remove(upper);
        } else {
            this.checkActive();
            chListenersByUpper.put(upper, listener);
        }
    }

    public ChannelListener<E> getChannelListener(ObjectId upper) {
        return chListenersByUpper.get(upper);
    }
    
    public void setChannelListener(ChannelListener<E> listener) {
		setChannelListener(getDefaultAppId(), listener);
    }
    
    public ChannelListener<E> getChannelListener() {
		return getChannelListener(getDefaultAppId());
    }

    public Channel<E> newChannel(ObjectId sender, ObjectId receiver,
            E dst) throws ProtocolUnsupportedException, IOException {
        return newChannel(sender, receiver, dst, true,
        		GTransConfigValues.newChannelTimeout);
    }

    public Channel<E> newChannel(ObjectId sender, ObjectId receiver,
            E dst, int timeout) throws ProtocolUnsupportedException,
            IOException {
        return newChannel(sender, receiver, dst, true, timeout);
    }

    public Channel<E> newChannel(ObjectId sender, ObjectId receiver,
            E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException {
        return newChannel(sender, receiver, dst, isDuplex,
                GTransConfigValues.newChannelTimeout);
    }
    
    // Reduced arguments versions.
    public Channel<E> newChannel(ObjectId appId, E dst)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(appId, appId, dst);
    }
    
    public Channel<E> newChannel(E dst)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(getDefaultAppId(), dst);
    }
    
    public Channel<E> newChannel(ObjectId appId, E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(appId, appId, dst, isDuplex);
    }
    
    public Channel<E> newChannel(E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(getDefaultAppId(), dst, isDuplex);
    }
    
    public Channel<E> newChannel(ObjectId appId, E dst, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(appId, appId, dst, true, opts);
    }
    
    public Channel<E> newChannel(E dst, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(getDefaultAppId(), getDefaultAppId(), dst, true, opts);
    }
    
    public Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            boolean isDuplex, TransOptions opts) throws ProtocolUnsupportedException,
            IOException {
    		// XXX We should distinguish the 'response timeout' and 'connection timeout'.
    		// But we mixed them up. 
    		return newChannel(sender, receiver, dst, isDuplex, (int)TransOptions.timeout(opts));
    }
    
    public abstract Channel<E> newChannel(ObjectId sender, ObjectId receiver,
    	    E dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException;

    public Channel<E> newChannel(TransportId upperTrans, E dst)
            throws ProtocolUnsupportedException, IOException {
    		return newChannel(upperTrans, upperTrans, dst);
    }

    public Channel<E> newChannel(TransportId upperTrans, E dst, int timeout)
            throws ProtocolUnsupportedException, IOException {
        return newChannel(upperTrans, upperTrans, dst, timeout);
    }

    public Channel<E> newChannel(TransportId upperTrans, E dst,
            boolean isDuplex) throws ProtocolUnsupportedException, IOException {
        return newChannel(upperTrans, upperTrans, dst, isDuplex);
    }

    public Channel<E> newChannel(TransportId upperTrans, E dst,
            boolean isDuplex, int timeout) throws ProtocolUnsupportedException,
            IOException {
        return newChannel(upperTrans, upperTrans, dst, isDuplex, timeout);
    }
    
    public Channel<E> newChannel(TransportId upperTrans, E dst,
            boolean isDuplex, TransOptions opts) throws ProtocolUnsupportedException,
            IOException {
        return newChannel(upperTrans, upperTrans, dst, isDuplex, opts);
    }
    
    @Override
    public String toString0() {
        return super.toString0()
                + ", chListeners=" + chListenersByUpper.keySet();
    }
}
