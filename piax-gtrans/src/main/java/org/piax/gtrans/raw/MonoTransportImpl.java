/*
 * MonoTransportImpl.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: MonoTransportImpl.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.raw;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.ChannelTransportImpl;

/**
 * 
 */
public abstract class MonoTransportImpl<E extends Endpoint> extends
        ChannelTransportImpl<E> implements MonoTransport<E> {
    protected volatile TransportListener<E> listener;
    protected volatile ChannelListener<E> chListener;

    protected MonoTransportImpl(Peer peer, TransportId transId,
            Transport<?> lowerTrans, boolean supportsDuplex)
            throws IdConflictException {
        super(peer, transId, lowerTrans, supportsDuplex);
    }

    /**
     * RawTransportのように、ServiceIdを持たないTransportを生成する。
     * 
     * @param supportsDuplex
     */
    protected MonoTransportImpl(PeerId peerId, boolean supportsDuplex) {
        super(Peer.getInstance(peerId), supportsDuplex);
    }
    
    @Override
    public void fin() {
        listener = null;
        chListener = null;
        super.fin();
    }

    @Override
    public void setListener(ObjectId receiver, TransportListener<E> listener) {
        this.checkActive();
        // receiverは無視される
        this.listener = listener;
    }

    @Override
    public TransportListener<E> getListener(ObjectId receiver) {
        // receiverは無視される
        return listener;
    }

    public void setListener(TransportListener<E> listener) {
        this.checkActive();
        this.listener = listener;
    }

    public TransportListener<E> getListener() {
        return listener;
    }

    @Override
    public void setChannelListener(ObjectId receiver, ChannelListener<E> listener) {
        this.checkActive();
        // receiverは無視される
        this.chListener = listener;
    }

    @Override
    public ChannelListener<E> getChannelListener(ObjectId receiver) {
        // receiverは無視される
        return chListener;
    }

    public void setChannelListener(ChannelListener<E> listener) {
        this.checkActive();
        this.chListener = listener;
    }

    public ChannelListener<E> getChannelListener() {
        return chListener;
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, E dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
        // sender, receiverは無視される
        send(dst, msg);
    }

    public abstract void send(E dst, Object msg)
            throws ProtocolUnsupportedException, IOException;

    @Override
    public Channel<E> newChannel(ObjectId sender, ObjectId receiver,
            E dst, boolean isDuplex, int timeout)
            throws ProtocolUnsupportedException, IOException {
        // sender, receiverは無視される
        return newChannel(dst, isDuplex, timeout);
    }

    public Channel<E> newChannel(E dst)
            throws ProtocolUnsupportedException, IOException {
        return newChannel(dst, true, 0);
    }

    public Channel<E> newChannel(E dst, int timeout)
            throws ProtocolUnsupportedException, IOException {
        return newChannel(dst, true, timeout);
    }

    public Channel<E> newChannel(E dst, boolean isDuplex)
            throws ProtocolUnsupportedException, IOException {
        return newChannel(dst, isDuplex, 0);
    }
    
    public abstract Channel<E> newChannel(E dst, boolean isDuplex,
            int timeout) throws ProtocolUnsupportedException, IOException;
}
