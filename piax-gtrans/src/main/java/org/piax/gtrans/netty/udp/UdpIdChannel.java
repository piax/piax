/*
 * UdpIdChannel.java - IdChannel on UDP
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.netty.udp.UdpRawChannel.UdpChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;

public class UdpIdChannel implements Channel<UdpPrimaryKey> {

    final ObjectId localObjectId;
    final ObjectId remoteObjectId;
    final UdpPrimaryKey channelInitiator;
    final UdpPrimaryKey dst;
    UdpRawChannel raw;
    
    final boolean isCreator;
    final UdpChannelTransport trans;
    
    private final BlockingQueue<Object> rcvQueue;
    final int id;
    boolean isClosed;
    long timestamp;
    private static final Logger logger = LoggerFactory.getLogger(UdpIdChannel.class.getName());

    public UdpIdChannel(int channelNo, UdpPrimaryKey channelInitiator, UdpPrimaryKey destination,
            ObjectId localObjectId, ObjectId remoteObjectId, boolean isCreator,
            UdpRawChannel raw, UdpChannelTransport udpChannelTransport) {
        this.id = channelNo;
        this.channelInitiator = channelInitiator;
        this.dst = destination;
        this.localObjectId = localObjectId;
        this.remoteObjectId = remoteObjectId;
        this.isCreator = isCreator;
        this.raw = raw;
        this.trans = udpChannelTransport;
        this.isClosed = false;
        this.timestamp = System.currentTimeMillis();
        rcvQueue = new LinkedBlockingQueue<Object>();
    }
    
    static String getKeyString(int id, PrimaryKey key) {
        return "" + id + key.hashCode();
    }
    
    String getKeyString() {
        return getKeyString(id, channelInitiator);
    }

    @Override
    public void close() {
        raw.close();
    }

    public long elapsedTimeAfterClose() {
        return System.currentTimeMillis() - timestamp;
    }

    @Override
    public boolean isClosed() {
        //return raw.isClosed();
        return isClosed;
    }

    @Override
    public TransportId getTransportId() {
        return trans.getTransportId();
    }

    @Override
    public int getChannelNo() {
        return id;
    }

    @Override
    public UdpPrimaryKey getLocal() {
        return (UdpPrimaryKey)trans.getEndpoint();
    }

    @Override
    public ObjectId getLocalObjectId() {
        return localObjectId;
    }

    @Override
    public UdpPrimaryKey getRemote() {
        return dst;
    }

    @Override
    public ObjectId getRemoteObjectId() {
        return remoteObjectId;
    }

    @Override
    public boolean isDuplex() {
        return true;
    }

    @Override
    public boolean isCreatorSide() {
        return channelInitiator.equals(trans.getEndpoint());
    }

    public PrimaryKey getChannelInitiator() {
        return channelInitiator;
    }

    @Override
    public void send(Object msg) throws IOException {
        PrimaryKey src = trans.getEndpoint();
        NettyMessage<PrimaryKey> nmsg = new NettyMessage<PrimaryKey>(remoteObjectId, src,
                dst,
                getChannelInitiator(), trans.getPeerId(), msg, true,
                getChannelNo());
        if (!raw.isClosed()) {
            logger.debug("ch {}{} send {} from {} to {}", getChannelNo(), getChannelInitiator(), msg, trans.getEndpoint(), getRemote());
        }
        else { // re-create the raw channel.
            logger.debug("locator channel is closed");
        }

        // returns null if transport is finished.
        if (raw != null) {
            try {
                raw.sendAsync(nmsg);
            } catch (UdpChannelException e) {
                throw new IOException(e);
            }
        }
        else {
            logger.debug("locator channel is null");
        }
    }

    public ChannelFuture sendAsync(NettyMessage<UdpPrimaryKey> nmsg) throws Exception {
        if (!raw.isClosed()) {
            logger.debug("ch {}{} send {} from {} to {}", getChannelNo(), getChannelInitiator(), nmsg, trans.getEndpoint(), getRemote());
        }
        else { // re-create the raw channel?
            logger.debug("channel for {} is already closed.", nmsg);
        }
        return raw.sendAsync(nmsg);
    }
    
    private static final Object EOF = new Object();
    protected void putReceiveQueue(Object msg) {
        try {
            if (msg == null) {
                rcvQueue.put(EOF);
            } else {
                rcvQueue.put(msg);
            }
        } catch (InterruptedException ignore) {
            ignore.printStackTrace();
        }
    }

    public Object receive() {
        Object msg = rcvQueue.poll();
        logger.debug("ch {} received {} on {} thread={}", getChannelNo(), msg, trans.getEndpoint(), Thread.currentThread());
        if (msg == EOF) {
            logger.debug("ch {} received EOF on {}", getChannelNo(), msg, trans.getEndpoint());
            isClosed = true;
            //trans.deleteChannel(this);
            return null;
        }
        return msg;
    }

    public Object receive(int timeout) throws NetworkTimeoutException {
        try {
            Object msg = rcvQueue.poll(timeout, TimeUnit.MILLISECONDS);
            logger.debug("ch received {} on {} thread={}", msg, this, Thread.currentThread());
            if (msg == EOF) {
                logger.debug("ch {} received EOF on {}", getChannelNo(), msg, trans.getEndpoint());
                isClosed = true;
                //trans.deleteChannel(this);
                return null;
            }
            if (msg == null) {
                throw new NetworkTimeoutException("ch.receive timed out");
            }
            return msg;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public String toString() {
        return getRemote().toString() + ":" + getLocalObjectId()+":" + getChannelNo();
    }

}
