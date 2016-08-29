/*
 * RawChannel.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RawChannel.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.piax.common.ObjectId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.NetworkTimeoutException;

/**
 * 
 */
public abstract class RawChannel<E extends PeerLocator> implements Channel<E> {
    
    /**
     * Channelをcloseする際に、ブロック中のreceiveメソッドを正常に終了させるためのpoisonオブジェクト 
     */
    private static final Object END_OF_MESSAGE = new Object();
    
    private final BlockingQueue<Object> rcvQueue = new LinkedBlockingQueue<Object>();
    protected volatile boolean isActive = true;

    /**
     * RawChannelをcloseする。
     * 尚、すでにcloseされたRawChannelを再びcloseしても問題は起こらない。
     */
    public void close() {
        try {
            rcvQueue.clear();
            rcvQueue.put(END_OF_MESSAGE);
            isActive = false;
        } catch (InterruptedException ignore) {
        }
    }
    
    public boolean isClosed() {
        return !isActive;
    }
    
    public abstract int getChannelNo();

    // TODO このメソッドが本当に必要か要検討
    /**
     * RawChannelの相手側のPeerLocatorを取得する。
     * 
     * @return RawChannelの相手側のPeerLocator
     */
    public abstract E getRemote();
    public abstract boolean isCreatorSide();
    
    public abstract void send(ByteBuffer bbuf) throws IOException;

    public void send(Object msg) throws IOException {
        if (!(msg instanceof ByteBuffer)) {
            throw new IllegalArgumentException("msg type should be ByteBuffer");
        }
        send((ByteBuffer) msg);
    }
    
    protected void putReceiveQueue(Object msg) {
        try {
            rcvQueue.put(msg);
        } catch (InterruptedException ignore) {
        }
    }

    public Object receive() {
        try {
            return receive(0);
        } catch (NetworkTimeoutException e) {
            return null;
        }
    }

    public Object receive(int timeout) throws NetworkTimeoutException {
        try {
            Object msg = rcvQueue.poll(timeout, TimeUnit.MILLISECONDS);
            if (msg == END_OF_MESSAGE) return null;
            if (msg == null) {
                throw new NetworkTimeoutException("ch.receive timed out");
            }
            return msg;
        } catch (InterruptedException e) {
            return null;
        }
    }

    //--- the following methods are all unsupported ---

    public TransportId getTransportId() {
        throw new UnsupportedOperationException();
    }

    public E getLocal() {
        throw new UnsupportedOperationException();
    }

    public ObjectId getLocalObjectId() {
        throw new UnsupportedOperationException();
    }

    public ObjectId getRemoteObjectId() {
        throw new UnsupportedOperationException();
    }

    public boolean isDuplex() {
        throw new UnsupportedOperationException();
    }
}
