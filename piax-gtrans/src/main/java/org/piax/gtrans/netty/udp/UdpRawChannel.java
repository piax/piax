/*
 * UdpRawChannel.java - A Raw Channel implementation
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;


// an abstract class that uses signaling 
// - to obtain a channel to dst by UDP.
// - to know the src IP address for the dst by UDP. 
// - to synchronize the channel status(opened/closed).
public abstract class UdpRawChannel implements Closeable {
    final protected UdpPrimaryKey dst;
    final protected UdpPrimaryKey src;
    protected static final Logger logger = LoggerFactory.getLogger(UdpRawChannel.class.getName());

    static public class UdpChannelException extends Exception {
        
    }
    
    public UdpRawChannel(UdpPrimaryKey src, UdpPrimaryKey dst) {
        this.src = src;
        this.dst = dst;
    }

    public abstract ChannelFuture sendAsync(Object obj) throws UdpChannelException;

    public abstract void close();
    
    public abstract boolean isClosed();
}
