/*
 * DirectUdpChannel.java - UDP channel with direct message exchange
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp.direct;

import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.udp.UdpChannelTransport;
import org.piax.gtrans.netty.udp.UdpPrimaryKey;
import org.piax.gtrans.netty.udp.UdpRawChannel;

import io.netty.channel.ChannelFuture;

public class DirectUdpChannel extends UdpRawChannel {
    final UdpChannelTransport trans;
    public DirectUdpChannel(UdpChannelTransport trans, UdpPrimaryKey src, UdpPrimaryKey dst) {
        super(src, dst);
        this.trans = trans;
    }

    @Override
    public ChannelFuture sendAsync(Object obj) throws UdpChannelException {
        logger.debug("dst={} src={}", dst, src);
        NettyLocator loc = trans.getPrimaryLocator(dst.getRawKey());
        logger.debug("loc={}", loc);
        return trans.rawSend(loc.getSocketAddress(), obj);
    }

    @Override
    public void close() {
        // XXX send removal request to the other side.
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

}
