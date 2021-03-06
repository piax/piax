/*
 * NettyOutboundHandler.java - An OutboundHandler
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.loctrans;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyOutboundHandler extends ChannelInboundHandlerAdapter {
    final NettyChannelTransport trans;
    final NettyRawChannel raw;
    /**
     * Creates a client-side handler.
     */
    public NettyOutboundHandler(NettyRawChannel raw, NettyChannelTransport trans) {
        this.raw = raw;
        this.trans = trans;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // this handler is a client-side handler.
        trans.outboundActive(raw, ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        trans.getPeer().execute(() -> {
            trans.outboundReceive(raw, ctx, msg);
        });
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // this handler is a client-side handler.
        trans.outboundInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        trans.outboundInactive(ctx);
    }

}
