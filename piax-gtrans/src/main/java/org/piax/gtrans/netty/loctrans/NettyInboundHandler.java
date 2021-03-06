/*
 * NettyInboundHandler.java - An InboundHandler
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.loctrans;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@Sharable
public class NettyInboundHandler extends ChannelInboundHandlerAdapter {
    NettyChannelTransport trans;
    public NettyInboundHandler(NettyChannelTransport trans) {
        this.trans = trans;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        trans.inboundActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        trans.getPeer().execute(() -> {
            trans.inboundReceive(ctx, msg);
        });
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        trans.inboundInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
