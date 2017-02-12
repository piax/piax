package org.piax.gtrans.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

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
