package org.piax.gtrans.netty;

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
