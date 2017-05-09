package org.piax.gtrans.netty.bootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;

import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyRawChannel;

public interface NettyBootstrap<E extends NettyEndpoint> {
    EventLoopGroup getParentEventLoopGroup();
    EventLoopGroup getChildEventLoopGroup();
    EventLoopGroup getClientEventLoopGroup();
    
    ServerBootstrap getServerBootstrap(NettyChannelTransport<E> trans);
    Bootstrap getBootstrap(NettyRawChannel<E> raw, NettyChannelTransport<E> trans);

    Bootstrap getBootstrap(NettyLocator dst, ChannelInboundHandlerAdapter ohandler);// { return null; }
    ServerBootstrap getServerBootstrap(ChannelInboundHandlerAdapter ihandler);// { return null; }
}
