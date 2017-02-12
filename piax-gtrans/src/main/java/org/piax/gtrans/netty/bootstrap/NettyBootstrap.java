package org.piax.gtrans.netty.bootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;

import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyRawChannel;

public interface NettyBootstrap {
    EventLoopGroup getParentEventLoopGroup();
    EventLoopGroup getChildEventLoopGroup();
    EventLoopGroup getClientEventLoopGroup();
    
    ServerBootstrap getServerBootstrap(NettyChannelTransport trans);
    Bootstrap getBootstrap(NettyRawChannel raw, NettyChannelTransport trans);
}
