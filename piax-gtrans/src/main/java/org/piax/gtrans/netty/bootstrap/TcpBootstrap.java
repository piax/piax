/*
 * TcpBootstrap.java - TCP setting
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.bootstrap;

import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class TcpBootstrap<E extends NettyEndpoint> extends NettyBootstrap<E> {
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    EventLoopGroup clientGroup;
    
    public TcpBootstrap() {
        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup(NettyBootstrap.NUMBER_OF_THREADS_FOR_SERVER);
        clientGroup = new NioEventLoopGroup(NettyBootstrap.NUMBER_OF_THREADS_FOR_CLIENT);
    }

    @Override
    public EventLoopGroup getParentEventLoopGroup() {
        return parentGroup;
    }

    @Override
    public EventLoopGroup getChildEventLoopGroup() {
        return childGroup;
    }

    @Override
    public EventLoopGroup getClientEventLoopGroup() {
        return clientGroup;
    }

    @Override
    public Bootstrap getBootstrap(NettyLocator dst,
            ChannelInboundHandlerAdapter ohandler) {
        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
        .channel(NioSocketChannel.class)
        .handler(getChannelOutboundInitializer(dst, ohandler));
        return b;
    }

    private ChannelHandler getChannelOutboundInitializer(NettyLocator dst,
            ChannelInboundHandlerAdapter ohandler) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel sch)
                    throws Exception {
                ChannelPipeline p = sch.pipeline();
                setupSerializers(p);
                p.addLast(ohandler);
            }
        };
    }

    @Override
    public ServerBootstrap getServerBootstrap(
            ChannelInboundHandlerAdapter ihandler) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.AUTO_READ, true);
        //b.handler(new LoggingHandler(LogLevel.INFO))
        b.childHandler(getChannelInboundInitializer(ihandler));
        return b;
    }

    private ChannelHandler getChannelInboundInitializer(
            ChannelInboundHandlerAdapter ihandler) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                setupSerializers(p);
                p.addLast(ihandler);
            }
        };
    }

}
