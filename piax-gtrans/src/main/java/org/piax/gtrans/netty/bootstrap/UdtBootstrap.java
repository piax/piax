/*
 * UdtBootstrap.java - UDT setting
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.bootstrap;

import java.util.concurrent.ThreadFactory;

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
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

public class UdtBootstrap<E extends NettyEndpoint> extends NettyBootstrap<E> {
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    EventLoopGroup clientGroup;

    public UdtBootstrap() {
        ThreadFactory bossFactory = new DefaultThreadFactory("parent");
        ThreadFactory serverFactory = new DefaultThreadFactory("child");
        ThreadFactory clientFactory = new DefaultThreadFactory("client");

        parentGroup = new NioEventLoopGroup(1, bossFactory, NioUdtProvider.BYTE_PROVIDER);
        childGroup = new NioEventLoopGroup(NettyBootstrap.NUMBER_OF_THREADS_FOR_SERVER, serverFactory, NioUdtProvider.BYTE_PROVIDER);
        clientGroup = new NioEventLoopGroup(NettyBootstrap.NUMBER_OF_THREADS_FOR_CLIENT, clientFactory, NioUdtProvider.BYTE_PROVIDER);
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
        //.channel(transType.getChannelClass())
        .channelFactory(NioUdtProvider.BYTE_CONNECTOR)
        .handler(getChannelOutboundInitializer(dst, ohandler));
        return b;
    }

    private ChannelHandler getChannelOutboundInitializer(NettyLocator dst,
            ChannelInboundHandlerAdapter ohandler) {
        return new ChannelInitializer<UdtChannel>() {
            @Override
            public void initChannel(UdtChannel sch)
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
        .channelFactory(NioUdtProvider.BYTE_ACCEPTOR)
        //.channel(transType.getServerChannelClass())//NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 10)
        .option(ChannelOption.SO_REUSEADDR, true);
        //.option(ChannelOption.AUTO_READ, true)
        b.handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getChannelInboundInitializer(ihandler));
        return b;
    }

    private ChannelHandler getChannelInboundInitializer(
            ChannelInboundHandlerAdapter ihandler) {
        return new ChannelInitializer<UdtChannel>() {
            @Override
            public void initChannel(UdtChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                setupSerializers(p);
                p.addLast(ihandler);
            }
        };
    }

}
