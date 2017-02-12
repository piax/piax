package org.piax.gtrans.netty.bootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ThreadFactory;

import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyInboundHandler;
import org.piax.gtrans.netty.NettyOutboundHandler;
import org.piax.gtrans.netty.NettyRawChannel;

public class UdtBootstrap implements NettyBootstrap {
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    EventLoopGroup clientGroup;

    public UdtBootstrap() {
        ThreadFactory bossFactory = new DefaultThreadFactory("parent");
        ThreadFactory serverFactory = new DefaultThreadFactory("child");
        ThreadFactory clientFactory = new DefaultThreadFactory("client");

        parentGroup = new NioEventLoopGroup(1, bossFactory, NioUdtProvider.BYTE_PROVIDER);
        childGroup = new NioEventLoopGroup(10, serverFactory, NioUdtProvider.BYTE_PROVIDER);
        clientGroup = new NioEventLoopGroup(10, clientFactory, NioUdtProvider.BYTE_PROVIDER);
    }

    private ChannelInitializer<?> getChannelInboundInitializer(
            NettyChannelTransport trans) {
        return new ChannelInitializer<UdtChannel>() {
            @Override
            public void initChannel(UdtChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(new NettyInboundHandler(trans));
            }
        };
    }

     private ChannelInitializer<?> getChannelOutboundInitializer(
            NettyRawChannel raw, NettyChannelTransport trans) {
        return new ChannelInitializer<UdtChannel>() {
            @Override
            public void initChannel(UdtChannel sch)
                    throws Exception {
                ChannelPipeline p = sch.pipeline();
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(new NettyOutboundHandler(raw, trans));
            }
        };
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
    public ServerBootstrap getServerBootstrap(NettyChannelTransport trans) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
        .channelFactory(NioUdtProvider.BYTE_ACCEPTOR)
        //.channel(transType.getServerChannelClass())//NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 10)
        .option(ChannelOption.SO_REUSEADDR, true);
        //.option(ChannelOption.AUTO_READ, true)
        b.handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getChannelInboundInitializer(trans));
        return b;
    }

    @Override
    public Bootstrap getBootstrap(NettyRawChannel raw, NettyChannelTransport trans) {
        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
        //.channel(transType.getChannelClass())
        .channelFactory(NioUdtProvider.BYTE_CONNECTOR)
        .handler(getChannelOutboundInitializer(raw, trans));
        return b;
    }

}
