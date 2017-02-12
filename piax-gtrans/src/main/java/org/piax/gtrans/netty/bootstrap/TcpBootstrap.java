package org.piax.gtrans.netty.bootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyInboundHandler;
import org.piax.gtrans.netty.NettyOutboundHandler;
import org.piax.gtrans.netty.NettyRawChannel;

public class TcpBootstrap implements NettyBootstrap {
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    EventLoopGroup clientGroup;

    public TcpBootstrap() {
        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup(10);
        clientGroup = new NioEventLoopGroup(10);
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

    private ChannelInitializer<?> getChannelInboundInitializer(
            NettyChannelTransport trans) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(new NettyInboundHandler(trans));
            }
        };
    }

    private ChannelInitializer<?> getChannelOutboundInitializer(NettyRawChannel raw, NettyChannelTransport trans) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel sch)
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
    public ServerBootstrap getServerBootstrap(NettyChannelTransport trans) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.AUTO_READ, true);
        //b.handler(new LoggingHandler(LogLevel.INFO))
        b.childHandler(getChannelInboundInitializer(trans));
        return b;
    }

    @Override
    public Bootstrap getBootstrap(NettyRawChannel raw, NettyChannelTransport trans) {
        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
        .channel(NioSocketChannel.class)
        .handler(getChannelOutboundInitializer(raw, trans));
        return b;
    }

}
