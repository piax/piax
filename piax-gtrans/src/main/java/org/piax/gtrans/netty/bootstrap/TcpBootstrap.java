package org.piax.gtrans.netty.bootstrap;

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
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.netty.ControlMessage;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyInboundHandler;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.NettyOutboundHandler;
import org.piax.gtrans.netty.NettyRawChannel;
import org.piax.gtrans.netty.kryo.KryoDecoder;
import org.piax.gtrans.netty.kryo.KryoEncoder;
import org.piax.gtrans.netty.kryo.KryoUtil;

import com.esotericsoftware.kryo.Kryo;

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
