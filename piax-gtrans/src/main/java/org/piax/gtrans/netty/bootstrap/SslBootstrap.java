package org.piax.gtrans.netty.bootstrap;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyInboundHandler;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyOutboundHandler;
import org.piax.gtrans.netty.NettyRawChannel;

public class SslBootstrap<E extends NettyEndpoint> implements NettyBootstrap<E> {
//    private static final Logger logger = LoggerFactory.getLogger(NettySslTransport.class.getName());
    String host;
    int port;

    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    EventLoopGroup clientGroup;

    public SslBootstrap(String host, int port) {
        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup(NettyBootstrap.NUMBER_OF_THREADS_FOR_SERVER);
        clientGroup = new NioEventLoopGroup(NettyBootstrap.NUMBER_OF_THREADS_FOR_CLIENT);
        this.host = host;
        this.port = port;
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
            NettyChannelTransport<E> trans) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                final SslContext sslCtx;
                SelfSignedCertificate ssc = null;
                try {
                    ssc = new SelfSignedCertificate();
                } catch (CertificateException e) {
                    e.printStackTrace();
                }
                if (ssc != null) {
                    sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
                }
                else {
                    sslCtx = null;
                }
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc()));
                }
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(new NettyInboundHandler(trans));
            }
        };
    }

    private ChannelInitializer<?> getChannelInboundInitializer(ChannelInboundHandlerAdapter ihandler) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                final SslContext sslCtx;
                SelfSignedCertificate ssc = null;
                try {
                    ssc = new SelfSignedCertificate();
                } catch (CertificateException e) {
                    e.printStackTrace();
                }
                if (ssc != null) {
                    sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
                }
                else {
                    sslCtx = null;
                }
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc()));
                }
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(ihandler);
            }
        };
    }

    private ChannelInitializer<?> getChannelOutboundInitializer(
            NettyRawChannel<E> raw, NettyChannelTransport<E> trans) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                
                final SslContext sslCtx;
                SslContext ssl;
                try {
                    ssl = SslContextBuilder.forClient()
                            .trustManager(InsecureTrustManagerFactory.INSTANCE)
                            .build();
                } catch (SSLException e) {
                    ssl = null;
                }
                sslCtx = ssl;
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc(), raw.getRemote().getHost(),
                            raw.getRemote().getPort()));
                }
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(new NettyOutboundHandler(raw, trans));
            }
        };
    }

    private ChannelInitializer<?> getChannelOutboundInitializer(
            NettyLocator raw, ChannelInboundHandlerAdapter ohandler) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                
                final SslContext sslCtx;
                SslContext ssl;
                try {
                    ssl = SslContextBuilder.forClient()
                            .trustManager(InsecureTrustManagerFactory.INSTANCE)
                            .build();
                } catch (SSLException e) {
                    ssl = null;
                }
                sslCtx = ssl;
                ChannelPipeline p = ch.pipeline();
                if (sslCtx != null) {
                    p.addLast(sslCtx.newHandler(ch.alloc(), raw.getHost(), raw.getPort()));
                }
                p.addLast(
                        new ObjectEncoder(),
                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                p.addLast(ohandler);
            }
        };
    }

    @Override
    public ServerBootstrap getServerBootstrap(NettyChannelTransport<E> trans) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.AUTO_READ, true);
        b.handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getChannelInboundInitializer(trans));
        return b;
    }

    @Override
    public ServerBootstrap getServerBootstrap(ChannelInboundHandlerAdapter ihandler) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.AUTO_READ, true);
        b.handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(getChannelInboundInitializer(ihandler));
        return b;
    }

    @Override
    public Bootstrap getBootstrap(NettyRawChannel<E> raw, NettyChannelTransport<E> trans) {
        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
        .channel(NioSocketChannel.class)
        .handler(getChannelOutboundInitializer(raw, trans));
        return b;
    }

    @Override
    public Bootstrap getBootstrap(NettyLocator locator, ChannelInboundHandlerAdapter ohandler) {
        Bootstrap b = new Bootstrap();
        b.group(clientGroup)
        .channel(NioSocketChannel.class)
        .handler(getChannelOutboundInitializer(locator, ohandler));
        return b;
    }

}
