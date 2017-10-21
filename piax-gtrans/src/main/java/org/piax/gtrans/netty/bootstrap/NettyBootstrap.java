package org.piax.gtrans.netty.bootstrap;

import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.kryo.KryoDecoder;
import org.piax.gtrans.netty.kryo.KryoEncoder;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public abstract class NettyBootstrap<E extends NettyEndpoint> {

    static int NUMBER_OF_THREADS_FOR_CLIENT = 1;
    static int NUMBER_OF_THREADS_FOR_SERVER = 1;

    public abstract EventLoopGroup getParentEventLoopGroup();
    public abstract EventLoopGroup getChildEventLoopGroup();
    public abstract EventLoopGroup getClientEventLoopGroup();

    /*ServerBootstrap getServerBootstrap(NettyChannelTransport<E> trans);
    Bootstrap getBootstrap(NettyRawChannel<E> raw, NettyChannelTransport<E> trans);
     */
    public abstract Bootstrap getBootstrap(NettyLocator dst, ChannelInboundHandlerAdapter ohandler);// { return null; }
    public abstract ServerBootstrap getServerBootstrap(ChannelInboundHandlerAdapter ihandler);// { return null; }

    public enum SerializerType {
        Java, Kryo
    }

    // by default, use Kryo serializer.
    public static SerializerType SERIALIZER = SerializerType.Kryo;

    protected void setupSerializers(ChannelPipeline p) {
        switch(SERIALIZER) {
        case Kryo:
            p.addLast(
                    new KryoEncoder(),
                    new KryoDecoder());
            break;
        case Java:
            p.addLast(
                    new ObjectEncoder(),
                    new ObjectDecoder(ClassResolvers.cacheDisabled(GTransConfigValues.classLoaderForDeserialize)));
            break;
        }
    }
}
