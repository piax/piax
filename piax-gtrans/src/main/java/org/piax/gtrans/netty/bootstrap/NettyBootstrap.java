/*
 * NettyBootstrap.java - Bootstrap for Netty
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.bootstrap;

import org.piax.common.Option.EnumOption;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.kryo.KryoDecoder;
import org.piax.gtrans.netty.kryo.KryoEncoder;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
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
    public abstract AbstractBootstrap getServerBootstrap(ChannelInboundHandlerAdapter ihandler);// { return null; }

    public enum SerializerType {
        Java, Kryo
    }

    // by default, use Kryo serializer.
    //public static SerializerType SERIALIZER = SerializerType.Kryo;
    
    public static EnumOption<SerializerType> SERIALIZER
        = new EnumOption<>(SerializerType.class, SerializerType.Kryo, "-serializer");

    protected void setupSerializers(ChannelPipeline p) {
        switch(SERIALIZER.value()) {
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
    
    public ChannelFuture connect(Bootstrap b, String host, int port) {
        return b.connect(host, port);
    }
}
