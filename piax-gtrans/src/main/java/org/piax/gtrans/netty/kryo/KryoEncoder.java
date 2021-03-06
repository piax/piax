/*
 * KryoEncoder.java - An Encoder for Kryo
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.kryo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KryoEncoder extends MessageToByteEncoder<Object> {

    static int KRYO_BUFSIZE = 1024;
    static int KRYO_BUFSIZE_MAX = 256 * 1024 * 1024; // 256 MB
    
    public KryoEncoder() {
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object obj, ByteBuf out) throws Exception {
        byte[] buf = KryoUtil.encode(obj, KRYO_BUFSIZE, KRYO_BUFSIZE_MAX);
        out.writeInt(buf.length);
        out.writeBytes(buf);
    }
}
