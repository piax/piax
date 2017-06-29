package org.piax.gtrans.netty.kryo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.esotericsoftware.kryo.Kryo;

public class KryoEncoder extends MessageToByteEncoder<Object> {

    private final Kryo kryo;
    static int KRYO_BUFSIZE = 4096;
    
    public KryoEncoder(Kryo kryo) {
        this.kryo = kryo;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object obj, ByteBuf out) throws Exception {
        byte[] buf = KryoUtil.encode(kryo, obj, KRYO_BUFSIZE);
        out.writeShort(buf.length);
        out.writeBytes(buf);
    }
}
