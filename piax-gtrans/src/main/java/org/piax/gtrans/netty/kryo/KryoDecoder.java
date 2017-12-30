package org.piax.gtrans.netty.kryo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;

import com.esotericsoftware.kryo.Kryo;

public class KryoDecoder extends ByteToMessageDecoder {
    public KryoDecoder() {
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws Exception {
        if (in.readableBytes() < 2)
            return;
        in.markReaderIndex();
        int len = in.readInt();
        if (in.readableBytes() < len) {
            in.resetReaderIndex();
            return;
        }
        byte[] buf = new byte[len];
        in.readBytes(buf);
        Object object = KryoUtil.decode(buf);
        out.add(object);
    }
}
