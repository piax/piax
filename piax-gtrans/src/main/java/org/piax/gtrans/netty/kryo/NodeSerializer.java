package org.piax.gtrans.netty.kryo;

import org.piax.common.Endpoint;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.ddll.DdllKey;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NodeSerializer extends Serializer<Node> {
    public void write (Kryo kryo, Output output, Node node) {
        byte[] keyBytes = KryoUtil.encode(kryo, node.key, 256, 256);
        output.writeShort(keyBytes.length);
        output.writeBytes(keyBytes);
        byte[] epBytes = KryoUtil.encode(kryo, node.addr, 256, 256);
        output.writeShort(epBytes.length);
        output.writeBytes(epBytes);
    }

    public Node read (Kryo kryo, Input input, Class<Node> type) {
        short len = input.readShort();
        byte[] buf = new byte[len];
        input.readBytes(buf);
        Object obj = KryoUtil.decode(kryo, buf);
        DdllKey key = null;
        if (obj instanceof DdllKey) {
            key = (DdllKey)obj;
        }
        len = input.readShort();
        byte[] epbuf = new byte[len];
        input.readBytes(epbuf);
        obj = KryoUtil.decode(kryo, epbuf);
        Endpoint ep = null;
        if (obj instanceof Endpoint) {
            ep = (Endpoint)obj;
        }
        return Node.getInstance(key, ep);
    }
} 