package org.piax.gtrans.netty.udp;

import org.piax.common.ComparableKey;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.kryo.KryoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class UdpPrimaryKeySerializer extends Serializer<UdpPrimaryKey> {
    protected static final Logger logger = LoggerFactory.getLogger(UdpPrimaryKeySerializer.class);
    UdpLocatorManager mgr = null; 
    public void setLocatorManager(UdpLocatorManager mgr) {
        this.mgr = mgr;
    }
    
    public void write (Kryo kryo, Output output, UdpPrimaryKey key) {
        byte[] keyBytes = KryoUtil.encode(key.getRawKey(), 256, 256);
        output.writeShort(keyBytes.length);
        output.writeBytes(keyBytes);
        byte[] epBytes = KryoUtil.encode(key.getLocator(), 256, 256);
        output.writeShort(epBytes.length);
        output.writeBytes(epBytes);
        logger.trace("encode: {} -> {}", key.getRawKey(), key.getLocator());
    }

    public UdpPrimaryKey read (Kryo kryo, Input input, Class<UdpPrimaryKey> type) {
        short len = input.readShort();
        byte[] buf = new byte[len];
        input.readBytes(buf);
        Object obj = KryoUtil.decode(buf);
        ComparableKey<?> key = null;
        if (obj instanceof ComparableKey<?>) {
            key = (ComparableKey<?>)obj;
        }
        len = input.readShort();
        byte[] epbuf = new byte[len];
        input.readBytes(epbuf);
        obj = KryoUtil.decode(epbuf);
        NettyLocator loc = null;
        if (obj instanceof NettyLocator) {
            loc = (NettyLocator)obj;
        }
        return new UdpPrimaryKey(key, loc);
    }
} 
