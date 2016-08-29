/*
 * BinaryJsonner.java - A utility for Binary JSON
 * 
 * Copyright (c) 2009-2015 PIAX development team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: BinaryJsonner.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;

/**
 * 
 */
public class BinaryJsonner {
    /*
     * TODO
     * 仮実装の状態
     */
    
    public static final byte nullType = 0;
    public static final byte emuType = 1;
    public static final byte tcpType = 2;
    public static final byte udpType = 3;
    public static final byte btType = 4;
    public static final byte peerIdType = 11;
    public static final byte objectIdType = 12;
    public static final byte transportIdType = 13;
    public static final byte byteType = 20;
    public static final byte intType = 21;
    public static final byte bytesType = 22;
    public static final byte strType = 23;
    public static final byte otherJavaType = 99;
    
    static int merginBufSize = 256;
    static int incBufSize = 32000;

    public static ByteBuffer serialize(ByteBuffer bbuf, Serializable obj)
            throws BinaryJsonabilityException {
        ByteBuffer bb = bbuf;
        if (bb.remaining() < merginBufSize) {
            bb = ByteBufferUtil.enhance(bb, incBufSize);
        }
        if (obj == null) {
            bb.put(nullType);
        }
        // Embedded locators
        else if (obj instanceof TcpLocator) {
            bb.put(tcpType);
            InetLocator.putAddr(bb, ((InetLocator) obj).getSocketAddress());
        }
        else if (obj instanceof UdpLocator) {
            bb.put(udpType);
            InetLocator.putAddr(bb, ((InetLocator) obj).getSocketAddress());
        }
        else if (obj instanceof EmuLocator) {
            bb.put(emuType).putInt(((EmuLocator) obj).getVPort());
        }
        // Plug-in locators
        else if (obj instanceof PeerLocator) {
            ((PeerLocator)obj).serialize(bbuf);
        }/*
        else if (obj instanceof BluetoothLocator) {
            bb.put(btType).put(((BluetoothLocator) obj).getAddr().getBytes());
        } */
        else if (obj instanceof PeerId) {
            byte[] v = ((PeerId) obj)._getBytes();
            bb.put(peerIdType).putShort((short) v.length);
            bb = ByteBufferUtil.put(bb, v);
        }
        else if (obj instanceof TransportId) {
            byte[] v = ((TransportId) obj)._getBytes();
            bb.put(transportIdType).putShort((short) v.length);
            bb = ByteBufferUtil.put(bb, v);
        }
        else if (obj instanceof ObjectId) {
            byte[] v = ((ObjectId) obj)._getBytes();
            bb.put(objectIdType).putShort((short) v.length);
            bb = ByteBufferUtil.put(bb, v);
        }
        else if (obj instanceof Byte) {
            bb.put(byteType).put((Byte) obj);
        }
        else if (obj instanceof Integer) {
            bb.put(intType).putInt((Integer) obj);
        }
        else if (obj instanceof byte[]) {
            byte[] v = (byte[]) obj;
            bb.put(bytesType).putInt(v.length);
            bb = ByteBufferUtil.put(bb, v);
        }
        else if (obj instanceof String) {
            try {
                byte[] v = ((String) obj).getBytes("UTF-8");
                bb.put(strType).putInt(v.length);
                bb = ByteBufferUtil.put(bb, v);
            } catch (UnsupportedEncodingException ignore) {
            }
        }
        else {
            try {
                byte[] v = SerializingUtil.serialize(obj);
                bb.put(otherJavaType).putInt(v.length);
                bb = ByteBufferUtil.put(bb, v);
            } catch (ObjectStreamException e) {
                throw new BinaryJsonabilityException("could not serialize: "
                        + obj.getClass().getName(), e);
            }
        }
        return bb;
    }

    public static Serializable deserialize(ByteBuffer bbuf)
            throws BinaryJsonabilityException {
        try {
            byte typeId = bbuf.get();
            switch (typeId) {
            case nullType:
                return null;
            case tcpType:
                InetSocketAddress addr = InetLocator.getAddr(bbuf);
                return new TcpLocator(addr);
            case udpType:
                addr = InetLocator.getAddr(bbuf);
                return new UdpLocator(addr);
            case emuType:
                int vport = bbuf.getInt();
                return new EmuLocator(vport);
            case peerIdType:
                int len = bbuf.getShort();
                byte[] v = new byte[len];
                bbuf.get(v);
                return new PeerId(v);
            case transportIdType:
                len = bbuf.getShort();
                v = new byte[len];
                bbuf.get(v);
                return new TransportId(v);
            case objectIdType:
                len = bbuf.getShort();
                v = new byte[len];
                bbuf.get(v);
                return new ObjectId(v);
            case byteType:
                byte b = bbuf.get();
                return b;
            case intType:
                int i = bbuf.getInt();
                return i;
            case bytesType:
                len = bbuf.getInt();
                v = new byte[len];
                bbuf.get(v);
                return v;
            case strType:
                len = bbuf.getInt();
                v = new byte[len];
                bbuf.get(v);
                return new String(v, "UTF-8");
            case otherJavaType:
                len = bbuf.getInt();
                v = new byte[len];
                bbuf.get(v);
                return SerializingUtil.deserialize(v);
            default:
                // plug-in locator type.
                PeerLocator pl = PeerLocator.deserialize(bbuf);
                if (pl == null) {
                    throw new BinaryJsonabilityException("unknown type " + typeId);
                }
                else {
                    return pl;
                }                
            }
        } catch (Exception e) {
            throw new BinaryJsonabilityException(e);
        }
    }
}
