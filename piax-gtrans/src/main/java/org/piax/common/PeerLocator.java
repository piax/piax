/*
 * PeerLocator.java - An abstract class of peer locator.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: PeerLocator.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.raw.RawTransport;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;

/**
 * ピアのlocatorを示す抽象クラスを定義する。
 */
public abstract class PeerLocator implements Endpoint {
    private static final long serialVersionUID = 1L;

    protected static HashMap<Byte, Class<? extends PeerLocator>> magicMap = new HashMap<Byte, Class<? extends PeerLocator>>();
    
    /*
     * Returns null if the type is unknown.
     */
    public static PeerLocator deserialize(ByteBuffer bb) {
        byte magic = bb.get();
        Class<? extends PeerLocator> clazz = magicMap.get(magic);
        if (clazz == null) {
            return null;
        }
        Class<?>[] types = { ByteBuffer.class };
        Constructor<? extends PeerLocator> constructor;
        try {
            constructor = clazz.getConstructor(types);
        } catch (SecurityException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        Object[] args = { bb };
        PeerLocator locator;
        try {
            locator = constructor.newInstance(args);
        } catch (IllegalArgumentException |
                 ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return locator;
    }

    public abstract void serialize(ByteBuffer bb);
    /**
     * このピアlocatorを使った通信をサポートするRawTransportを生成する。
     * rawListenerにはRawTransportが受信したバイト列を受け取る上位層のオブジェクトを指定する。
     * 
     * @param peerId the peerId of the peer object.
     * @return このピアlocatorを使った通信をサポートするRawTransport
     * @throws IOException an exception that occurs when there is I/O error. 
     */
    public abstract RawTransport<? extends PeerLocator> newRawTransport(PeerId peerId)
            throws IOException;
    
    static public PeerLocator newLocator(String spec) {
        PeerLocator locator = null;
        String specs[] = spec.split(":");
        if (specs[0].equals("tcp")) {
            locator = new TcpLocator(spec);
        }
        else if (specs[0].equals("udp")) {
            locator = new UdpLocator(spec);
        }
        else if (specs[0].equals("netty")) {
            try {
                locator = new NettyLocator(spec);
            } catch (ProtocolUnsupportedException e) {
            }
        }
        return locator;
    }
    
    /**
     * targetに指定されたPeerLocatorオブジェクトと同一のクラスであるときに
     * trueを返す。
     * 
     * @param target 比較対象となるPeerLocatorオブジェクト
     * @return targetが同じクラスであるときtrue
     */
    public boolean sameClass(PeerLocator target) {
        return this.getClass().equals(target.getClass());
    }
}
