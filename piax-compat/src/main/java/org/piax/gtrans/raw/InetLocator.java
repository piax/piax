/*
 * InetLocator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: InetLocator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.piax.common.PeerLocator;

/**
 * IPv4アドレスを示すPeerLocatorの抽象クラス。
 * 
 * 
 */
public abstract class InetLocator extends PeerLocator {
    private static final long serialVersionUID = 1L;

    private static final byte[] nullAddr = new byte[] {0,0,0,0,0,0};

    /**
     * NATをだますために、IPアドレスについてはビット反転させる。
     * @param ip
     */
    private static void xor(byte[] ip) {
        for (int i = 0; i < ip.length; i++) {
            ip[i] = (byte)~ip[i];
        }
    }
    
    public static void putAddr(ByteBuffer bbuf, InetSocketAddress addr) {
        if (addr == null) {
            bbuf.put(nullAddr);
        } else {
            byte[] ip = addr.getAddress().getAddress();
            xor(ip);
            bbuf.put(ip);
            bbuf.putShort((short) addr.getPort());
        }
    }

    public static InetSocketAddress getAddr(ByteBuffer bbuf)
            throws UnknownHostException {
        byte[] ip = new byte[4];
        int port;
        InetSocketAddress addr = null;
        bbuf.get(ip);
        xor(ip);
        port = (bbuf.getShort() & 0xffff);
        if (port != 0) {
            addr = new InetSocketAddress(InetAddress.getByAddress(ip), port);
        }
        return addr;
    }

    /*
     * patch for GCJ-4.1.0 bug
     */
    protected transient InetSocketAddress addr;
    
    protected InetLocator(InetSocketAddress addr) {
        if (addr == null)
            throw new IllegalArgumentException("argument should not be null");
        this.addr = addr;
    }

    public InetLocator() {
        // TODO Auto-generated constructor stub
    }

    public String getHostName() {
        return addr.getHostName();
    }
    
    public byte[] getHostAddress() {
        return addr.getAddress().getAddress();
    }

    public InetAddress getInetAddress() {
        return addr.getAddress();
    }

    public int getPort() {
        return addr.getPort();
    }

    public InetSocketAddress getSocketAddress() {
        return addr;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof InetLocator)) {
            return false;
        }
        InetLocator _obj = (InetLocator) obj;
        if (getPort() != _obj.getPort()) return false;
        if (addr.getAddress().equals(_obj.addr.getAddress())) 
            return true;
//        if (LocalInetAddrs.isLocal(addr.getAddress())
//                && LocalInetAddrs.isLocal(_obj.addr.getAddress()))
//            return true;
        return false;
    }
    
    // TODO this not equivalent to equals method
    @Override
    public int hashCode() {
        return addr.getPort();
    }
    
    @Override
    public String toString() {
        return addr.getAddress().getHostAddress() + ":" + getPort();
    }
    
    /*
     * patch for GCJ-4.1.0 bug
     */
    private void writeObject(java.io.ObjectOutputStream s)
    throws java.io.IOException {
        // Write out element count, and any hidden stuff
        s.defaultWriteObject();

        s.writeObject(addr.getAddress().getHostAddress());
        s.writeInt(addr.getPort());
    }

    private void readObject(java.io.ObjectInputStream s)
    throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        
        String hostname = (String) s.readObject();
        int port = s.readInt();
        addr = new InetSocketAddress(hostname, port);
    }
}
