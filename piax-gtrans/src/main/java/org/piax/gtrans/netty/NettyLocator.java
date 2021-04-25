/*
 * NettyLocator.java - A Locator of Netty
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.piax.common.PeerId;
import org.piax.gtrans.PeerLocator;
import org.piax.gtrans.Transport;

public class NettyLocator extends PeerLocator implements NettyEndpoint {
    
    public enum TYPE {
        TCP, SSL, WS, WSS, UDT, UDP, UNKNOWN
    };
    TYPE type;
    //InetAddress addr;
    String host;
    int port;
    
    static public TYPE DEFAULT_TYPE=TYPE.TCP;

    public NettyLocator() {
        // XXX should define default value;
    }
    
    static public NettyLocator parse(String spec) {
        return (NettyLocator)NettyEndpoint.parseLocator(spec);
    }
    
    public NettyLocator(String type, InetAddress addr, int port) {
        this(parseType(type), addr, port);
    }

    public NettyLocator(InetAddress addr, int port) {
        this(TYPE.UNKNOWN, addr, port);
    }

    public NettyLocator(InetSocketAddress addr) {
        this(DEFAULT_TYPE, addr.getHostName(), addr.getPort());
    }

    public NettyLocator(TYPE type, InetAddress addr, int port) {
        this.type = type;
        this.host = addr.getHostAddress();
        this.port = port;
    }
    
    public NettyLocator(TYPE type, int port) {
        this.type = type;
        this.host = null;
        this.port = port;
    }
    
    public NettyLocator(TYPE type, InetSocketAddress saddr) {
        this(type, saddr.getHostName(), saddr.getPort());
    }

    public NettyLocator(String host, int port) {
        this(DEFAULT_TYPE, host, port);
    }
    
    public NettyLocator(TYPE type, String host, int port) {
        this.type = type;
        this.host = host;
        this.port = port;
    }

    static public TYPE parseType(String str) {
        TYPE t;
        if (str.equals("tcp")) {
            t=TYPE.TCP;
        }
        else if (str.equals("ssl")) {
            t=TYPE.SSL;
        }
        else if (str.equals("ws")) {
            t =TYPE.WS;
        }
        else if (str.equals("wss")) {
            t =TYPE.WSS;
        }
        else if (str.equals("udt")) {
            t = TYPE.UDT;
        }
        else if (str.equals("udp")) {
            t = TYPE.UDP;
        }
        else {
            t = TYPE.TCP; // fallback.
        }
        return t;
    }

    public String getHost() {
        return host;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NettyLocator) {
            NettyLocator l = (NettyLocator)o;
            // null means wildcard.
            return (host == null || l.host == null || (host != null && host.equals(l.host))) && port == l.port;
        }
        return false;
    }

    public TYPE getType() {
        return type;
    }

    public int getPort() {
        return port;
    }
    
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    // This class does not create raw transport.
    private static final long serialVersionUID = -2778097890346547201L;
    @Override
    public void serialize(ByteBuffer bb) {
    }

    @Override
    public Transport<NettyLocator> newRawTransport(PeerId peerId)
            throws IOException {
        return null;
    }

    public String getKeyString() {
        return toString();
    }

    @Override
    public int hashCode() {
        return (type + ":" + toString()).hashCode();
    }

    @Override
    public String toString() {
        return type + ":" + (host == null ? "NONE" : host) +":"+ port;
    }

}
