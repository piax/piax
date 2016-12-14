package org.piax.gtrans.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.raw.RawTransport;

public class NettyLocator extends PeerLocator {
    
    protected enum TYPE {
        TCP, SSL, WS, WSS, UDT
    };
    TYPE type;
    String host;
    int port;
    
    static public String DEFAULT_TYPE="tcp"; 
    
    public NettyLocator() {
        // XXX should define default value;
    }
    
    public NettyLocator(InetSocketAddress addr) {
        this.type = parseType(DEFAULT_TYPE); 
        this.host = addr.getHostName();
        this.port = addr.getPort();
    }

    public NettyLocator(String host, int port) {
        this.type = parseType(DEFAULT_TYPE);
        this.host = host;
        this.port = port;
    }

    public TYPE parseType(String str) {
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
        else {
            t = TYPE.TCP; // fallback.
        }
        return t;
    }

    public NettyLocator(String spec) throws ProtocolUnsupportedException {
        // "netty:tcp:localhost:12367"
        // "netty:tcp:localhost:12367"
        // "netty:ssl:localhost:12367"
        // "netty:ws:localhost:12367"
        // "netty:wss:localhost:12367"
        // "netty:udt:localhost:12367"
        String specs[] = spec.split(":");
        if (specs.length != 4) {
            throw new ProtocolUnsupportedException("netty specification is not supported:" + spec);
        }
        if (!specs[0].equals("netty")) {
            throw new ProtocolUnsupportedException("illegal use of constructor:");
        }
        this.type = parseType(specs[1]);
        this.host = specs[2];
        this.port = Integer.parseInt(specs[3]);
    }

    public String getHost() {
        return host;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NettyLocator) {
            NettyLocator l = (NettyLocator)o;
            return host.equals(l.host) && port == l.port;
        }
        return false;
    }

    public TYPE getType() {
        return type;
    }

    public int getPort() {
        return port;
    }

    // This class does not create raw transport.
    private static final long serialVersionUID = -2778097890346547201L;
    @Override
    public void serialize(ByteBuffer bb) {
    }

    @Override
    public RawTransport<NettyLocator> newRawTransport(PeerId peerId)
            throws IOException {
        return null;
    }
    
    public String getKeyString() {
        return type + ":" + toString();
    }

    @Override
    public String toString() {
        return host +":"+ port;
    }
}
