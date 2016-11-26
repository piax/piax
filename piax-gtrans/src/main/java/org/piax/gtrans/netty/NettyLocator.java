package org.piax.gtrans.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.raw.RawTransport;

public class NettyLocator extends PeerLocator {
    
    enum TYPE {
        TCP, SSL, WS, WSS, UDT
    };
    TYPE type;
    String host;
    int port;
    
    TYPE DEFAULT_TYPE=TYPE.SSL; 
    
    public NettyLocator(InetSocketAddress addr) {
        this.type = DEFAULT_TYPE; 
        this.host = addr.getHostName();
        this.port = addr.getPort();
    }
    
    public NettyLocator(String host, int port) {
        this.type = DEFAULT_TYPE;
        this.host = host;
        this.port = port;
    }
    
    public NettyLocator(String spec) throws ProtocolUnsupportedException {
        // "tcp:localhost:12367"
        // "ssl:localhost:12367"
        // "ws:localhost:12367"
        // "wss:localhost:12367"
        // "udt:localhost:12367"
        String specs[] = spec.split(":");
        if (specs.length != 3) {
            throw new ProtocolUnsupportedException("netty specification is not supported:" + spec);
        }
        if (specs[0].equals("tcp")) {
            type=TYPE.TCP;
        }
        else if (specs[0].equals("ssl")) {
            type=TYPE.SSL;
        }
        else if (specs[0].equals("ws")) {
            type =TYPE.WS;
        }
        else if (specs[0].equals("wss")) {
            type =TYPE.WSS;
        }
        else if (specs[0].equals("udt")) {
            type = TYPE.UDT;
        }
        this.host = specs[1];
        this.port = Integer.parseInt(specs[2]);
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

    @Override
    public String toString() {
        return host +":"+ port;
    }
}
