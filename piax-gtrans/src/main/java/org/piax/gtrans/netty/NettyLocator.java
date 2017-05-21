package org.piax.gtrans.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.raw.RawTransport;

public class NettyLocator extends PeerLocator implements NettyEndpoint {
    
    public enum TYPE {
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
        this(DEFAULT_TYPE, addr.getHostName(), addr.getPort()); 
    }

    public NettyLocator(String host, int port) {
        this(DEFAULT_TYPE, host, port);
    }
    
    public NettyLocator(String type, String host, int port) {
        this.type = parseType(type);
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
    public int hashCode() {
        return (type + ":" + toString()).hashCode();
    }

    @Override
    public String toString() {
        return host +":"+ port;
    }

}
