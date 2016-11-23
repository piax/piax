package org.piax.gtrans.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.raw.RawTransport;

public class NettyLocator extends PeerLocator {
    String host;
    int port;
    
    public NettyLocator(InetSocketAddress addr) {
        this.host = addr.getHostName();
        this.port = addr.getPort();
    }
    
    public NettyLocator(String host, int port) {
        this.host = host;
        this.port = port;
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
    
    public int getPort() {
        return port;
    }
    
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
