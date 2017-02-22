package org.piax.gtrans.netty.loctrans;

import io.netty.bootstrap.ServerBootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.NettyRawChannel;
import org.piax.gtrans.netty.bootstrap.SslBootstrap;
import org.piax.gtrans.netty.bootstrap.TcpBootstrap;
import org.piax.gtrans.netty.bootstrap.UdtBootstrap;

public class LocatorChannelTransport extends NettyChannelTransport<NettyLocator> {
    
    public LocatorChannelTransport(Peer peer, TransportId transId,
            PeerId peerId, NettyLocator peerLocator)
            throws IdConflictException, IOException {
        super(peer, transId, peerId, peerLocator);
    }
    
    protected NettyRawChannel<NettyLocator> getRawCreateAsClient(NettyLocator dst) throws IOException {
        NettyRawChannel<NettyLocator> raw = getRawCreateAsClient0(dst);
        return raw;
    }

    @Override
    protected void bootstrap(NettyLocator peerLocator) throws ProtocolUnsupportedException {
        this.locator = peerLocator;
        switch(peerLocator.getType()){
        case TCP:
            bs = new TcpBootstrap();
            break;
        case SSL:
            bs = new SslBootstrap(locator.getHost(), locator.getPort());
            break;
        case UDT:
            bs = new UdtBootstrap();
            break;
        default:
            throw new ProtocolUnsupportedException("not implemented yet.");
        }
        bossGroup = bs.getParentEventLoopGroup();
        serverGroup = bs.getChildEventLoopGroup();
        clientGroup = bs.getClientEventLoopGroup();
        
        ServerBootstrap b = bs.getServerBootstrap(this);
        b.bind(new InetSocketAddress(peerLocator.getHost(), peerLocator.getPort())).syncUninterruptibly();
            
        logger.debug("bound " + peerLocator);
    }

    @Override
    protected NettyLocator createEndpoint(String host, int port) {
        return new NettyLocator(host, port);
    }

    @Override
    protected boolean filterMessage(NettyMessage<NettyLocator> msg) {
        // TODO Auto-generated method stub
        return false;
    }

}
