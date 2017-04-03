package org.piax.gtrans.netty.loctrans;

import java.io.IOException;

import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.NettyRawChannel;

public class LocatorChannelTransport extends NettyChannelTransport<NettyLocator> {
    
    public LocatorChannelTransport(Peer peer, TransportId transId,
            PeerId peerId, NettyLocator peerLocator)
            throws IdConflictException, IOException {
        super(peer, transId, peerId, peerLocator);
    }
    
    protected NettyRawChannel<NettyLocator> getRawCreateAsClient(NettyLocator dst, NettyMessage<NettyLocator> nmsg) throws IOException {
        NettyRawChannel<NettyLocator> raw = getRawCreateAsClient0(dst);
        return raw;
    }

    @Override
    protected void bootstrap(NettyLocator peerLocator) throws ProtocolUnsupportedException {
        this.ep = peerLocator;
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

    @Override
    protected NettyLocator directLocator(NettyLocator l) {
        return l;
    }

    @Override
    protected NettyRawChannel<NettyLocator> getResolvedRawChannel(
            NettyLocator ep) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
