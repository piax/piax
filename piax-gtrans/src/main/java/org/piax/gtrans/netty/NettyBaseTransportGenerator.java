package org.piax.gtrans.netty;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.ConfigurationError;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.BaseTransportGenerator;
import org.piax.gtrans.impl.TransportImpl;
import org.piax.gtrans.netty.idtrans.IdChannelTransport;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.netty.loctrans.LocatorChannelTransport;

public class NettyBaseTransportGenerator extends BaseTransportGenerator {

    public NettyBaseTransportGenerator(Peer peer) {
        super(peer);
    }
    
    @Override
    public <E extends Endpoint> ChannelTransport<E> _newBaseChannelTransport(String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        
        if (loc instanceof NettyLocator) {
            transId = new TransportId("netty");
        } else if (loc instanceof PrimaryKey) { 
            transId = new TransportId("id");
        }
        else {
            throw new ConfigurationError(loc.getClass() +" is not supported in NettyBaseTransportGenerator.");
        }
        ChannelTransport<E> trans = null;
        if (loc instanceof NettyLocator){
            trans = (ChannelTransport<E>)new LocatorChannelTransport(peer, transId, peer.getPeerId(), (NettyLocator)loc);
        } else if (loc instanceof PrimaryKey){ 
            trans = (ChannelTransport<E>)new IdChannelTransport(peer, transId, peer.getPeerId(), (PrimaryKey)loc);
        } else {
            throw new ConfigurationError("Locator " + loc.getClass() +" is not supported in NettyBaseTransportGenerator.");
        }
        ((TransportImpl<?>) trans).setBaseTransport();
        return trans;
    }

    @Override
    public <E extends Endpoint> Transport<E> _newBaseTransport(String desc, TransportId transId, E loc)
            throws IdConflictException, IOException {
        return newBaseChannelTransport(desc, transId, loc);
    }

}
