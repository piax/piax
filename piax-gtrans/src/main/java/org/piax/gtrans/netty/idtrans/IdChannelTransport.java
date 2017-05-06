package org.piax.gtrans.netty.idtrans;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.ControlMessage;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.NettyRawChannel;
import org.piax.gtrans.netty.idtrans.PrimaryKey.NeighborEntry;

public class IdChannelTransport extends NettyChannelTransport<PrimaryKey> {
    LocatorManager mgr;
    
    protected static final int FORWARD_HOPS_LIMIT = 5;
    public int forwardCount = 0; // just for monitoring

    // By default, peerId becomes the primary key. 
    public IdChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, peerId, peerLocator);
        this.ep= new PrimaryKey(peerId, peerLocator);
        mgr = new LocatorManager();
    }

    public IdChannelTransport(Peer peer, TransportId transId, PrimaryKey key, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, peerId, peerLocator);
        this.ep = key;
        mgr = new LocatorManager();
    }

    // By default, peerId becomes the primary key.
    // The transports that have no direct locator (have only indirect neighbors) use this constructor. 
    public IdChannelTransport(Peer peer, TransportId transId, PeerId peerId) throws IdConflictException, IOException {
        super(peer, transId, peerId, null);
        this.ep = new PrimaryKey(peerId);
        mgr = new LocatorManager();
    }

    public IdChannelTransport(Peer peer, TransportId transId, PrimaryKey key, PeerId peerId)
            throws IdConflictException, IOException {
        super(peer, transId, peerId, null);
        this.ep = key;
        mgr = new LocatorManager();
    }

    public boolean filterMessage (NettyMessage<PrimaryKey> nmsg) {
        PrimaryKey src = nmsg.getSource();
        src = mgr.registerAndGet(src);
        nmsg.setSource(src);
        return forwardMessage(nmsg);
    }

    // forwarding the channel attempt signal:
    // first, use channels for neighbors if already exists.
    // second, initiate a channel for neighbors if not exists.
    
    @Deprecated
    boolean forwardMessage(NettyMessage<PrimaryKey> nmsg) {
        PrimaryKey dst = (PrimaryKey)nmsg.getDestination();
        if (ep.equals(dst)) {
            return false; // go to receive process.
        }
        else {
            if (nmsg.getHops() == FORWARD_HOPS_LIMIT) {
                logger.warn("Exceeded forward hops to {} on {}.", dst, ep);
                return true;
            }
            nmsg.incrementHops();
            logger.debug("NAT forwarding from {} to {} on {}", nmsg.getSource(), dst, ep);
            try {
                // the hop count affects on resolving a locator.
                NettyRawChannel<PrimaryKey> raw = getRawCreateAsClient(dst, nmsg);
                raw.touch();
                raw.send(nmsg);
            } catch (IOException e) {
                logger.warn("Exception occured: " + e.getMessage());
            }
            return true;
        }
    }

    // handle the control message for IdChannelTrans.
    protected void handleControlMessage(ControlMessage<PrimaryKey> cmsg) {
        switch(cmsg.getType()) {
        case UPDATE: // update locator information.
            {
                PrimaryKey k = (PrimaryKey)cmsg.getArg();
            //
            } 
            break;
        case INIT: // start a new channel initiation
            {
                PrimaryKey k = (PrimaryKey)cmsg.getArg();
            }
            break;
        case WAIT:
            // wait for the connection from specified locator...do nothing.
            break;
        default:
            break;
        }
    }

    protected NettyRawChannel<PrimaryKey> getRaw(PrimaryKey ep) {
        return (ep.getRawKey() == null)? null : raws.get(ep); 
    }

    protected void channelSendHook(PrimaryKey src, PrimaryKey dst) {
        // update the manager with newest neighbors.
        PrimaryKey k = mgr.registerAndGet(dst);
        // mark self as forwarded
        for (NeighborEntry e : k.getNeighbors()) {
            if (e.key.equals(ep)) {
                e.visited = true;
            }
        }
        dst.setNeighbors(k.getNeighbors());
    }

    @Override
    protected NettyLocator directLocator(PrimaryKey key) {
        return key.getLocator();
    }

    // get RawChannel with remote locator included in the list 
    private NettyRawChannel<PrimaryKey> getExistingRawChannel(List<NeighborEntry> list) {
        NettyRawChannel<PrimaryKey> ret = null;
        for(ListIterator<NeighborEntry> it=list.listIterator(list.size()); it.hasPrevious();){
            NeighborEntry le = it.previous();
            for (NettyRawChannel<PrimaryKey> raw : raws.values()) {
                if (raw.getRemote().equals(le.key)) {
                    ret = raw;
                    break;
                }
            }
        }
        return ret;
    }

    private PrimaryKey getUnmarkedAndMarkLocator(List<NeighborEntry> list) {
        PrimaryKey ret = null;
        for(ListIterator<NeighborEntry> it=list.listIterator(list.size()); it.hasPrevious();){
            NeighborEntry le = it.previous();
            if (!le.visited) {
                le.visited = true;
                ret = le.key;
            }
        }
        return ret;
    }

    @Override
    protected NettyRawChannel<PrimaryKey> getResolvedRawChannel(PrimaryKey key) throws IOException {
        // find the node itself or the adjacent neighbor node, then get raw channel for it.
        return null; // XXX not implemented yet.
    }

    private NettyRawChannel<PrimaryKey> getIndirectRawChannel(PrimaryKey key) throws IOException {
     // if there is only indirect candidate
        NettyRawChannel<PrimaryKey> raw = getExistingRawChannel(key.getNeighbors());
        if (raw == null) { // no exiting channel. create new
            PrimaryKey next = getUnmarkedAndMarkLocator(key.getNeighbors());
            raw = getRawCreateAsClient0(next);
            logger.debug("no existing indirect rawchannel. creating {} for {} on {}",
                    raw.getRemote(), key, ep);
        }
        return raw;
    }

    /* 
     * Get or Create a RawChannel that corresponds to the {@code dst}. 
     * @see org.piax.gtrans.netty.NettyChannelTransport#getRawCreateAsClient(org.piax.gtrans.netty.NettyEndpoint, org.piax.gtrans.netty.NettyMessage)
     */
    @Override
    protected NettyRawChannel<PrimaryKey> getRawCreateAsClient(PrimaryKey dst, NettyMessage<PrimaryKey> nmsg) throws IOException {
        NettyRawChannel<PrimaryKey> raw = null;
        dst = mgr.registerAndGet(dst); // refresh the locator entries;
        // cached raw channel.
        raw = getRaw(dst);
        if (raw != null) {
            logger.debug("existing rawchannel for {} is {} on {}", dst,
                    raw.getRemote(), ep);
            return raw;
        } else {
            if (directLocator(dst) == null) {
                raw = getIndirectRawChannel(dst);
            }
            else {
                raw = getRawCreateAsClient0(dst);
            }
        }
        ep.addNeighbor(raw.getRemote());
        return raw;
    }

    @Override
    protected PrimaryKey createEndpoint(String host, int port) {
        // TODO Auto-generated method stub
        return null;
    }

    public void fin() {
        super.fin();
        mgr.fin();
    }

    @Override
    protected void bootstrap(NettyLocator locator)
            throws ProtocolUnsupportedException {
        ep = new PrimaryKey(locator); // at bootstrap, temporary wildcard key.
        mgr = new LocatorManager();
    }

    // Id transport API.
    public void setPrimaryKey(PrimaryKey key) {
        key.setLocator(ep.getLocator());
        ep = key;
    }
    
    public void setLocator(NettyLocator locator) {
        ep.setLocator(locator);
    }
}
