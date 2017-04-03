package org.piax.gtrans.netty.idtrans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyChannelTransport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.netty.NettyRawChannel;
import org.piax.gtrans.netty.PrimaryKey;
import org.piax.gtrans.netty.PrimaryKey.LocatorEntry;

public class IdChannelTransport extends NettyChannelTransport<PrimaryKey> {

    PrimaryKey primaryKey;
    LocatorManager mgr;
    
    protected static final int FORWARD_HOPS_LIMIT = 3;
    public int forwardCount = 0;

    // By default, peerId becomes the primary key. 
    public IdChannelTransport(Peer peer, TransportId transId, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, peerId, peerLocator);
        primaryKey = new PrimaryKey(peerId, peerLocator);
        mgr = new LocatorManager();
    }

    public IdChannelTransport(Peer peer, TransportId transId, PrimaryKey key, PeerId peerId,
            NettyLocator peerLocator) throws IdConflictException, IOException {
        super(peer, transId, peerId, peerLocator);
        this.primaryKey = key;
        mgr = new LocatorManager();
    }
    
    public boolean filterMessage (NettyMessage<PrimaryKey> nmsg) {
        PrimaryKey src = nmsg.getSource();
        src = mgr.registerAndGet(src);
        nmsg.setSource(src);
        return forwardMessage(nmsg);
    }

    boolean forwardMessage(NettyMessage<PrimaryKey> nmsg) {
        PrimaryKey dst = (PrimaryKey)nmsg.getDestination();
        if (primaryKey.equals(dst)) {
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

    protected NettyRawChannel<PrimaryKey> getRaw(PrimaryKey ep) {
        return (ep.getRawKey() == null)? null : raws.get(ep); 
    }

    protected void channelSendHook(PrimaryKey src, PrimaryKey dst) {
        ArrayList<NettyLocator> locators = new ArrayList<>();
        for (NettyRawChannel<PrimaryKey> raw : raws.values()) {
            locators.add((raw.getRemote().getLocator()));
        }
        src.setNeighborLocators(locators);

        // update the manager with newest neighbors.
        PrimaryKey k = mgr.registerAndGet(dst);
        // mark self as forwarded
        for (LocatorEntry e : k.getNeighbors()) {
            if (e.locator.equals(ep.getLocator())) {
                e.flag = true;
            }
        }
        dst.setNeighbors(k.getNeighbors());
    }

    @Override
    protected NettyLocator directLocator(PrimaryKey key) {
        return key.getLocator();
    }

    // get RawChannel with remote locator included in the list 
    private NettyRawChannel<PrimaryKey> getExistingRawChannel(List<LocatorEntry> list) {
        NettyRawChannel<PrimaryKey> ret = null;
        for(ListIterator<LocatorEntry> it=list.listIterator(list.size()); it.hasPrevious();){
            LocatorEntry le = it.previous();
            for (NettyRawChannel<PrimaryKey> raw : raws.values()) {
                if (raw.getRemote().equals(le.locator)) {
                    ret = raw;
                    break;
                }
            }
        }
        return ret;
    }

    private NettyLocator getUnmarkedAndMarkLocator(List<LocatorEntry> list) {
        NettyLocator ret = null;
        for(ListIterator<LocatorEntry> it=list.listIterator(list.size()); it.hasPrevious();){
            LocatorEntry le = it.previous();
            if (!le.flag) {
                le.flag = true;
                ret = le.locator;
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
            NettyLocator l = getUnmarkedAndMarkLocator(key.getNeighbors());
            raw = getRawCreateAsClient0(new PrimaryKey(l));
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
        ep.addNeighbor(raw.getRemote().getLocator());
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
        ep = new PrimaryKey(locator); // at bootstrap, wildcard key.
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
