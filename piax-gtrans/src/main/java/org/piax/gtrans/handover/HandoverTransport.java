/*
 * HandoverTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: HandoverTransport.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.handover;

import java.io.IOException;
import java.util.List;

import org.piax.common.PeerId;
import org.piax.common.PeerIdWithLocator;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.NoSuchPeerException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.BaseTransportMgr;
import org.piax.gtrans.impl.DatagramBasedTransport;
import org.piax.gtrans.impl.IdResolver;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.raw.LocatorStatusObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class HandoverTransport extends
        DatagramBasedTransport<PeerId, PeerLocator> implements
        LocatorStatusObserver {
    /*
     * HandoverTransportは下位のtransportが定まらないため、DatagramBasedTransportを
     * 実装テンプレートとして用いている。
     * channelの実装を考えると、OneToOneMappingTransportを用いるのがよいが、下位のtransport
     * の変更とともに、下位のchannelまで変更されると、channelの維持が困難になるため。
     */
    
    /*--- logger ---*/
    private static final Logger logger = 
            LoggerFactory.getLogger(HandoverTransport.class);

    final Peer peer;
    final IdResolver idResolver;
    final BaseTransportMgr baseTransMgr;
    
    public HandoverTransport(Peer peer, TransportId transId)
            throws IdConflictException {
        super(peer, transId, null, true);
        this.peer = peer;
        idResolver = peer.getIdResolver();
        baseTransMgr = peer.getBaseTransportMgr();
    }

    @Override
    public synchronized void fin() {
        super.fin();
        broadcastPeerFin();
    }

    @Override
    public Transport<?> getBaseTransport() {
        return baseTransMgr.getRecentlyUsedTransport();
    }

    public PeerId getEndpoint() {
        return peerId;
    }

    private PeerLocator selectLocator(PeerId target) {
        if (!idResolver.contains(target)) return null;
        List<PeerLocator> locs = idResolver.getLocators(target);
        for (PeerLocator loc : locs) {
            Transport<?> bt = baseTransMgr.getApplicableBaseTransport(loc);
            if (bt != null) return loc;
        }
        return null;
    }
    
    @Override
    protected void lowerSend(PeerId dst, NestedMessage nmsg)
            throws ProtocolUnsupportedException, IOException {
        Transport<PeerLocator> bt;
        PeerId peerId = null;
        PeerLocator loc = null;
//        if (dst instanceof PeerLocator) {
//            // このケースは例外にしてもよいかもしれない
//            logger.info("destination is not PeerId");
//            loc = (PeerLocator) dst;
//            bt = baseTransMgr.getApplicableBaseTransport(loc);
//        }
//        else 
        if (dst instanceof PeerIdWithLocator) {
            peerId = ((PeerIdWithLocator) dst).getPeerId();
            loc = ((PeerIdWithLocator) dst).getLocator();
            bt = baseTransMgr.getApplicableBaseTransport(loc);
        }
        else if (dst instanceof PeerId) {
            peerId = (PeerId) dst;
            loc = selectLocator(peerId);
            if (loc == null) {
                throw new NoSuchPeerException(
                        "No such PeerLocator could send to " + dst);
            }
            bt = baseTransMgr.getApplicableBaseTransport(loc);
        } else {
            bt = null;
        }
        if (bt == null) {
            throw new ProtocolUnsupportedException(
                    "No such Transport could send to " + dst);
        }
        bt.send(transId, loc, nmsg);
        
        /*
         * idResolverにエントリがなく、peerが初めて通信する相手である場合に、
         * 自peerが持っているすべてのlocatorをそのpeerに通知する
         */
        if (peerId != null && !idResolver.contains(peerId)) {
            idResolver.set(peerId, loc);
            for (PeerLocator myLoc : baseTransMgr.getAvailableLocators()) {
                sendIdLocChange(bt, loc, myLoc, null);
            }
        }
    }

    @Override
    protected NestedMessage _preReceive(ReceivedMessage rmsg) {
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        // 来たアドレス情報をidResolverの表に追記する
        idResolver.add(nmsg.srcPeerId, (PeerLocator) rmsg.getSource());
        // idResolverの表の更新を行う（制御メッセージの処理）
        if (nmsg.getInner() == null && nmsg.option instanceof PeerLocator[]) {
            if (peerId.equals(nmsg.srcPeerId)) {
                logger.warn("locator change command from me has no effect");
            } else {
                PeerLocator[] changes = (PeerLocator[]) nmsg.option;
                PeerLocator addLoc = changes[0];
                PeerLocator removeLoc = changes[1];
                if (addLoc == null && removeLoc == null) {
                    idResolver.removeAll(nmsg.srcPeerId);
                } else {
                    if (addLoc != null) {
                        idResolver.add(nmsg.srcPeerId, addLoc);
                    }
                    if (removeLoc != null) {
                        idResolver.remove(nmsg.srcPeerId, removeLoc);
                    }
                }
            }
            // 続きのonReceiveの処理を行わせないため、nullを返す
            return null;
        }
        return nmsg;
    }

    private <E extends PeerLocator> void sendIdLocChange(Transport<E> bt, E target, 
            PeerLocator addLoc, PeerLocator removeLoc)
                    throws ProtocolUnsupportedException, IOException {
        // TODO changeコマンドは簡易実装になっている
        PeerLocator[] changes = new PeerLocator[] {addLoc, removeLoc};
        NestedMessage nmsg = new NestedMessage(null, null, peerId, 
                bt.getEndpoint(), 0, changes, null);
        bt.send(transId, target, nmsg);
    }
    
    private <E extends PeerLocator> void sendIdLocChange(E target, PeerLocator addLoc,
            PeerLocator removeLoc) throws ProtocolUnsupportedException,
            IOException {
        Transport<E> bt = baseTransMgr.getApplicableBaseTransport(target);
        if (bt == null) {
            throw new ProtocolUnsupportedException(
                    "No such Transport could send to " + target);
        }
        sendIdLocChange(bt, target, addLoc, removeLoc);
    }
    
    private void broadcastIdLocChange(PeerLocator addLoc, PeerLocator removeLoc) {
        List<PeerId> peers = idResolver.getPeerIds();
        for (PeerId peer : peers) {
            PeerLocator loc = selectLocator(peer);
            try {
                if (loc == null) {
                    throw new NoSuchPeerException(
                            "No such PeerLocator could send to " + peer);
                }
                sendIdLocChange(loc, addLoc, removeLoc);
            } catch (IOException e) {
                logger.warn("", e);
            }
        }
    }

    private void broadcastLocatorChange(PeerLocator addLoc, PeerLocator removeLoc) {
        broadcastIdLocChange(addLoc, removeLoc);
    }
    
    private void broadcastPeerFin() {
        // TODO think!
        broadcastIdLocChange(null, null);
    }

    @Override
    public synchronized void onEnabled(PeerLocator loc, boolean isNew) {
        try {
            Transport<PeerLocator> bt = baseTransMgr.newBaseTransport(null,
                    null, loc);
            bt.setListener(transId, this);
            if (bt instanceof ChannelTransport<?>) {
                ((ChannelTransport<PeerLocator>) bt).setChannelListener(
                        transId, this);
            }
            broadcastLocatorChange(loc, null);
        } catch (IOException e) {
            logger.warn("", e);
        } catch (IdConflictException e) {
            logger.error("", e);
        }
    }

    @Override
    public synchronized void onFadeout(PeerLocator loc, boolean isFin) {
        Transport<PeerLocator> bt = baseTransMgr.removeBaseTransport(loc);
        if (bt != null) {
            // btはfinしているので、listenerを削除する必要はない
//            bt.setListener(transId, null);
//            bt.setChannelListener(transId, null);
            broadcastLocatorChange(null, loc);
        }
    }

    @Override
    public synchronized void onChanging(PeerLocator oldLoc, PeerLocator newLoc) {
        Transport<?> oldBt = baseTransMgr.removeBaseTransport(oldLoc);
        if (oldBt == null) {
            onEnabled(newLoc, false);
            return;
        }
        try {
            Transport<PeerLocator> newBt = baseTransMgr.newBaseTransport(
                    null, null, newLoc);
            newBt.setListener(transId, this);
            if (newBt instanceof ChannelTransport<?>) {
                ((ChannelTransport<PeerLocator>) newBt).setChannelListener(
                        transId, this);
            }
            broadcastLocatorChange(newLoc, oldLoc);
            return;
        } catch (IOException e) {
            logger.warn("", e);
        } catch (IdConflictException e) {
            logger.error("", e);
        }
        broadcastLocatorChange(null, oldLoc);
    }

    @Override
    public void onHangup(PeerLocator loc, Exception cause) {
        // TODO causeの処理をしていない
        onFadeout(loc, false);
    }

    @Override
    protected boolean useReceiverThread(int numProc) {
        return false;
    }
}
