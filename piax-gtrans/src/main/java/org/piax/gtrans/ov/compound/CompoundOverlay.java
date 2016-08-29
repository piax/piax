/*
 * CompoundOverlay.java - An implementation of compound overlay
 * 
 * Copyright (c) 2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: CompoundOverlay.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.compound;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of compound overlay
 */
public class CompoundOverlay<D extends Destination, K extends Key> extends
        OverlayImpl<D, K> implements OverlayListener<D, K> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(CompoundOverlay.class);

    /*
     * TODO think
     * messageIdを付与していないので、複数のgatewayがあった場合、メッセージが重複して行き渡る
     */
    
    /**
     * 複合オーバーレイのgateway機能のためにセットする特殊key
     */
    public enum SpecialKey implements ComparableKey<SpecialKey> {
        WILDCARD
    }
    
    /**
     * 連結させるオーバーレイのtransIdPathを連結させて、このCompoundOverlayのデフォルトとして使う
     * TransportIdを生成する。
     * 
     * @param overlays
     * @return このCompoundOverlayのデフォルトとして使うTransportId
     */
    private static TransportId concat(List<TransportIdPath> overlays) {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (TransportIdPath ovId : overlays) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append("+");
            }
            sb.append(ovId.toString());
        }
        return new TransportId(sb.toString());
    }

    public static class PayloadOption implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final Destination dst;
        final List<TransportIdPath> nextOvs;
        final TransOptions opts;
        PayloadOption(Destination dst, List<TransportIdPath> nextOvs, TransOptions opts) {
            this.dst = dst;
            this.nextOvs = nextOvs;
            this.opts = opts;
        }
    }
    
    private final List<TransportIdPath> overlays;
    private volatile Boolean gatewayOn = false;
    final Peer peer;

    public CompoundOverlay(Peer peer, List<TransportIdPath> overlays) 
            throws IdConflictException, IOException {
        this(peer, concat(overlays), overlays);
    }

    public CompoundOverlay(Peer peer, TransportId transId, List<TransportIdPath> overlays)
            throws IdConflictException, IOException {
        super(peer, transId, null);
        this.overlays = overlays;
        this.peer = peer;
    }
    
    @Override
    public synchronized void fin() {
        super.fin();
        try {
            leave();
        } catch (IOException ignore) {
        }
    }
    
    /**
     * このCompoundOverlayの生成時に指定されたOverlayを探し出すためのTransportIdPathのリストを返す。
     * 
     * @return Overlayを探し出すためのTransportIdPathのリスト
     */
    public List<TransportIdPath> getSpecifiedOverlays() {
        return new ArrayList<TransportIdPath>(overlays);
    }
    
    @Override
    public Endpoint getEndpoint() {
        return this.peerId;
    }
    
    /**
     * 複合オーバーレイのgateway機能をセットする。
     * 
     * @param isOn gateway機能をonする場合はtrue
     */
    @SuppressWarnings("unchecked")
    public void setGateway(boolean isOn) {
        synchronized (gatewayOn) {
            if (gatewayOn == isOn) return;
            for (TransportIdPath ovIdPath : peer.getBaseOverlays()) {
                Overlay<D, K> ov = (Overlay<D, K>) peer.getTransport(ovIdPath);
                if (ov == null) {
                    logger.error("overlay " + transId + " don\'t exist");
                    continue;
                }
                // on->off の時は、wildcardキーをはずすための処理を行う
                if (gatewayOn && !isOn) {
                    try {
                        ov.removeKey(transId, (K) SpecialKey.WILDCARD);
                    } catch (IOException e) {
                        logger.warn("", e);
                    }
                }
                // off->on
                if (!gatewayOn && isOn) {
                    try {
                        ov.addKey(transId, (K) SpecialKey.WILDCARD);
                    } catch (IOException e) {
                        logger.warn("", e);
                    }
                }
            }
            gatewayOn = isOn;
        }
    }

    @Override
    public boolean addKey(ObjectId upper, K key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeKey(ObjectId upper, K key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver,
            D dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
            Object msg, TransOptions opts) throws ProtocolUnsupportedException,
            IOException {
        return request0(sender, receiver, overlays, dst, msg, opts);
    }

    public FutureQueue<?> request0(ObjectId sender, ObjectId receiver,
            List<TransportIdPath> nextOvs, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        logger.debug("nextOvIds {}", nextOvs);
        logger.debug("peer:{} dst:{} msg:{}", peerId, dst, msg);
        try {
            // pick up first overlay and make the new next overlay list
            List<TransportIdPath> _nextOvs = new ArrayList<TransportIdPath>();
            Overlay<D, K> firstOv = null;
            for (TransportIdPath ovIdPath : nextOvs) {
                List<Transport<?>> matched = peer.getMatchedTransport(ovIdPath);
                int matchedNum = 0;
                for (Transport<?> trans : matched) {
                    if (trans instanceof Overlay) {
                        matchedNum++;
                        @SuppressWarnings("unchecked")
                        Overlay<D, K> ov = (Overlay<D, K>) trans;
                        if (firstOv == null) {
                            firstOv = ov;
                        }
                    }
                }
                if (matchedNum == 0) {
                    _nextOvs.add(ovIdPath);
                    logger.debug("no overlay has matched with {} ", ovIdPath);
                } else if (matchedNum > 1) {
                    logger.info("{} overlays have matched with {} ", matchedNum, ovIdPath);
                }
            }
            if (firstOv == null || !firstOv.isJoined()) {
                throw new ProtocolUnsupportedException("no active overlay");
            }
            
            // make new msg
            NestedMessage nmsg;
            if (_nextOvs.size() == 0) {
                // 転送をしないケース
                nmsg = new NestedMessage(sender, receiver, null, getEndpoint(), msg);
                nmsg.setPassthrough(null);
            } else {
                nmsg = new NestedMessage(sender, receiver, null, getEndpoint(), 0,
                        new PayloadOption(dst, _nextOvs, opts), msg);
                nmsg.setPassthrough(SpecialKey.WILDCARD);
            }
            // call lower overlay
            return firstOv.request(transId, dst, nmsg, opts);
        } finally {
            logger.trace("EXIT:");
        }
    }

    public void onReceive(Overlay<D, K> ov, OverlayReceivedMessage<K> msg) {
        logger.error("unexpected onReceive");
    }

    @SuppressWarnings("unchecked")
    public FutureQueue<?> onReceiveRequest(Overlay<D, K> trans,
            OverlayReceivedMessage<K> rmsg) {
        logger.trace("ENTRY:");
        Collection<K> matchedKeys = rmsg.getMatchedKeys();
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);
        
        @SuppressWarnings("rawtypes")
        FutureQueue fq = new FutureQueue<Object>();
        // gatewayにトライする。
//        if (gatewayOn && matchedKeys.contains(SpecialKey.WILDCARD)) {
        if (gatewayOn && nmsg.passthrough == SpecialKey.WILDCARD) {
            PayloadOption option = (PayloadOption) nmsg.option;
            /*
             * TODO 本来ならtimeoutの減算処理をしないといけない
             */
            try {
                FutureQueue<?> forward = request0(nmsg.sender, nmsg.receiver,
                        option.nextOvs, (D) option.dst, nmsg.getInner(), option.opts);
                for (RemoteValue<?> rv : forward) {
//                    if (SpecialKey.WILDCARD == rv.getValue()) {
//                        logger.info("FutureQueue includes WILDCARD key");
//                        continue;
//                    }
                    fq.add(rv);
                }
            } catch (ProtocolUnsupportedException e) {
                logger.warn("", e);
            } catch (IOException e) {
                logger.warn("", e);
            }
        }
        matchedKeys.remove(SpecialKey.WILDCARD);
        OverlayListener<D, K> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
        } else {
            OverlayReceivedMessage<K> rcvMsg = new OverlayReceivedMessage<K>(
                    nmsg.sender, nmsg.src, matchedKeys, nmsg.getInner());
            FutureQueue<?> response = ovl.onReceiveRequest(this, rcvMsg);
            for (RemoteValue<?> rv : response) {
                fq.add(rv);
            }
        }
        fq.setEOFuture();
        return fq;
    }

    @Override
    public synchronized boolean join(Collection<? extends Endpoint> seeds)
            throws IOException {
        if (isJoined()) {
            return false;
        }
//        setGateway(true);
        isJoined = true;
        return true;
    }

    @Override
    public boolean leave() throws IOException {
        if (!isJoined()) {
            return false;
        }
        setGateway(false);
        isJoined = false;
        return true;
    }
    
    //-- unnecessary but need to be defined methods by Java8
    public void onReceive(Transport<D> trans, ReceivedMessage rmsg) {
    }
    public void onReceive(RequestTransport<D> trans, ReceivedMessage rmsg) {
    }
    public FutureQueue<?> onReceiveRequest(RequestTransport<D> trans,
            ReceivedMessage rmsg) {
        return null;
    }
/*
	@Override
	public FutureQueue<?> request(ObjectId sender, ObjectId receiver, D dst,
			Object msg, TransOptions opts) throws ProtocolUnsupportedException,
			IOException {
		return request(sender, receiver, dst, msg, (int)TransOptions.timeout(opts));
	}
	*/
}
