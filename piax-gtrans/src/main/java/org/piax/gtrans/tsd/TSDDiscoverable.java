/*
 * TSDDiscoverable.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TSDDiscoverable.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Discoverable;
import org.piax.gtrans.DiscoveryListener;
import org.piax.gtrans.PeerInfo;

/**
 * 
 */
public class TSDDiscoverable<E extends Endpoint> implements Discoverable<E>,
        TSDListener {

    public static long DEFAULT_EXPIRATION_TIME = 30 * 1000;
    
    /*
     * ServiceInfoを入れる箱。生成時刻の管理のため。
     */
    static class InfoBox<T> {
        final T serv;
        final long lastObserved;

        InfoBox(T info) {
            this.serv = info;
            lastObserved = System.currentTimeMillis();
        }

        @Override
        public int hashCode() {
            return (serv == null) ? 0 : serv.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || !(obj instanceof InfoBox))
                return false;
            @SuppressWarnings("unchecked")
            InfoBox<T> _obj = (InfoBox<T>) obj;
            return (serv == null) ? (_obj.serv == null) : serv.equals(_obj.serv);
        }
    }

    public enum Type {
        MULTICAST, BROADCAST
    }
    
    final PeerId peerId;
    private final TSD<PeerInfo<E>> tsd;
    final TransportId transId;
    protected final Set<DiscoveryListener<E>> listeners = Collections
            .newSetFromMap(new ConcurrentHashMap<DiscoveryListener<E>, Boolean>());
    
    protected final ConcurrentLinkedQueue<InfoBox<PeerInfo<E>>> availableInfoQueue = 
            new ConcurrentLinkedQueue<InfoBox<PeerInfo<E>>>();
    private long expireTime = DEFAULT_EXPIRATION_TIME;

    public TSDDiscoverable(PeerId peerId, Type type, TransportId transId)
            throws IOException {
        this.peerId = peerId;
        switch (type) {
        case MULTICAST:
            tsd = MulticastTSD.genTSD();
            break;
        case BROADCAST:
            tsd = BroadcastTSD.genTSD();
            break;
        default:
            tsd = null;
        }
        this.transId = transId;
        tsd.setDiscoveryListener(peerId, transId, this);
    }
    
    public void fin() {
        tsd.setDiscoveryListener(peerId, transId, null);
        tsd.unregisterAllServices(peerId, transId);
        tsd.fin();
        availableInfoQueue.clear();
        listeners.clear();
    }
    
    /**
     * 近傍から受け取ったサービスの有効期限(ms)をセットする。
     * この期限を越えて更新されなかったサービス情報は破棄される。
     * 
     * @param period サービスの有効期限(ms)
     */
    public void setExpireTime(long period) {
        expireTime = period;
    }

    public void register(PeerInfo<E> info) {
        tsd.registerService(peerId, transId, info);
    }
    
    public void unregister(PeerInfo<E> info) {
        tsd.unregisterService(peerId, transId, info);
    }

    /**
     * peerId, receiverの区分で、有効なサービスのリストを取得する。
     * 
     * @param peerId
     * @param receiver
     * @return
     */
    public List<PeerInfo<E>> getAvailablePeerInfos() {
        List<PeerInfo<E>> avail = new ArrayList<PeerInfo<E>>();
        for (InfoBox<PeerInfo<E>> infoBox : availableInfoQueue) {
            avail.add(infoBox.serv);
        }
        return avail;
    }
    
    public boolean addDiscoveryListener(DiscoveryListener<E> listener) {
        return listeners.add(listener);
    }

    public boolean removeDiscoveryListener(DiscoveryListener<E> listener) {
        return listeners.remove(listener);
    }

    @Override
    public void scheduleDiscovery(long delay, long period) {
        tsd.scheduleDiscovery(peerId, transId, delay, period);
    }

    @Override
    public void cancelDiscovery() {
        tsd.cancelDiscovery(peerId, transId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onDiscovered(Object info) {
        InfoBox<PeerInfo<E>> infoBox = new InfoBox<PeerInfo<E>>((PeerInfo<E>) info);
        boolean isNew = !availableInfoQueue.remove(infoBox);
        availableInfoQueue.offer(infoBox);
        for (DiscoveryListener<E> listener : listeners)
            listener.onDiscovered((PeerInfo<E>) info, isNew);
    }

    @Override
    public void onFadeoutCheck() {
        long past = System.currentTimeMillis() - expireTime;
        InfoBox<PeerInfo<E>> infoBox = availableInfoQueue.peek();
        if (infoBox == null) return;
        if (past < infoBox.lastObserved) return;
        availableInfoQueue.poll();
        for (DiscoveryListener<E> listener : listeners)
            listener.onFadeout(infoBox.serv);
    }
}
