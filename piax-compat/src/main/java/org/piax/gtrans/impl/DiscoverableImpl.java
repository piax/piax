/*
 * DiscoverableImpl.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: DiscoverableImpl.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.piax.common.Endpoint;
import org.piax.gtrans.Discoverable;
import org.piax.gtrans.DiscoveryListener;
import org.piax.gtrans.PeerInfo;

/**
 * 
 */
public abstract class DiscoverableImpl<E extends Endpoint> implements
        Discoverable<E> {
    
    protected static final Timer timer = new Timer("RawDiscoveryTimer", true);

    static class InfoBox<E extends Endpoint> {
        final PeerInfo<E> info;
        final long lastObserved;

        InfoBox(PeerInfo<E> info) {
            this.info = info;
            lastObserved = System.currentTimeMillis();
        }

        @Override
        public int hashCode() {
            return (info == null) ? 0 : info.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || !(obj instanceof InfoBox))
                return false;
            @SuppressWarnings("unchecked")
            InfoBox<E> _obj = (InfoBox<E>) obj;
            return (info == null) ? (_obj.info == null) : info.equals(_obj.info);
        }
    }
    
    protected final ConcurrentLinkedQueue<InfoBox<E>> availableInfoQueue = 
            new ConcurrentLinkedQueue<InfoBox<E>>();
    protected final Set<DiscoveryListener<E>> listeners = Collections
            .newSetFromMap(new ConcurrentHashMap<DiscoveryListener<E>, Boolean>());
    private TimerTask task;

    public void fin() {
        availableInfoQueue.clear();
        listeners.clear();
    }

    public boolean addDiscoveryListener(DiscoveryListener<E> listener) {
        return listeners.add(listener);
    }

    public boolean removeDiscoveryListener(DiscoveryListener<E> listener) {
        return listeners.remove(listener);
    }

    protected abstract TimerTask getDiscoveryTask();
    
    @Override
    public synchronized void scheduleDiscovery(long delay, long period) {
        if (task != null)
            task.cancel();
        task = getDiscoveryTask();
        timer.schedule(task, delay, period);
    }

    @Override
    public synchronized void cancelDiscovery() {
        if (task != null) {
            task.cancel();
            task = null;
        }
    }

    @Override
    public List<PeerInfo<E>> getAvailablePeerInfos() {
        List<PeerInfo<E>> avail = new ArrayList<PeerInfo<E>>();
        for (InfoBox<E> infoBox : availableInfoQueue) {
            avail.add(infoBox.info);
        }
        return avail;
    }
    
    protected void found(PeerInfo<E> info) {
        InfoBox<E> infoBox = new InfoBox<E>(info);
        boolean isNew = !availableInfoQueue.remove(infoBox);
        availableInfoQueue.offer(infoBox);
        for (DiscoveryListener<E> listener : listeners)
            listener.onDiscovered(info, isNew);
    }
    
    /**
     * periodで指定された期間(ms)より古いPeerInfoを削除する。
     * 
     * @param period 削除のための指定期間（ms）
     */
    protected void discardOldInfos(long period) {
        long past = System.currentTimeMillis() - period;
        while (true) {
            InfoBox<E> infoBox = availableInfoQueue.peek();
            if (infoBox == null) break;
            if (past < infoBox.lastObserved) break;
            availableInfoQueue.poll();
            for (DiscoveryListener<E> listener : listeners)
                listener.onFadeout(infoBox.info);
        }
    }
}
