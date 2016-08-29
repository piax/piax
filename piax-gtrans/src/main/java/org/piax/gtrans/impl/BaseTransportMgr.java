/*
 * BaseTransportMgr.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BaseTransportMgr.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.raw.LocatorStatusObserver;

/**
 * 
 */
public class BaseTransportMgr implements LocatorStatusObserver {
    /*--- logger ---*/
//    private static final Logger logger = 
//        LoggerFactory.getLogger(BaseTransportMgr.class);

    public static LinkStatAndScoreIf statAndScore = null;

    private final List<Transport<?>> baseTransList = 
            new ArrayList<Transport<?>>();

    final Peer peer;
    BaseTransportGenerator baseTransGen;
    private volatile Transport<?> recentlyUsedTransport;
    
    private final Set<LocatorStatusObserver> observers = 
            new CopyOnWriteArraySet<LocatorStatusObserver>();
    
    public BaseTransportMgr(Peer peer) {
        this.peer = peer;
        baseTransGen = new DefaultBaseTransportGenerator(peer);
    }

    public void fin() {
        synchronized (baseTransList) {
            for (Transport<?> bt : baseTransList) {
                bt.fin();
            }
        }
    }
    
    public synchronized void addBaseTransportGenerator(BaseTransportGenerator generator) {
        baseTransGen.addLast(generator);
    }
    
    public synchronized void addFirstBaseTransportGenerator(BaseTransportGenerator generator) {
        BaseTransportGenerator prev = baseTransGen;
        baseTransGen = generator;
        baseTransGen.addNext(prev);
    }

    public boolean hasMatchedBaseTransport(PeerLocator target) {
        synchronized (baseTransList) {
            for (Transport<?> bt : baseTransList) {
                if (bt.getEndpoint().equals(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    public <E extends PeerLocator> Transport<E> newBaseTransport(
            String desc, TransportId transId, E myLocator) throws IOException,
            IdConflictException {
        if (myLocator == null)
            throw new IllegalArgumentException("me should not be null");
        synchronized (baseTransList) {
            if (getRelatedBaseTransport(myLocator) != null) {
                throw new IOException("this locator is already used:" + myLocator);
            }
            Transport<E> bt = baseTransGen.newBaseTransport(desc, transId, myLocator);
            baseTransList.add(0, bt);
            onEnabled(myLocator, true);
            return bt;
        }
    }
    
    public <E extends PeerLocator> ChannelTransport<E> newBaseChannelTransport(
            String desc, TransportId transId, E myLocator) throws IOException,
            IdConflictException {
        if (myLocator == null)
            throw new IllegalArgumentException("me should not be null");
        synchronized (baseTransList) {
            if (getRelatedBaseTransport(myLocator) != null) {
                throw new IOException("this locator is already used:" + myLocator);
            }
            ChannelTransport<E> bt = baseTransGen.newBaseChannelTransport(
                    desc, transId, myLocator);
            baseTransList.add(0, bt);
            onEnabled(myLocator, true);
            return bt;
        }
    }

    public List<Transport<?>> getBaseTransports() {
        synchronized (baseTransList) {
            return new ArrayList<Transport<?>>(baseTransList);
        }
    }

    public List<TransportIdPath> getBaseTransportIdPaths() {
        synchronized (baseTransList) {
            List<TransportIdPath> paths = new ArrayList<TransportIdPath>();
            for (Transport<?> bt : baseTransList) {
                paths.add(bt.getTransportIdPath());
            }
            return paths;
        }
    }

    /**
     * Remove the specified PeerLocator
     */
    @SuppressWarnings("unchecked")
    public <E extends PeerLocator> Transport<E> removeBaseTransport(E myLocator) {
        Transport<E> match = null;
        synchronized (baseTransList) {
            for (Transport<?> bt : baseTransList) {
                if (bt.getEndpoint().equals(myLocator)) {
                    match = (Transport<E>) bt;
                    break;
                }
            }
            if (match == null) {
                return null;
            } else {
                match.fin();
                baseTransList.remove(match);
                return match;
            }
        }
    }

    /**
     * Remove all PeerLocators
     */
    public void removeAllBaseTransports() {
        synchronized (baseTransList) {
            for (Transport<?> bt : baseTransList) {
                bt.fin();
            }
            baseTransList.clear();
        }
    }
    
    /**
     * isUpであるローカルPeerのlocatorをリストとして返す。
     * 
     * @return isUpであるローカルPeerのlocatorのリスト
     */
    public List<PeerLocator> getAvailableLocators() {
        List<PeerLocator> locs = new ArrayList<PeerLocator>();
        synchronized (baseTransList) {
            for (Transport<?> bt : baseTransList) {
                // downしているbtは除く
                if (!bt.isUp()) continue;
                locs.add((PeerLocator) bt.getEndpoint());
            }
        }
        return locs;
    }

    /**
     * 指定されたmyLocatorに対応するBaseTransportを返す。
     * 存在しない場合は、nullが返る。
     * 
     * @param myLocator PeerLocatorオブジェクト
     * @return myLocatorと対応するBaseTransport、存在しない場合はnull
     */
    @SuppressWarnings("unchecked")
    public <E extends PeerLocator> Transport<E> getRelatedBaseTransport(E localLocator) {
        synchronized (baseTransList) {
            for (Transport<?> bt : baseTransList) {
                if (bt.getEndpoint().equals(localLocator)) {
                    return (Transport<E>) bt;
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <E extends PeerLocator> Transport<E> getApplicableBaseTransport(E target) {
        synchronized (baseTransList) {
            // 型が一致する
            for (Transport<?> bt : baseTransList) {
                // downしているbtは除く
                if (!bt.isUp()) continue;
                if (((PeerLocator) bt.getEndpoint()).sameClass(target)) {
                    // TODO 暫定候補
                    recentlyUsedTransport = bt;
                    return (Transport<E>) bt;
                }
            }
            return null;
        }
    }

    public Transport<?> getRecentlyUsedTransport() {
        /*
         * TODO think!
         * 暫定的に最後にセットされたlocatorをdefaultとしている
         */
        synchronized (baseTransList) {
            if (recentlyUsedTransport != null && recentlyUsedTransport.isUp()) {
                return recentlyUsedTransport;
            }
            for (Transport<?> bt : baseTransList) {
                // downしているbtは除く
                if (!bt.isUp()) continue;
//                recentlyUsedTransport = bt;
                return bt;
            }
            return null;
        }
    }

    private int score(PeerLocator src, PeerLocator dst) {
        if (statAndScore == null) return 0;
        Integer[] e = statAndScore.eval(src, dst);
        return e[e.length - 1];
    }

    public static class ConnectionStat {
        public final PeerLocator src;
        public final PeerLocator dst;
        public final String stat;
        public final int score;
        public ConnectionStat(PeerLocator src, PeerLocator dst, String stat, 
                int score) {
            this.src = src;
            this.dst = dst;
            this.stat = stat;
            this.score = score;
        }
    }
    
    public List<ConnectionStat> getConnections(Collection<? extends PeerLocator> remoteLocs) {
        List<ConnectionStat> conns = new ArrayList<ConnectionStat>();
        for (PeerLocator loc : remoteLocs) {
            Transport<?> bt = getApplicableBaseTransport(loc);
            if (bt != null) {
                String stat;
                int score;
                if (statAndScore == null) {
                    stat = "";
                    score = 0;
                } else {
                    Object[] e = statAndScore.eval((PeerLocator) bt.getEndpoint(), loc);
                    stat = String.format(statAndScore.evalFormat(), e);
                    score = (Integer) e[e.length - 1];
                }
                conns.add(new ConnectionStat(
                        (PeerLocator) bt.getEndpoint(), loc, stat, score));
            }
        }
        return conns;
    }
    
    public PeerLocator bestRemoteLocator(Collection<? extends PeerLocator> remoteLocs) {
        PeerLocator best = null;
        int bestScore = -1;
        for (PeerLocator loc : remoteLocs) {
            Transport<?> bt = getApplicableBaseTransport(loc);
            if (bt != null) {
                int score = score((PeerLocator) bt.getEndpoint(), loc);
                if (score > bestScore) {
                    bestScore = score;
                    best = loc;
                }
            }
        }
        return best;
    }
    
    public void registerObserver(LocatorStatusObserver observer) {
        observers.add(observer);
    }
    
    public void unregisterObserver(LocatorStatusObserver observer) {
        observers.remove(observer);
    }
    
    public void onEnabled(PeerLocator loc, boolean isNew) {
        for (LocatorStatusObserver ob : observers) {
            ob.onEnabled(loc, isNew);
        }
    }

    public void onFadeout(PeerLocator loc, boolean isFin) {
        if (isFin) {
            removeBaseTransport(loc);
        }
        for (LocatorStatusObserver ob : observers) {
            ob.onFadeout(loc, isFin);
        }
    }

    public void onChanging(PeerLocator oldLoc, PeerLocator newLoc) {
        for (LocatorStatusObserver ob : observers) {
            ob.onChanging(oldLoc, newLoc);
        }
    }

    public void onHangup(PeerLocator loc, Exception cause) {
        removeBaseTransport(loc);
        for (LocatorStatusObserver ob : observers) {
            ob.onHangup(loc, cause);
        }
    }

    @Override
    public String toString() {
        return "BaseTransportMgr{baseTrans=" + getBaseTransports()
                + ", recentlyUsedTransport=" + recentlyUsedTransport + "}";
    }
}
