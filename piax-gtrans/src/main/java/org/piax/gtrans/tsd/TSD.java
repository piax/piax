/*
 * TSD.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TSD.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transport Service Discovery （TSD） のためのテンプレートクラス。
 * <p>
 * TSDは、mDNS のプロトコルに従って、近傍のピアとサービス情報の交換を行う仕組みを提供する。
 * サービス情報として、ピアのアドレス情報、つまり <peerId, endpoint> の組を交換することが主な用途であるが、
 * これ以外にも一般のオブジェクトを扱うことができる。
 * <p>
 * このようにTSDは汎用性を持った近傍との情報交換のクラスであるが、交換したいサービス毎にインスタンスを生成
 * することは非効率であるため、サービスを交換したいアプリが共通のTSDを使うようにしている。
 * サービスを交換するアプリの識別にはObjectIdを用いる。交換されるサービス情報にはこのObjectIdが付与
 * されて、同じObjectIdを持つリモートアプリに通知される。
 * <p>
 * TSDを使ってピア情報を交換する代表的なアプリに、gtransのDiscoverableがある。Bluetooth以外の
 * TCP/IP通信はTSDを使って、Discoverable にしている。
 * <p>
 * TSDの実装には、UDPのマルチキャストを使う MulticastTSDとブロードキャストを使うBroadcastTSDの2つがある。
 * 
 */
public abstract class TSD<T> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(TSD.class);

    /** schedule timer for TSD */
    protected static final Timer timer = new Timer("TSDTimer", true);

    /**
     * TSDを使用する objectId ごと発行するTimerTask。
     * advertiseAll と discardOldInfos を定期的に呼び出す。
     * <p>
     * TODO
     * checkDiscarding の呼び出しタイミングに改良の余地あり。
     */
    class DiscoveryTask extends TimerTask {
        final PeerId peerId;
        final ObjectId receiver;

        DiscoveryTask(PeerId peerId, ObjectId receiver) {
            this.peerId = peerId;
            this.receiver = receiver;
        }

        @Override
        public void run() {
            advertiseAll(peerId, receiver);
            checkDiscarding(peerId, receiver);
        }
    }
    
    /*
     * TODO
     * availableとlocalサービスを持つリスト。
     * objectId毎に管理を分けた方がよい。
     */
    private final List<ServiceInfo<T>> localServices = new ArrayList<ServiceInfo<T>>();

    /*
     * TODO
     * リスナーとタイマーをobjectId毎に管理するためのMap。
     * この2つは統合化できる。
     * keyはpeerId+objIdの文字列（やや手抜き）
     */
    private final Map<String, TSDListener> listenersByUpper = 
            new ConcurrentHashMap<String, TSDListener>();
    private final Set<PeerId> usingPeers = new CopyOnWriteArraySet<PeerId>();
    private final Map<String, DiscoveryTask> discoveryTasks = 
            new ConcurrentHashMap<String, DiscoveryTask>();

    public abstract void fin();
    
    private String getKey(PeerId peerId, ObjectId receiver) {
        return peerId.toString() + "/" + receiver.toString();
    }
    
    public void setDiscoveryListener(PeerId peerId, ObjectId receiver, TSDListener listener) {
        usingPeers.add(peerId);
        if (listener == null) {
            listenersByUpper.remove(getKey(peerId, receiver));
        } else {
            listenersByUpper.put(getKey(peerId, receiver), listener);
        }
    }

    public TSDListener getDiscoveryListener(PeerId peerId, ObjectId receiver) {
        return listenersByUpper.get(getKey(peerId, receiver));
    }

    /**
     * ServiceInfo<T>を広告する。
     * このメソッドは下位層で実装する。
     * 
     * @param info
     * @throws IOException
     */
    protected abstract void advertise(ServiceInfo<T> info) throws IOException;
    
    public void registerService(PeerId peerId, ObjectId receiver, T info) {
        ServiceInfo<T> serv = new ServiceInfo<T>(info, peerId, receiver);
        synchronized (localServices) {
            if (localServices.contains(serv)) return;
            localServices.add(serv);
        }
        try {
            advertise(serv);
        } catch (IOException e) {
            logger.warn("", e);
        }
    }
    
    public void unregisterService(PeerId peerId, ObjectId receiver, T info) {
        ServiceInfo<T> serv = new ServiceInfo<T>(info, peerId, receiver);
        synchronized (localServices) {
            localServices.remove(serv);
        }
    }
    
    public void unregisterAllServices(PeerId peerId, ObjectId receiver) {
        synchronized (localServices) {
            Iterator<ServiceInfo<T>> it = localServices.iterator();
            while (it.hasNext()) {
                ServiceInfo<T> sinfo = it.next();
                if (peerId.equals(sinfo.peerId) && receiver.equals(sinfo.objId))
                    it.remove();
            }
        }
    }

    /**
     * peerId, receiverの区分で登録されているすべてのlocalServiceを広告する。
     * 
     * @param peerId
     * @param receiver
     */
    protected void advertiseAll(PeerId peerId, ObjectId receiver) {
        try {
            synchronized (localServices) {
                for (ServiceInfo<T> sinfo : localServices) {
                    if (peerId.equals(sinfo.peerId) && receiver.equals(sinfo.objId))
                        advertise(sinfo);
                }
            }
        } catch (IOException e) {
            logger.warn("", e);
        }
    }

    /**
     * peerId, receiverの区分で、discoveryTasksのタイマー登録をする。
     * 
     * @param peerId
     * @param receiver
     * @param delay
     * @param period
     */
    public void scheduleDiscovery(PeerId peerId, ObjectId receiver, long delay, long period) {
        DiscoveryTask task = new DiscoveryTask(peerId, receiver);
        DiscoveryTask old = discoveryTasks.put(getKey(peerId, receiver), task);
        if (old != null)
            old.cancel();
        timer.schedule(task, delay, period);
    }

    /**
     * peerId, receiverの区分でセットしたタイマーをキャンセルする。
     * 
     * @param peerId
     * @param receiver
     */
    public void cancelDiscovery(PeerId peerId, ObjectId receiver) {
        DiscoveryTask task = discoveryTasks.remove(getKey(peerId, receiver));
        if (task != null)
            task.cancel();
    }
    
    /**
     * ServiceInfo<T>が発見されたことを通知する。
     * このメソッドは下位層から呼ばれる。
     * 
     * @param serv
     */
    protected void found(ServiceInfo<T> serv) {
        for (PeerId p : usingPeers) {
            if (p.equals(serv.peerId)) continue;
            TSDListener listener = listenersByUpper.get(getKey(p, serv.objId));
            if (listener != null)
                listener.onDiscovered(serv.info);
        }
    }
    
    /**
     * 削除タイミングをTSDListenerに通知する。
     */
    protected void checkDiscarding(PeerId peerId, ObjectId receiver) {
        TSDListener listener = listenersByUpper.get(getKey(peerId, receiver));
        if (listener != null)
            listener.onFadeoutCheck();
    }
}
