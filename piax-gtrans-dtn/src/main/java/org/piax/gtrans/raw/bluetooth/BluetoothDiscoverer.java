/*
 * BluetoothDiscoverer.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BluetoothDiscoverer.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.bluetooth;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import javax.bluetooth.BluetoothStateException;
import javax.bluetooth.DataElement;
import javax.bluetooth.DeviceClass;
import javax.bluetooth.DiscoveryAgent;
import javax.bluetooth.LocalDevice;
import javax.bluetooth.RemoteDevice;
import javax.bluetooth.ServiceRecord;
import javax.bluetooth.UUID;

import org.piax.common.PeerId;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.PeerInfo;
import org.piax.gtrans.impl.DiscoverableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.piax.common.PeerId;

/**
 * 
 */
public class BluetoothDiscoverer extends DiscoverableImpl<BluetoothLocator>
        implements javax.bluetooth.DiscoveryListener {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(BluetoothDiscoverer.class);

    static final int[] attrIDs = new int[] {0x0100};
    static final UUID[] uuidSet = new UUID[] {BluetoothTransport.serviceUUID};
    public static final long DEFAULT_EXPIRATION_TIME = 5 * 60 * 1000;
    
    private static BluetoothDiscoverer instance = null;
    
    public static synchronized BluetoothDiscoverer getInstance()
            throws BluetoothStateException {
        if (instance != null) return instance;
        instance = new BluetoothDiscoverer();
        return instance;
    }
    
    private volatile long expireTime = DEFAULT_EXPIRATION_TIME;
    final DiscoveryAgent agent;
    
    /**
     * これまでに見つかった RemoteDevice をとにかく蓄積する Map。
     * 古いものは捨てるというロジックが必要。
     */
    final Map<String, RemoteDevice> pooledDevs = 
            new ConcurrentHashMap<String, RemoteDevice>();
    private List<RemoteDevice> cachedDevs;
    
    // 同期用のMap
    final Map<Integer, SynchronousQueue<ServiceRecord>> syncs = 
            new ConcurrentHashMap<Integer, SynchronousQueue<ServiceRecord>>();
    
    private BluetoothDiscoverer() throws BluetoothStateException {
        agent = LocalDevice.getLocalDevice().getDiscoveryAgent();
        setDiscoveryEnabled(true);
    }

    /**
     * シングルトンインスタンスなので、実際には機能をリセットするだけ。
     */
    @Override
    public void fin() {
        stopDiscovery();
        pooledDevs.clear();
        setDiscoveryEnabled(false);
        super.fin();
    }
    
    public void setExpireTime(long period) {
        expireTime = period;
    }

    public void register(PeerInfo<BluetoothLocator> info) {
        throw new UnsupportedOperationException();
    }

    public void unregister(PeerInfo<BluetoothLocator> info) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deviceDiscovered(RemoteDevice dev, DeviceClass cod) {
        logger.trace("ENTRY:");
        if (dev == null) return;
        pooledDevs.put(dev.getBluetoothAddress(), dev);
        if (cachedDevs != null && cachedDevs.contains(dev)) return;
        if (logger.isDebugEnabled()) {
            String name;
            try {
                name = dev.getFriendlyName(false);
            } catch (IOException e) {
                name = "none";
            }
            logger.debug("Bluetooth macAddr:{} name:{}",
                    dev.getBluetoothAddress(), name);
        }
        try {
            int reqNo = agent.searchServices(attrIDs, uuidSet, dev, this);
            logger.debug("search reqNo: {}", reqNo);
        } catch (BluetoothStateException e) {
            // 発行できる searchServices の上限を越えた場合
            // cancelInquiry をする
            logger.info("", e);
            agent.cancelInquiry(this);
        }
    }

    @Override
    public void inquiryCompleted(int inqResult) {
        logger.trace("ENTRY:");
        /*
         * 発見成功:0, 発見失敗:7, cancel:5 が返されるが、
         * 特に結果に対する対応はしない。 
         */
        logger.debug("inqResult {}", inqResult);
    }

    @Override
    public void servicesDiscovered(int reqNo, ServiceRecord[] servRecord) {
        logger.trace("ENTRY:");
        for (ServiceRecord sr : servRecord) {
            SynchronousQueue<ServiceRecord> queue = syncs.get(reqNo);
            if (queue != null) {
                queue.offer(sr);
                agent.cancelServiceSearch(reqNo);
                break;
            }
            String addr = sr.getHostDevice().getBluetoothAddress();
            DataElement de = sr.getAttributeValue(0x0100);
            if (de == null) continue;
            String peerName = (String) de.getValue();
            if (peerName == null) {
                logger.warn("peerName is null");
                break;
            }
            PeerId pId = new PeerId(peerName);
            PeerInfo<BluetoothLocator> info = new PeerInfo<BluetoothLocator>(
                    pId, new BluetoothLocator(addr));
            found(info);
            logger.debug("peerInfo: {}", info);
            // 見つかったらすぐにキャンセルする
            agent.cancelServiceSearch(reqNo);
            break;
        }
    }
    
    @Override
    public void serviceSearchCompleted(int reqNo, int result) {
        logger.trace("ENTRY:");
        /*
         * 完了:1, cancel:2, エラー:3, no service:4, 不到達:6 が返されるが、
         * 特に結果に対する対応はしない。 
         */
        if (result == 3 || result == 6) {
            logger.debug("reqNo:{} result:{}", reqNo, result);
        } else {
            logger.debug("reqNo:{} result:{}", reqNo, result);
        }
    }

    public boolean setDiscoveryEnabled(boolean onOff) {
        int mode = onOff ? DiscoveryAgent.GIAC : DiscoveryAgent.NOT_DISCOVERABLE;
        try {
            LocalDevice.getLocalDevice().setDiscoverable(mode);
            return true;
        } catch (BluetoothStateException e) {
            logger.info("", e);
            return false;
        }
    }

    public void startDiscovery() throws BluetoothStateException {
        logger.trace("ENTRY:");
        cachedDevs = null;
        RemoteDevice[] devs = agent.retrieveDevices(DiscoveryAgent.CACHED);
        if (devs != null) {
            try {
                for (RemoteDevice dev : devs) {
                    int reqNo = agent.searchServices(attrIDs, uuidSet, dev,
                            this);
                    logger.debug("search reqNo: {}", reqNo);
                }
            } catch (BluetoothStateException e) {
                // 発行できる searchServices の上限を越えた場合
                // ここで中断する。
                logger.info("", e);
                return;
            }
            cachedDevs = Arrays.asList(devs);
        }
        // device searchを行う
        agent.startInquiry(DiscoveryAgent.GIAC, this);
        logger.trace("EXIT:");
    }
    
    public void stopDiscovery() {
        logger.trace("ENTRY:");
        agent.cancelInquiry(this);
        /*
         * 緻密に行うなら、cancelServiceSearch も行うべきであるが、searchServicesはすぐに
         * 終了するので、何もしないことにする
         */
        logger.trace("EXIT:");
    }
    
    ServiceRecord searchServiceRecord(String macAddr, int timeout)
            throws NetworkTimeoutException, IOException {
        logger.trace("ENTRY:");
        logger.debug("macAddr {}", macAddr);
        logger.debug("pooledDevs {}", pooledDevs);
        RemoteDevice rdev = pooledDevs.get(macAddr);
        if (rdev == null) return null;
        int reqNo;
        reqNo = agent.searchServices(attrIDs, uuidSet, rdev, this);
        SynchronousQueue<ServiceRecord> queue = new SynchronousQueue<ServiceRecord>();
        syncs.put(reqNo, queue);
        try {
            ServiceRecord sr = queue.poll(timeout, TimeUnit.MILLISECONDS);
            syncs.remove(reqNo);
            if (sr == null) {
                throw new NetworkTimeoutException("searching timed out");
            }
            return sr;
        } catch (InterruptedException ignore) {
            throw new NetworkTimeoutException("searching interrupted");
        }
    }

    @Override
    protected TimerTask getDiscoveryTask() {
        return new TimerTask() {
            @Override
            public void run() {
                try {
                    startDiscovery();
                } catch (BluetoothStateException e) {
                    logger.info("", e);
                }
                discardOldInfos(expireTime);
            }
        };
    }
    
    public static void main(String[] args) throws Exception {
        BluetoothDiscoverer bd = new BluetoothDiscoverer();
        bd.startDiscovery();
        Thread.sleep(25000);
    }
}
