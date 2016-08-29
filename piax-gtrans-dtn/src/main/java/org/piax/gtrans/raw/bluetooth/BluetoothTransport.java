/*
 * BluetoothTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BluetoothTransport.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.bluetooth;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.bluetooth.DataElement;
import javax.bluetooth.LocalDevice;
import javax.bluetooth.ServiceRecord;
import javax.bluetooth.UUID;
import javax.microedition.io.Connector;
import javax.microedition.io.StreamConnection;

import org.piax.common.PeerId;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.raw.RawChannel;
import org.piax.gtrans.raw.RawTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class BluetoothTransport extends RawTransport<BluetoothLocator> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(BluetoothTransport.class);

    static String THREAD_NAME_PREFIX = "blueth-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    public static int NEWCHANNEL_TIMEOUT = 12000;
    public static UUID serviceUUID = new UUID(0x50494158);  // means "PIAX"
    public static int DEFAULT_MAX_READ_LEN = 4*1024;
    private static final UUID RFCOMM_UUID = new UUID(0x0003);
    
    public static String[] btProperties = new String[] {
        "bluetooth.api.version",
        "bluetooth.connected.devices.max",
        "bluetooth.sd.trans.max",
        "bluetooth.connected.inquiry.scan",
        "bluetooth.connected.inquiry",
    };
    
    @SuppressWarnings("rawtypes")
    public static int getRFCOMMPort(ServiceRecord sr) {
        DataElement protoList = sr.getAttributeValue(0x0004);
        try {
            for (Enumeration en = (Enumeration) protoList.getValue(); 
                    en.hasMoreElements();) {
                DataElement proto = (DataElement) en.nextElement();
                for (Enumeration en2 = (Enumeration) proto.getValue(); 
                        en2.hasMoreElements();) {
                    DataElement uuid = (DataElement) en2.nextElement();
                    if (RFCOMM_UUID.equals(uuid.getValue())) {
                        DataElement port = (DataElement) en2.nextElement();
                        return (int) port.getLong();
                    }
                }
            }
        } catch (ClassCastException e) {
            logger.error("", e);
        }
        return -1;
    }

    /**
     * activeなchannelを保持するスレッドフリーなset
     */
    private final Set<BluetoothChannel> channels = Collections.newSetFromMap(
            new ConcurrentHashMap<BluetoothChannel, Boolean>());
    private volatile ConnectionAcceptor acceptor = null;
    final LocalDevice localDev;
    
    /**
     * connectionからreadする際に取得するバイト数の最大値
     */
    final int maxReadLen;
    final String listenerURL;
    final PeerId peerId;
    
    BluetoothTransport(PeerId peerId, BluetoothLocator locator) throws IOException {
        super(peerId, locator, true);
        this.peerId = peerId;
        // Bluetoothが使用可能でない場合は例外を発行する
        localDev = LocalDevice.getLocalDevice();
        if (!LocalDevice.isPowerOn()) {
            throw new IOException("Bluetooth device is power off");
        }
        // update given BluetoothLocator's address
        listenerURL = "btspp://localhost:" + serviceUUID + ";name=" + getPeerId();
        logger.debug("listenerURL {}", listenerURL);
        this.maxReadLen = DEFAULT_MAX_READ_LEN;
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("\n    ** Bluetooth device properties **");
            sb.append("\n    MAC addr: " + localDev.getBluetoothAddress());
            for (String p : btProperties) {
                sb.append("\n    " + p + ": " + LocalDevice.getProperty(p));
            }
            logger.debug(sb.toString());
        }
        // start acceptor
        acceptor = new ConnectionAcceptor(this, listenerURL);
        acceptor.start();
    }
    
    @Override
    public void fin() {
        // stop acceptor
        if (acceptor != null) {
            acceptor.terminate();
            acceptor = null;
        }
        for (BluetoothChannel ch : channels) {
            ch.close();
        }
        super.fin();
    }

    @Override
    public PeerId getPeerId() {
        return peerId;
    }

    @Override
    public int getMTU() {
        return maxReadLen;
    }

    @Override
    public boolean hasStableLocator() {
        return false;
    }

    @Override
    public RawChannel<BluetoothLocator> newChannel(BluetoothLocator dst) 
            throws ProtocolUnsupportedException, IOException {
        return newChannel(dst, true, NEWCHANNEL_TIMEOUT);
    }

    @Override
    public RawChannel<BluetoothLocator> newChannel(BluetoothLocator dst,
            boolean isDuplex, int timeout) throws IOException {
        String macAddr = ((BluetoothLocator) dst).getAddr();
        ServiceRecord sr = BluetoothDiscoverer.getInstance()
                .searchServiceRecord(macAddr, timeout);
        String url = sr.getConnectionURL(ServiceRecord.NOAUTHENTICATE_NOENCRYPT, false);
        StreamConnection conn = (StreamConnection) Connector.open(url);
        int port = getRFCOMMPort(sr);
        BluetoothChannel ch = new BluetoothChannel(false, this, conn, port, maxReadLen);
        new Thread(ch, THREAD_NAME_PREFIX + thNum.getAndIncrement()).start();
        addChannel(ch);
        return ch;
    }

    BluetoothChannel newAcceptedChannel(StreamConnection conn, int port) 
            throws IOException {
        BluetoothChannel ch = new BluetoothChannel(true, this, conn, port, maxReadLen);
        new Thread(ch, THREAD_NAME_PREFIX + thNum.getAndIncrement()).start();
        addChannel(ch);
        return ch;
    }
    
    /**
     * ConnectionAcceptorから異常通知を受ける。
     * 通常は起こらないことなので、エラーログを吐くだけにどどめる。
     * 
     * @param cause SocketAcceptorが受けた例外
     */
    void onHangup(Exception cause) {
        logger.error("", cause);
    }

    private void addChannel(BluetoothChannel channel) {
        channels.add(channel);
    }

    /**
     * 指定されたBluetoothChannelをactiveなchannel setから削除する。
     * BluetoothChannelのclose時にupcallされる。
     * 
     * @param channel BluetoothChannel
     */
    void removeChannel(BluetoothChannel channel) {
        channels.remove(channel);
    }
    
    void onReceive(BluetoothChannel channel) {
        if (chListener != null)
            chListener.onReceive(channel);
    }

    boolean onAccepting(BluetoothChannel channel) {
        if (chListener != null) {
            return chListener.onAccepting(channel);
        } else {
            // TODO ここをどう解釈するか？
            return false;
        }
    }

    void onClosed(BluetoothChannel channel) {
        channel.close();
        if (chListener != null)
            chListener.onClosed(channel);
    }

    void onFailure(BluetoothChannel channel, Exception cause) {
        if (chListener != null)
            chListener.onFailure(channel, cause);
        channel.close();
    }

//    @Override
//    public void send(BluetoothLocator toPeer, ByteBuffer bbuf) throws IOException {
//        throw new UnsupportedOperationException();
//    }
}
