/*
 * BluetoothLocator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * Copyright (c) 2016 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BluetoothLocator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.bluetooth;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.bluetooth.LocalDevice;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.raw.RawTransport;
import org.piax.util.BinaryJsonner;


/**
 * BluetoothのためのPeerLocatorを表現するクラス。
 * 
 * 
 */
public class BluetoothLocator extends PeerLocator {
    private static final long serialVersionUID = 1L;

    private static BluetoothLocator local = null;
    public static final byte btType = 4;

    // Register as a plug-in at loading time.
    static {
        init();
    }
    // this method is required for plugin.
    static void init() {
        magicMap.put(new Byte(btType), BluetoothLocator.class);
    }
    
    public static synchronized BluetoothLocator getLocal() throws IOException {
        if (local != null) return local;
        String addr = LocalDevice.getLocalDevice().getBluetoothAddress();
        local = new BluetoothLocator(addr);
        return local;
    }
    
    // below values should be set by BluetoothTransport
    final String macAddr;
    
    public BluetoothLocator(String macAddr) {
        this.macAddr = macAddr;
    }
    
    public BluetoothLocator(ByteBuffer bb) {
        byte[] mac = new byte[12];
        bb.get(mac);
        this.macAddr = new String(mac);
    }

    public String getAddr() {
        return macAddr;
    }

    @Override
    public RawTransport<BluetoothLocator> newRawTransport(PeerId peerId)
            throws IOException {
        if (this != local) {
            throw new IOException("locator should be local address");
        }
        return new BluetoothTransport(peerId, this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof BluetoothLocator)) {
            return false;
        }
        return macAddr.equals(((BluetoothLocator) obj).macAddr);
    }
    
    @Override
    public void serialize(ByteBuffer bb) {
        bb.put(btType).put(getAddr().getBytes());
    }
    
    @Override
    public int hashCode() {
        return macAddr.hashCode();
    }
    
    @Override
    public String toString() {
        return macAddr;
    }
}
