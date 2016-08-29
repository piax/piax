/*
 * BluetoothChannel.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BluetoothChannel.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.bluetooth;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.microedition.io.StreamConnection;

import org.piax.gtrans.raw.RawChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class BluetoothChannel extends RawChannel<BluetoothLocator> implements Runnable {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(BluetoothChannel.class);

    final boolean isServer;
    final BluetoothTransport transport;
    final StreamConnection conn;
    final int port;
    final InputStream in;
    final OutputStream out;
    final int maxReadLen;
    final byte[] readBuf;
    private volatile boolean isClosed;
    private volatile long lastActivated;

    BluetoothChannel(boolean isServer, BluetoothTransport transport,
            StreamConnection conn, int port, int maxReadLen) throws IOException {
        this.isServer = isServer;
        this.conn = conn;
        this.transport = transport;
        this.port = port;
        in = conn.openInputStream();
        out = conn.openOutputStream();
        this.maxReadLen = maxReadLen;
        readBuf = new byte[maxReadLen];
        isClosed = false;
        lastActivated = System.currentTimeMillis();
    }
    
    @Override
    public boolean isCreatorSide() {
        return isServer;
    }

    private long getIdleTime() {
        long t = System.currentTimeMillis() - lastActivated;
        return t;
    }
    
    @Override
    public synchronized void close() {
        logger.trace("ENTRY:");
        if (isClosed) return;
        isClosed = true;
        super.close();
        try {
            in.close();
        } catch (IOException ignore) {}
        try {
            out.close();
        } catch (IOException ignore) {}
        try {
            conn.close();
        } catch (IOException ignore) {}
        transport.removeChannel(this);
        logger.trace("EXIT:");
    }
    
    /**
     * channelが生きているかチェックし、生きている場合は、
     * 並行スレッドにより破棄されないために、lastActivatedを更新しておく。
     * このメソッドは、並行スレッドの排他制御のためのatomicな処理になる。
     * 
     * @return channelが生きている場合はtrue、それ以外はfalse
     */
    synchronized boolean checkActiveAndTouch() {
        if (isClosed) return false;
        lastActivated = System.currentTimeMillis();
        return true;
    }
    
    /**
     * channelが指定したpurgeTimeより長くアイドル状態にある場合は、
     * channelを終了させる。そうでない場合は、何もしないで、falseを返す。
     * このメソッドは、並行スレッドの排他制御のためのatomicな処理になる。
     * 
     * @param purgeTime 破棄時間
     * @return channelが終了した場合はtrue、それ以外はfalse
     */
    synchronized boolean closeIfPurged(long purgeTime) {
        if (isClosed) return true;
        if (getIdleTime() < purgeTime) return false;
        close();
        return true;
    }
    
    @Override
    public synchronized void send(ByteBuffer bbuf) throws IOException {
        out.write(bbuf.array(), 0, bbuf.remaining());
        out.flush();
        logger.debug("send: wrote bytes {}", bbuf.remaining());
        lastActivated = System.currentTimeMillis();
    }
    
    public void run() {
        logger.trace("ENTRY:");
        while (true) {
            try {
                int n = in.read(readBuf);
                if (n == -1) {
                    /*
                     * 相手のsocket(out)がshutdownされたことを受信側で確認したので、
                     * transportに通知する。
                     * closeをここでは呼ばない。これを呼び出すのはtransportの役割。
                     */
                    if (!isClosed) transport.onClosed(this);
                    break;
                }
                logger.debug("run: received body bytes {}", n);
                lastActivated = System.currentTimeMillis();
                ByteBuffer bb = ByteBuffer.wrap(Arrays.copyOf(readBuf, n));
                putReceiveQueue(bb);
                transport.onReceive(this);
            } catch (IOException e) {
                /*
                 * この例外が起こるのは次のケース。
                 * ・すでにcloseが呼ばれて、socketがcloseされた。
                 * ・予期せぬエラー
                 * 後者は、transportに通知。
                 */
                if (!isClosed) {
                    // unexpected socket error
                    transport.onFailure(this, e);
                }
                break;
            }
        }
        logger.trace("EXIT:");
    }
    
    /*
     * TODO caution!!
     * TCPと同様、Bluetoothの場合も、remoteアドレスは通常解らない。
     */
    @Override
    public BluetoothLocator getRemote() {
        return null;
    }

    @Override
    public int getChannelNo() {
        return port;
    }
}
