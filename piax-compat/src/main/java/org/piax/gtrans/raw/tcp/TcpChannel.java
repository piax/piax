/*
 * TcpChannel.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TcpChannel.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.gtrans.raw.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.NoSuchPeerException;
import org.piax.gtrans.raw.RawChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class TcpChannel extends RawChannel<TcpLocator> implements Runnable {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(TcpChannel.class);

    final boolean isServer;
    final TcpTransport transport;
    final Socket soc;
    final InputStream in;
    final OutputStream out;
    final int maxReadLen;
    final byte[] readBuf;
    private volatile boolean isClosed;
    private volatile long lastActivated;

    /**
     * server channel を生成する。
     * 
     * @param transport TcpChannelTransportオブジェクト
     * @param soc Socket
     * @throws IOException IO関係の例外が発生した場合
     */
    TcpChannel(TcpTransport transport, Socket soc, int maxReadLen)
            throws IOException {
        isServer = true;
        this.soc = soc;
        this.transport = transport;
//        soc.setReuseAddress(true);
//        soc.setSoLinger(true, 0);
        soc.setSendBufferSize(GTransConfigValues.SOCKET_SEND_BUF_SIZE);
        soc.setReceiveBufferSize(GTransConfigValues.SOCKET_RECV_BUF_SIZE);
        in = soc.getInputStream();
        out = soc.getOutputStream();
        this.maxReadLen = maxReadLen;
        readBuf = new byte[maxReadLen];
        isClosed = false;
        lastActivated = System.currentTimeMillis();
    }

    /**
     * client channel を生成する。
     * 
     * @param transport TcpChannelTransportオブジェクト
     * @param dst SocketAddress
     * @throws IOException IO関係の例外が発生した場合
     */
    TcpChannel(TcpTransport transport, SocketAddress dst, int maxReadLen, int timeout)
            throws IOException {
        isServer = false;
        soc = new Socket();
        try {
            soc.connect(dst, timeout);
        } catch (SocketException e) {
            // SocketExceptionが発生した場合は、通信先のPeerが存在しないとみなす
            throw new NoSuchPeerException(e + ": " + dst);
        }
        this.transport = transport;
//        soc.setReuseAddress(true);
//        soc.setSoLinger(true, 0);
        soc.setSendBufferSize(GTransConfigValues.SOCKET_SEND_BUF_SIZE);
        soc.setReceiveBufferSize(GTransConfigValues.SOCKET_RECV_BUF_SIZE);
        in = soc.getInputStream();
        out = soc.getOutputStream();
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
    
    /*
     * connection終了の手順
     * shutdownOut ---------> EOF受信
     * close                  (readループ終了) 
     * (readループ終了)         shutdownOut
     *                        close
     */
    
    @Override
    public synchronized void close() {
        logger.trace("ENTRY:");
        if (isClosed) return;
        isClosed = true;
        super.close();
        try {
            soc.shutdownOutput();
            soc.close();
        } catch (IOException ignore) {
            logger.info("", ignore);
        }
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
                     * closeをここでは呼ばない。closeを呼び出すのはtransportの役割なため。
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
    
    private TcpLocator remote = null;  // for cache

    /*
     * TODO caution!!
     * TCPの場合、remoteアドレスはclient側のbindポートになり、destination としてconnect
     * 指定可能なアドレスではない。
     */
    @Override
    public TcpLocator getRemote() {
        if (remote == null) {
            remote = new TcpLocator((InetSocketAddress) soc.getRemoteSocketAddress());
        }
        return remote;
    }

    @Override
    public int getChannelNo() {
        return isServer ? soc.getPort() : soc.getLocalPort();
    }
}
