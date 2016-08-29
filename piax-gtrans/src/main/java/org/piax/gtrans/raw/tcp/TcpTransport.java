/*
 * TcpTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TcpTransport.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.gtrans.raw.tcp;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.PeerId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.InetTransport;
import org.piax.gtrans.raw.RawChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP用のRawTransportを実現するクラス。
 * 
 * 
 */
public class TcpTransport extends InetTransport<TcpLocator> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(TcpTransport.class);

    static String THREAD_NAME_PREFIX = "tcp-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    /**
     * activeなTcpChannelを保持するスレッドフリーなset
     */
    private final Set<TcpChannel> channels = Collections.newSetFromMap(
            new ConcurrentHashMap<TcpChannel, Boolean>());
    private volatile SocketAcceptor acceptor = null;
    
    /**
     * socketからreadする際に取得するバイト数の最大値
     */
    final int maxReadLen;
    
    public TcpTransport(PeerId peerId, TcpLocator peerLocator) throws IOException {
        super(peerId, peerLocator, true);
        this.maxReadLen = GTransConfigValues.TCP_READ_BUF_LEN;
        // start acceptor
        acceptor = new SocketAcceptor(this, peerLocator.getSocketAddress());
        acceptor.start();
    }
    
    @Override
    public void fin() {
        // stop acceptor
        if (acceptor != null) {
            acceptor.terminate();
            acceptor = null;
        }
        for (TcpChannel ch : channels) {
            ch.close();
        }
        super.fin();
    }

    @Override
    public int getMTU() {
        return maxReadLen;
    }

    @Override
    public RawChannel<TcpLocator> newChannel(TcpLocator dst, boolean isDuplex,
            int timeout) throws IOException {
        TcpChannel ch = new TcpChannel(this,
                ((InetLocator) dst).getSocketAddress(), maxReadLen, timeout);
        new Thread(ch, THREAD_NAME_PREFIX + thNum.getAndIncrement()).start();
        addChannel(ch);
        return ch;
    }

    TcpChannel newAcceptedChannel(Socket soc) throws IOException {
        TcpChannel ch = new TcpChannel(this, soc, maxReadLen);
        new Thread(ch, THREAD_NAME_PREFIX + thNum.getAndIncrement()).start();
        addChannel(ch);
        return ch;
    }
    
    /**
     * SocketAcceptorから異常通知を受ける。
     * 通常は起こらないことなので、エラーログを吐くだけにどどめる。
     * 
     * @param cause SocketAcceptorが受けた例外
     */
    void onHangup(Exception cause) {
        logger.error("", cause);
    }

    private void addChannel(TcpChannel channel) {
        channels.add(channel);
    }

    /**
     * 指定されたTcpChannelをactiveなchannel setから削除する。
     * TcpChannelのclose時にupcallされる。
     * 
     * @param channel TcpChannel
     */
    void removeChannel(TcpChannel channel) {
        channels.remove(channel);
    }
    
    void onReceive(TcpChannel channel) {
        /*
         * TOD caution
         * ここで、srcとしてセットされるchannel.getRemote()が正しくない。
         * 現実な方法としては、connectさせた後、最初のデータとしてsrc locatorを流すのがよいだろう。
         * acceptした直後にsrc locatorを取得して、TcpChannelを作ったらよい。
         */
        if (chListener != null)
            chListener.onReceive(channel);
    }

    boolean onAccepting(TcpChannel channel) {
        if (chListener != null) {
            return chListener.onAccepting(channel);
        } else {
            // TODO ここをどう解釈するか？
            return false;
        }
    }

    void onClosed(TcpChannel channel) {
        channel.close();
        if (chListener != null)
            chListener.onClosed(channel);
    }

    void onFailure(TcpChannel channel, Exception cause) {
        if (chListener != null)
            chListener.onFailure(channel, cause);
        channel.close();
    }
}
