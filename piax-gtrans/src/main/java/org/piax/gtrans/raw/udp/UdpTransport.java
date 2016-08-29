/*
 * UdpTransportService.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: UdpTransport.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.gtrans.raw.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.PeerId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.NoSuchPeerException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.InetTransport;
import org.piax.gtrans.raw.RawChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDP用のRawTransportを実現するクラス。
 * IPフラグメンテーションを抑制するためのMTU以下へのパケット分割の処理は行っていない。
 * 
 * 
 */
public class UdpTransport extends InetTransport<UdpLocator> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(UdpTransport.class);
    
    static String THREAD_NAME_PREFIX = "udp-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    class SocketListener extends Thread {
        private final byte[] in = new byte[getMTU() + 1];
            // +1 しているのは、MTUを越えて受信するケースをエラーにするため
        private DatagramPacket inPac = new DatagramPacket(in, in.length);

        SocketListener() {
            super(THREAD_NAME_PREFIX + thNum.getAndIncrement());
        }
        
        @Override
        public void run() {
            while (!isTerminated) {
                try {
                    recvSoc.receive(inPac);
                    int len = inPac.getLength();
                    UdpLocator src = new UdpLocator(
                            (InetSocketAddress) inPac.getSocketAddress());
                    receiveBytes(src, Arrays.copyOf(in, len));
                } catch (IOException e) {
                    if (isTerminated) break;  // means shutdown
                    logger.warn("", e);      // temp I/O error
                }
            }
        }
    }

    private final DatagramSocket sendSoc;
    private final DatagramSocket recvSoc;
    private final SocketListener socListener;
    private volatile boolean isTerminated;
    
    public UdpTransport(PeerId peerId, UdpLocator peerLocator) throws IOException {
        super(peerId, peerLocator, true);
        // create socket
        int port = peerLocator.getPort();
        // send用のsocketでは明示的なbindをしない
        sendSoc = new DatagramSocket(); 
        recvSoc = new DatagramSocket(port, peerLocator.getInetAddress());
        sendSoc.setSendBufferSize(GTransConfigValues.SOCKET_SEND_BUF_SIZE);
        recvSoc.setReceiveBufferSize(GTransConfigValues.SOCKET_RECV_BUF_SIZE);
        
        // start lister thread
        isTerminated = false;
        socListener = new SocketListener();
        socListener.start();
    }

    @Override
    public void fin() {
        if (isTerminated) {
            return;     // already terminated
        }
        super.fin();
        // terminate the socListener thread
        isTerminated = true;
        sendSoc.close();
        recvSoc.close();
        try {
            socListener.join();
        } catch (InterruptedException e) {}
    }

    @Override
    public int getMTU() {
        return GTransConfigValues.MAX_PACKET_SIZE;
    }

    void receiveBytes(UdpLocator src, byte[] msg) {
        if (msg.length > getMTU()) {
            logger.error("receive data over MTU:" + msg.length + "bytes");
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(msg);
        ReceivedMessage rmsg = new ReceivedMessage(null, src, bb);
        if (this.listener != null)
            this.listener.onReceive(this, rmsg);
    }

    @Override
    public void send(UdpLocator toPeer, Object msg) throws IOException {
        ByteBuffer bbuf = (ByteBuffer) msg;
        int len = bbuf.remaining();
        if (len > getMTU()) {
            logger.error("send data over MTU:" + len + "bytes");
            return;
        }
        // send
        try {
            sendSoc.send(new DatagramPacket(bbuf.array(), len,
                    ((InetLocator) toPeer).getSocketAddress()));
        } catch (SocketException e) {
            // SocketExceptionが発生した場合は、通信先のPeerが存在しないとみなす
            throw new NoSuchPeerException(e);
        }
    }

    @Override
    public RawChannel<UdpLocator> newChannel(UdpLocator dst, boolean isDuplex,
            int timeout) throws IOException {
        throw new UnsupportedOperationException();
    }
}
