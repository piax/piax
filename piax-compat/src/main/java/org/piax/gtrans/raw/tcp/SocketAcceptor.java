/*
 * SocketAcceptor.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: SocketAcceptor.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class SocketAcceptor extends Thread {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(SocketAcceptor.class);

    /**
     * 受信するconnection(接続要求)のキューの最大長。 キューが埋まった際の接続要求は拒否される。 ServerSocketのデフォルトは
     * 50
     */
    static int REQ_QUEUE_LEN = 50;

    final TcpTransport transport;
    final ServerSocket ssoc;
    private volatile boolean isTerminated;

    SocketAcceptor(TcpTransport transport, InetSocketAddress sockAddr)
            throws IOException {
        this.transport = transport;
        ssoc = new ServerSocket(sockAddr.getPort(), REQ_QUEUE_LEN,
                sockAddr.getAddress());
        isTerminated = false;
    }

    synchronized void terminate() {
        logger.trace("ENTRY:");
        if (isTerminated)
            return;
        isTerminated = true;
        try {
            // causes ServerSocket#accept exception!
            ssoc.close();
        } catch (IOException ignore) {
        }
        logger.trace("EXIT:");
    }

    boolean isTerminated() {
        return isTerminated;
    }

    @Override
    public void run() {
        logger.trace("ENTRY:");
        while (!isTerminated) {
            try {
                Socket soc = ssoc.accept();
                TcpChannel ch = transport.newAcceptedChannel(soc);
                if (!transport.onAccepting(ch)) {
                    // onAcceptingで拒否された場合（またはTransListenerがない場合）
                    // 直ちにcloseさせる
                    ch.close();
                }
            } catch (IOException e) {
                if (!isTerminated) {
                    // unexpected socket error
                    transport.onHangup(e);
                }
                break;
            }
        }
        logger.trace("EXIT:");
    }
}
