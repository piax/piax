/*
 * LWTcpSocketAcceptor.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.piax.gtrans.raw.lwtcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class LWTcpSocketAcceptor extends Thread {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(LWTcpSocketAcceptor.class);

    /**
     * 受信するconnection(接続要求)のキューの最大長。 キューが埋まった際の接続要求は拒否される。 
     * ServerSocketのデフォルトは50
     */
    static int REQ_QUEUE_LEN = 50;
    
    /**
     * エラーが発生した時に、acceptを再開するまでの時間
     */
    static int SLEEP_TIME_WITH_IOERROR = 1000;// msec

    final LWTcpTransport transport;
    final ServerSocketChannel ssoc;
    private volatile boolean isTerminated;

    LWTcpSocketAcceptor(LWTcpTransport transport, InetSocketAddress sockAddr)
            throws IOException {
        this.transport = transport;
        ssoc = ServerSocketChannel.open();
        ssoc.socket().bind(sockAddr, REQ_QUEUE_LEN);
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
                SocketChannel soc = ssoc.accept();
                LWTcpChannel ch = transport.newAcceptedChannel(soc);
                if (!transport.onAccepting(ch)) {
                    // onAcceptingで拒否された場合（またはTransListenerがない場合）
                    // 直ちにcloseさせる
                    ch.close();
                }
            } catch (IOException e) {
                if (!isTerminated) {
                    /*
                     * 区別できないがToo Many open filesの可能性もある。その場合は、
                     * しばらくすると、回復できる可能性がある。
                     * そのため、ここでは、しばらくsleep後、acceptを再開する。
                     */
                    // unexpected socket error
                    //transport.onHangup(e);
                    logger.warn("IOException on accept socket: sleep {} msec and try again ",
                      SLEEP_TIME_WITH_IOERROR,e);
                    try {
                        Thread.sleep(SLEEP_TIME_WITH_IOERROR);
                    } catch (InterruptedException e1) {
                        // ignore
                    }
                }
                break;
            }
        }
        logger.trace("EXIT:");
    }
}
