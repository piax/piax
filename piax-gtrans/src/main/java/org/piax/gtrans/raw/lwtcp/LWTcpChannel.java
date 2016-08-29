/*
 * LWTcpChannel.java
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
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.TimerTask;

import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.NoSuchPeerException;
import org.piax.gtrans.raw.RawChannel;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class LWTcpChannel extends RawChannel<TcpLocator> implements Runnable {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(LWTcpChannel.class);

    static int READ_BUFFER_KEEP_TIME = 100;

    final boolean isServer;
    final LWTcpTransport transport;
    final SocketChannel soc;
    final int maxReadLen;
    final SelectionKey skey;
    private volatile boolean isClosed;
    private Object writableWaitObject = new Object();
    private TimerTask timerTask = null;

    /**
     * server channel を生成する。
     * 
     * @param transport TcpChannelTransportオブジェクト
     * @param soc Socket
     * @throws IOException IO関係の例外が発生した場合
     */
	LWTcpChannel(LWTcpTransport transport, SocketChannel soc, int maxReadLen,
			Selector sel) throws IOException {
		isServer = true;
		this.soc = soc;
		soc.configureBlocking(false);
		this.transport = transport;
		soc.socket().setSendBufferSize(GTransConfigValues.SOCKET_SEND_BUF_SIZE);
		soc.socket().setReceiveBufferSize(
				GTransConfigValues.SOCKET_RECV_BUF_SIZE);
		this.maxReadLen = maxReadLen;
		remote = new TcpLocator((InetSocketAddress) soc.socket()
				.getRemoteSocketAddress());
		isClosed = false;
		synchronized (transport) {
			// selectから抜けさせるためにwakeupを呼ぶ。次のselectに入るのを防ぐために、transportで排他制御する。
			sel.wakeup();
			// select実行中はブロックすることに注意
			skey = soc.register(sel, SelectionKey.OP_READ, this);
		}
	}

    /**
     * client channel を生成する。
     * 
     * @param transport TcpChannelTransportオブジェクト
     * @param dst SocketAddress
     * @throws IOException IO関係の例外が発生した場合
     */
    LWTcpChannel(LWTcpTransport transport, SocketAddress dst, int maxReadLen,
            Selector sel) throws IOException {
        isServer = false;
        soc = SocketChannel.open();
        try {
            soc.connect(dst);
        } catch (SocketException e) {
            // SocketExceptionが発生した場合は、通信先のPeerが存在しないとみなす
            throw new NoSuchPeerException(e + ": " + dst);
         }
        soc.configureBlocking(false);
        this.transport = transport;
        soc.socket().setSendBufferSize(GTransConfigValues.SOCKET_SEND_BUF_SIZE);
        soc.socket().setReceiveBufferSize(GTransConfigValues.SOCKET_RECV_BUF_SIZE);
        this.maxReadLen = maxReadLen;
        remote = new TcpLocator((InetSocketAddress) soc.socket().getRemoteSocketAddress());
        isClosed = false;
        synchronized (transport) {
            // selectから抜けさせるためにwakeupを呼ぶ。次のselectに入るのを防ぐために、transportで排他制御する。
            sel.wakeup();
            // select実行中はブロックすることに注意
            skey = soc.register(sel, SelectionKey.OP_READ, this);
        }
    }

    @Override
    public boolean isCreatorSide() {
        return isServer;
    }

    /*
     * connection終了の手順
     * shutdownOut ---------> EOF受信
     * close                  (readループ終了) 
     * (readループ終了)         shutdownOut
     *                        close
     */

    @Override
    public void close() {
        synchronized (transport) {
            logger.trace("ENTRY:");
            if (isClosed)
                return;
            isClosed = true;
            skey.cancel();
            super.close();
            try {
                if (transport.linger0Option()) {
                	  soc.socket().setSoLinger(true, 0);
                } else {
                    soc.socket().shutdownOutput();
                  }
                soc.close();
            } catch (IOException ignore) {
            }
            transport.removeChannel(this);
            wakeupWritable();
            logger.trace("EXIT:");
        }
    }

    public void wakeupWritable() {
        synchronized (writableWaitObject) {
            writableWaitObject.notifyAll();
        }
    }

    @Override
    public synchronized void send(ByteBuffer bbuf) throws IOException {
        bbuf.rewind();
        int n = bbuf.remaining();
        while (bbuf.remaining() > 0) {
            int r = soc.write(bbuf);
            if (r == 0) {
                synchronized (transport) {
                    // selectから抜けさせるためにwakeupを呼ぶ。次のselectに入るのを防ぐために、
                    // transportで排他制御する。
                    skey.selector().wakeup();
                    synchronized (skey) {
                        skey.interestOps(skey.interestOps()
                                | SelectionKey.OP_WRITE); // Select対象にWRITEを追加
                    }
                }
                synchronized (writableWaitObject) {
                    if ((skey.interestOps() & SelectionKey.OP_WRITE) != 0) {
                        try {
                            writableWaitObject.wait();
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            }
        }
        logger.debug("send: wrote bytes {}", n);
    }

    /**
     * GCを待たずにDirectByteBufferを開放する。 JVMの実装に依存しているので注意
     * 失敗しても元々で、GCで開放されるはず。
     * 
     * @param buffer
     */
    public static void freeDirectBuffer(ByteBuffer buffer) {
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            cleanMethod.invoke(cleaner);
        } catch (Throwable e) {
            logger.error("Freeing direct buffer:e " + e);
        }
    }

    public void run() {
        logger.trace("ENTRY:");
        if (timerTask != null) {
            timerTask.cancel();
        }
        ByteBuffer readBuf = ByteBuffer.allocateDirect(maxReadLen);
        try {
            int n;
            while ((n = soc.read(readBuf)) > 0) {
                logger.debug("run: received body bytes {}", n);

                ByteBuffer bb = ByteBuffer.allocate(n);
                readBuf.flip();
                bb.put(readBuf);
                bb.flip();
                putReceiveQueue(bb);

                transport.onReceive(this);
                readBuf.clear();
            }
            if (n == -1) {
                /*
                 * 相手のsocket(out)がshutdownされたことを受信側で確認したので、 transportに通知する。
                 * closeをここでは呼ばない。closeを呼び出すのはtransportの役割なため。
                 */
                if (!isClosed)
                    transport.onClosed(this);
            } else {
                synchronized (transport) {
                    // selectから抜けさせるためにwakeupを呼ぶ。次のselectに入るのを防ぐために、
                    // transportで排他制御する。
                    skey.selector().wakeup();
                    synchronized (skey) {
                        if (skey.isValid()) {
                            skey.interestOps(skey.interestOps()
                                    | SelectionKey.OP_READ); // Select対象にReadを追加
                        }
                    }
                }
            }

        } catch (IOException e) {
            /*
             * この例外が起こるのは次のケース。
             *  ・すでにcloseが呼ばれて、socketがcloseされた。
             *  ・予期せぬエラー
             * 後者は、transportに通知。
             */
            if (!isClosed) {
                // unexpected socket error
                transport.onFailure(this, e);
            }
        }
        freeDirectBuffer(readBuf);
        logger.trace("EXIT:");
    }

    private TcpLocator remote = null; // for cache

    /*
     * TODO caution!!
     * TCPでacceptしたチャネルの場合、remoteアドレスはclient側のbindポートになり、destination
     * としてconnect
     * 指定可能なアドレスではない。
     */
    @Override
    public TcpLocator getRemote() {
        return remote;
    }

    @Override
    public int getChannelNo() {
        return isServer ? soc.socket().getPort() : soc.socket().getLocalPort();
    }
}
