/*
 * LWTcpTransport.java
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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.piax.common.PeerId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.InetTransport;
import org.piax.gtrans.raw.RawChannel;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP用のRawTransportを実現するクラス。
 * 
 * 
 */
public class LWTcpTransport extends InetTransport<TcpLocator> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(LWTcpTransport.class);

    /**
     * activeなTcpChannelを保持するスレッドフリーなset
     */
    private final Set<LWTcpChannel> channels = Collections
            .newSetFromMap(new ConcurrentHashMap<LWTcpChannel, Boolean>());
    private volatile LWTcpSocketAcceptor acceptor = null;

    /**
     * socketからreadする際に取得するバイト数の最大値
     */
    final int maxReadLen;

    /**
     * socketの受信がreadyになるのを待つためのセレクタ
     */
    final Selector selector;

    /**
     * 受信処理を行うためのスレッドプール
     */
    static int TCP_RT_CORE_THREADS = 0;
    static int TCP_RT_MAX_THREADS = 10000;
    static int TCP_RT_KEEP_ALIVE_TIME = 500;
    final ThreadPoolExecutor executor;

    /**
     * 
     */
    boolean linger0;

    public boolean linger0Option() {
        return linger0;
    }

    /**
     * 受信がreadyになるのを待ち、スレッドプールのスレッドにディスパッチする
     */
    private void dispatch() {
        try {
            while (true) {
                // select keyを変更するために、transportで排他制御する。
                synchronized (this) {
                    // do nothing
                } 
                // wakeup後の最初のselectは、直ちに戻るので、こことselectの間にwakeupが入っても大丈夫
                if (selector.select() > 0 && !Thread.currentThread().isInterrupted()) {
                    Set<SelectionKey> skeys = selector.selectedKeys();
                    for (SelectionKey key : skeys) {
                        synchronized (key) {
                            if (!key.isValid())
                                continue;
                            if (key.isReadable()) {
                                LWTcpChannel ch = (LWTcpChannel) key
                                        .attachment();
                                if (ch != null) {
                                    // select対象からREADを外す
                                    key.interestOps(key.interestOps()
                                            & (~SelectionKey.OP_READ));
                                    executor.execute(ch);// 受信処理
                                } else {
                                    logger.error("No channel mapped with a key");
                                }
                            }
                            if (key.isWritable()) {
                                LWTcpChannel ch = (LWTcpChannel) key
                                        .attachment();
                                if (ch != null) {
                                    // select 対象からWRITEを外す
                                    key.interestOps(key.interestOps()
                                            & (~SelectionKey.OP_WRITE));
                                    ch.wakeupWritable(); // 送信再開
                                } else {
                                    logger.error("No channel mapped with a key");
                                }
                            }
                        }
                    }
                    skeys.clear();
                }
                if (Thread.currentThread().isInterrupted())
                    break;
            }
        } catch (IOException e) {
            logger.error("select exception, stop receiving:" + e);
        }
    }

    /**
     * dispatchを実行するスレッド
     */
    private Thread dispatcher;

    public LWTcpTransport(PeerId peerId, TcpLocator peerLocator,
            boolean linger0Option) throws IOException {
        super(peerId, peerLocator, true);
        linger0 = linger0Option;
        executor = new ThreadPoolExecutor(TCP_RT_CORE_THREADS,
                TCP_RT_MAX_THREADS, TCP_RT_KEEP_ALIVE_TIME,
                TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(),
                new ThreadPoolExecutor.AbortPolicy());
        // スレッドが不足の場合は、いずれかのスレッドが終了するまで待つ
        executor.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor exc) {
                if (exc.isShutdown())
                    return;
                BlockingQueue<Runnable> q = exc.getQueue();
                try {
                    q.put(r);// いずれかのスレッドが終了し、タスクを受取るまでブロックする
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        });
        this.maxReadLen = GTransConfigValues.TCP_READ_BUF_LEN;
        selector = Selector.open();
        // start acceptor
        acceptor = new LWTcpSocketAcceptor(this, peerLocator.getSocketAddress());
        acceptor.setName("lwtcp-acceptor:" + peerId);
        acceptor.start();
        dispatcher = new Thread("lwtcp-receive-dispatcher:" + peerId) {
            @Override
            public void run() {
                dispatch();
            }
        };
        dispatcher.start();
    }

    public LWTcpTransport(PeerId peerId, TcpLocator peerLocator)
            throws IOException {
        this(peerId, peerLocator, false);
    }

    @Override
    public void fin() {
        // stop dispatcher
        dispatcher.interrupt();
        selector.wakeup();
        executor.shutdown();
        // stop acceptor
        if (acceptor != null) {
            acceptor.terminate();
            acceptor = null;
        }
        for (LWTcpChannel ch : channels) {
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
        LWTcpChannel ch = new LWTcpChannel(this,
                ((InetLocator) dst).getSocketAddress(), maxReadLen, selector);
        addChannel(ch);
        return ch;
    }

    LWTcpChannel newAcceptedChannel(SocketChannel soc) throws IOException {
        LWTcpChannel ch = new LWTcpChannel(this, soc, maxReadLen, selector);
        addChannel(ch);
        return ch;
    }

    /**
     * SocketAcceptorから異常通知を受ける。 通常は起こらないことなので、エラーログを吐くだけにどどめる。
     * 
     * @param cause SocketAcceptorが受けた例外
     */
    void onHangup(Exception cause) {
        logger.error("", cause);
    }

    private void addChannel(LWTcpChannel channel) {
        channels.add(channel);
    }

    /**
     * 指定されたTcpChannelをactiveなchannel setから削除する。 
     * TcpChannelのclose時にupcallされる。
     * 
     * @param channel TcpChannel
     */
    void removeChannel(LWTcpChannel channel) {
        channels.remove(channel);
    }

    void onReceive(LWTcpChannel channel) {
        /*
         * TOD caution ここで、srcとしてセットされるchannel.getRemote()が正しくない。
         * 現実な方法としては、connectさせた後、最初のデータとしてsrc locatorを流すのがよいだろう。 
         * acceptした直後にsrc locatorを取得して、TcpChannelを作ったらよい。
         */
        if (chListener != null)
            chListener.onReceive(channel);
    }

    boolean onAccepting(LWTcpChannel channel) {
        if (chListener != null) {
            return chListener.onAccepting(channel);
        } else {
            // TODO ここをどう解釈するか？
            return false;
        }
    }

    void onClosed(LWTcpChannel channel) {
        channel.close();
        if (chListener != null)
            chListener.onClosed(channel);
    }

    void onFailure(LWTcpChannel channel, Exception cause) {
        if (chListener != null)
            chListener.onFailure(channel, cause);
        channel.close();
    }
}
