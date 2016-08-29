/*
 * DatagramBasedTransport.java
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
/*
 * Revision History:
 * ---
 * 2012/10/31 designed and implemented by M. Yoshida.
 * 
 * $ObjectId: DatagramBasedTransport.java 607 2012-10-31 13:35:46Z yos $
 */

package org.piax.gtrans.impl;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.util.UniqNumberGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel通信機能を提供しない下位のTransportまたはRawTransportを使って、
 * 通常のChannelを使ったTransportの機能を作るためのテンプレートクラス
 * 
 * 
 */
public abstract class DatagramBasedTransport<U extends Endpoint, L extends Endpoint>
        extends ChannelTransportImpl<U> implements TransportListener<L>,
        ChannelListener<L> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DatagramBasedTransport.class);
    
    public static final byte CH_NEW_CMD = 0x01;
    public static final byte CH_NEW_ACK_CMD = 0x02;
    public static final byte CH_NEW_NACK_CMD = 0x03;
    public static final byte CH_CLOSE_CMD = 0x04;
    
    protected static class DatagramChannel<E extends Endpoint> extends ChannelImpl<E> {
        private final CountDownLatch latch;
        
        // new client channel
        DatagramChannel(ChannelTransport<E> mother, ObjectId localObjId,
                ObjectId remoteObjId, E remote) {
            super(mother, localObjId, remoteObjId, remote, true);
            latch = new CountDownLatch(1);
        }
        
        // new accepted channel
        DatagramChannel(PeerId creator, ChannelTransport<E> mother,
                ObjectId localObjId, ObjectId remoteObjId, E remote) {
            super(creator, mother, localObjId, remoteObjId, remote, true);
            latch = null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void close() {
            super.close();
            ((DatagramBasedTransport<E, ?>) mother).closeCh(true, this);
        }

        @SuppressWarnings("unchecked")
        public void send(Object msg) throws IOException {
            int chNo = isCreatorSide() ? getChannelNo() : -getChannelNo();
            ((DatagramBasedTransport<E, ?>) mother)._send(localObjectId,
                    remoteObjectId, chNo, remote, msg);
        }
    }

    /**
     * このTransportがclientとして生成したChannelを管理するオブジェクト。
     * 新規にChannelを生成する際、Channelを識別する番号としてTransport内でユニークなchannelNoを生成する。
     */
    private final UniqNumberGenerator<DatagramChannel<U>> clientChMgr
            = new UniqNumberGenerator<DatagramChannel<U>>(
                        1, 1, GTransConfigValues.MAX_CHANNELS);
    
    /**
     * acceptしたChannelを登録・管理する。
     * 元来、リモートピアが生成したChannelであるため、keyとして、creatorであるpeerIdとそのchannelId
     * のペアを用いる。
     */
    private final AcceptedChannelMgr<U> acceptChMgr = new AcceptedChannelMgr<U>();

    protected DatagramBasedTransport(Peer peer, TransportId transId,
            Transport<?> lowerTrans, boolean supportsDuplex)
            throws IdConflictException {
        super(peer, transId, lowerTrans, supportsDuplex);
    }

    @Override
    public Channel<U> newChannel(ObjectId sender, ObjectId receiver, U dst,
            boolean isDuplex, int timeout) throws ProtocolUnsupportedException,
            IOException {
        DatagramChannel<U> ch = new DatagramChannel<U>(this, sender, receiver,
                dst);
        int chNo = clientChMgr.newNo(ch);
        ch.setChannelNo(chNo);
        // channel newのための制御cmdを発行する
        sendCmd(dst, CH_NEW_CMD, ch);
        // ackを待つ
        try {
            if (timeout == 0) timeout = GTransConfigValues.newChannelTimeout;
            if (!ch.latch.await(timeout, TimeUnit.MILLISECONDS)) {
                ch.latch.countDown();
                throw new NetworkTimeoutException("newChannel timed out: " + dst);
            }
        } catch (InterruptedException e) {
            ch.latch.countDown();
            throw new NetworkTimeoutException("newChannel interrupted: " + dst);
        }
        logger.trace("EXIT:");
        return ch;
    }
    
    protected DatagramChannel<U> getClientCh(int channelNo) {
        return clientChMgr.getObject(channelNo);
    }

    protected DatagramChannel<U> getAcceptCh(PeerId creator, int channelNo) {
        return (DatagramChannel<U>) acceptChMgr.getChannel(creator, channelNo);
    }
    
    protected DatagramChannel<U> newAcceptChIfAbsent(PeerId creator,
            ObjectId localObjId, ObjectId remoteObjId, int channelNo,
            U dst) {
        synchronized (acceptChMgr) {
            DatagramChannel<U> ch = getAcceptCh(creator, channelNo);
            if (ch == null) {
                ch = new DatagramChannel<U>(creator, this, localObjId, remoteObjId, dst);
                ch.setChannelNo(channelNo);
                acceptChMgr.putChannel(creator, channelNo, ch);
            }
            logger.trace("EXIT:");
            return ch;
        }
    }

    private void unbindClientCh(ChannelImpl<U> ch) {
        clientChMgr.discardNo(ch.getChannelNo());
        logger.trace("EXIT:");
    }

    private void unbindAcceptCh(ChannelImpl<U> ch) {
        synchronized (acceptChMgr) {
            acceptChMgr.removeChannel(ch);
        }
        logger.trace("EXIT:");
    }

    /**
     * channelのclose処理を行う。
     * こちら側のtransportがchannelのcloseを指示した場合は、isDirectをtrueにする。
     * 相手側のtransportがchannelのcloseを行った場合（制御メッセージが送られる）はfalseにする。
     * 
     * @param isDirect こちら側のtransportがchannelのcloseを指示した場合はtrue、
     * それ以外はfalseをセットする。
     * @param ch channelオブジェクト
     */
    protected void closeCh(boolean isDirect, ChannelImpl<U> ch) {
        if (isDirect) {
            // channel closeのための制御cmdを発行する
            try {
                sendCmd(ch.getRemote(), CH_CLOSE_CMD, ch);
            } catch (IOException ignore) {}
        }
        if (ch.isCreatorSide()) {
            unbindClientCh(ch);
        } else {
            unbindAcceptCh(ch);
        }
    }
    
    //*** about message sending ***
    
    /*
     * サブクラスで下位層の送信ロジックを埋めるための用いる。
     */
    
    /**
     * サブクラスで下位層の送信処理を実装するために用いるメソッド。
     * 
     * @param dst 送信先
     * @param nmsg NestedMessage
     * @throws ProtocolUnsupportedException プロトコルミスマッチ等の例外が出た場合
     * @throws IOException I/O関係の例外が出た場合
     */
    protected abstract void lowerSend(U dst, NestedMessage nmsg)
            throws ProtocolUnsupportedException, IOException;
    
    private void sendCmd(U dst, byte cmd, ChannelImpl<U> ch) throws IOException {
        int chNo = ch.isCreatorSide() ? ch.getChannelNo() : -ch.getChannelNo();
        // cmdであることを示すため、optionにcmdをセットし、innerはnullにしておく
        NestedMessage nmsg = new NestedMessage(ch.localObjectId, 
                ch.remoteObjectId, getPeerId(), ch.getLocal(), chNo, cmd, null);
        lowerSend(dst, nmsg);
    }

    /*
     * channel, transportからのsendに対し、共通化した処理を行う。
     */
    protected void _send(ObjectId sender, ObjectId receiver, int channelNo,
            U dst, Object msg) throws ProtocolUnsupportedException,
            IOException {
        logger.trace("ENTRY:");
        NestedMessage nmsg = new NestedMessage(sender, receiver, getPeerId(),
                this.getEndpoint(), channelNo, null, msg);
        lowerSend(dst, nmsg);
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, U dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
        _send(sender, receiver, 0, dst, msg);
    }

    //*** about message receiving ***
    
    /**
     * サブクラスで受信時のロジックを埋めるための用いるメソッド。
     * 
     * @param rmsg ReceivedMessage
     * @return 以降の処理に渡す NestedMessage、処理をここで止める場合はnullを返す
     */
    protected abstract NestedMessage _preReceive(ReceivedMessage rmsg);

    /**
     * ここでの受信処理をスレッドを使って並行化させるかどうかを判断する。
     * 通常は、処理数が2以上の場合はスレッド化が望ましい。
     * 下位の処理をすぐにreturnさせたい場合は、処理数が1の時でもtrueを返すよう実装する。
     * 
     * @param numProc procedureの数
     * @return スレッド化する場合true
     */
    protected abstract boolean useReceiverThread(int numProc);

    private void dispatchCmd(NestedMessage nmsg) {
        DatagramChannel<U> ch;
        ChannelListener<U> listener = getChannelListener(nmsg.receiver);
        if (listener == null) {
            logger.warn("message purged as no listener: {}", nmsg.receiver);
        }
        byte cmd = (Byte) nmsg.option;
        switch (cmd) {
        case CH_NEW_CMD:
            ch = newAcceptChIfAbsent(nmsg.srcPeerId, nmsg.receiver, nmsg.sender,
                    nmsg.channelNo, (U) nmsg.src);
            if (listener != null)
                listener.onAccepting(ch);
            // reply ack cmd
            try {
                sendCmd((U) nmsg.src, CH_NEW_ACK_CMD, ch);
            } catch (IOException e) {
                logger.error("", e);
            }
            break;
        case CH_NEW_ACK_CMD:
            if (nmsg.channelNo < 0) {
                ch = getClientCh(-nmsg.channelNo);
                if (ch == null) {
                    logger.error(
                            "CH_NEW_ACK_CMD accepted with invalid channelNo:{}",
                            -nmsg.channelNo);
                } else {
                    // latchの解除
                    ch.latch.countDown();
                }
            } else {
                logger.error("invalid channelNo: {}", nmsg.channelNo);
            }
            break;
        case CH_CLOSE_CMD:
            if (nmsg.channelNo < 0) {
                ch = getClientCh(-nmsg.channelNo);
            } else {
                ch = getAcceptCh(nmsg.srcPeerId, nmsg.channelNo);
            }
            if (ch != null) {
                if (listener != null)
                    listener.onClosed(ch);
                closeCh(false, ch);
            }
            break;
        default:
            logger.error("invalid control command in message header");
        }
    }

    private void dispatchTrans(NestedMessage nmsg) {
        final ReceivedMessage rcvMsg = new ReceivedMessage(nmsg.sender,
                nmsg.src, nmsg.getInner());
        final TransportListener<U> listener = getListener(nmsg.receiver);
        if (listener == null) {
            logger.warn("message purged as transport has no listener: {}", nmsg.receiver);
        }
        if (listener != null) {
            listener.onReceive(DatagramBasedTransport.this, rcvMsg);
        }
    }
    
    private void dispatchCh(final ChannelImpl<U> ch, NestedMessage nmsg) {
        final ChannelListener<U> listener = getChannelListener(nmsg.receiver);
        ch.putReceiveQueue(nmsg.getInner());
        if (listener != null) {
            listener.onReceive(ch);
        }
    }

    protected void raiseUpperListener(NestedMessage nmsg) {
        if (nmsg.channelNo == 0) {
            // datagram case
            dispatchTrans(nmsg);
            return;
        }
        if (nmsg.channelNo < 0) {
            // reply case
            ChannelImpl<U> ch = getClientCh(-nmsg.channelNo);
            if (ch == null) {
                logger.warn("unknown channelNo: {}", -nmsg.channelNo);
                logger.debug("clientChMgr {}", clientChMgr);
                return;
            }
            dispatchCh(ch, nmsg);
        } else {
            // send case
            ChannelImpl<U> ch = newAcceptChIfAbsent(nmsg.srcPeerId, 
                    nmsg.receiver, nmsg.sender, nmsg.channelNo, (U) nmsg.src);
            dispatchCh(ch, nmsg);
        }
    }

    /*
     * 下位からのonReceiveを共通化した処理を行う。
     * このクラスでは、channel, transport情報を無視して処理している。
     */
    protected void _onReceive(ReceivedMessage rmsg) {
        logger.trace("ENTRY:");
        NestedMessage nmsg = _preReceive(rmsg);
        if (nmsg == null) return;
        if (nmsg.getInner() == null && nmsg.option instanceof Byte) {
            // 制御コマンドの処理
            dispatchCmd(nmsg);
        } else {
            raiseUpperListener(nmsg);
        }
    }
    
    public void onReceive(Transport<L> lowerTrans, final ReceivedMessage rmsg) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        if (!this.isActive) {
            logger.info("received msg purged {}", rmsg.getMessage());
            return;
        }
        if (!useReceiverThread(1)) {
            _onReceive(rmsg);
        } else {
            peer.execute(new Runnable() {
                public void run() {
                    // log出力の便宜のため、currentThread名にpeerIdを付与する
                    peer.concatPeerId2ThreadName();
                    _onReceive(rmsg);
                }
            });
        }
    }
    
    public boolean onAccepting(Channel<L> channel) {
        logger.error("unexpected onAccepting");
        return false;
    }

    public void onClosed(Channel<L> channel) {
        logger.error("unexpected onClosed");
    }

    public void onFailure(Channel<L> channel, Exception cause) {
        logger.error("unexpected onFailure");
    }

    public void onReceive(Channel<L> lowerCh) {
        logger.error("unexpected onReceive");
    }
}
