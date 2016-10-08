/*
 * OneToOneMappingTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: OneToOneMappingTransport.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 下位層にTransportを持ち、send, channel.send をそのまま下位層のTransportに流すような
 * Transportを作成するためのテンプレートクラス
 * 
 * 
 */
public abstract class OneToOneMappingTransport<E extends Endpoint> extends
        ChannelTransportImpl<E> implements TransportListener<E>,
        ChannelListener<E> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(OneToOneMappingTransport.class);

    /**
     * OneToOneMappingTransportの内部で使用する下位層のchannelをラップするだけの
     * ChannelImplクラス
     */
    protected static class OneToOneChannel<E extends Endpoint> extends ChannelImpl<E> {
        final Channel<E> lowerCh;
        // new client channel
        OneToOneChannel(Channel<E> lowerCh, ChannelTransport<E> mother,
                ObjectId localObjId, ObjectId remoteObjId, E remote) {
            super(mother, localObjId, remoteObjId, remote, true);
            this.lowerCh = lowerCh;
        }

        // new accepted channel
        OneToOneChannel(Channel<E> lowerCh, PeerId creator,
                ChannelTransport<E> mother, ObjectId localObjId,
                ObjectId remoteObjId, E remote) {
            super(creator, mother, localObjId, remoteObjId, remote, true);
            this.lowerCh = lowerCh;
        }

        @Override
        public void close() {
        	 /* onReceiveの処理途中でclose処理が入ると、accept channelと間違うなど
        	  * おかしな動作をするので、lowerChで排他処理を行う。
        	  * XXX これは十分ではない？
        	  */
        	 synchronized (lowerCh) {
	            super.close();
	            ((OneToOneMappingTransport<E>) mother).removeCh(lowerCh);
	            lowerCh.close();
        	 }
        }

        @Override
        public void send(Object msg) throws IOException {
            ((OneToOneMappingTransport<E>) mother).chSend(this, msg);
        }
    };

    /** 下位層のchannelからOneToOneChannelを引くためのmap */
    private final Map<Channel<E>, OneToOneChannel<E>> relatedChMap = 
            new HashMap<Channel<E>, OneToOneChannel<E>>();

    protected OneToOneMappingTransport(TransportId transId,
            ChannelTransport<E> lowerTrans) throws IdConflictException {
        super(lowerTrans.getPeer(), transId, lowerTrans, lowerTrans.supportsDuplex());
        lowerTrans.setListener(transId, this);
        lowerTrans.setChannelListener(transId, this);
    }

    @Override
    public E getEndpoint() {
        return getLowerTransport().getEndpoint();
    }

    @Override
    public int getMTU() {
        return lowerTrans.getMTU();
    }
    
    /*
     * 下位層にセットするlowerTransは、ChannelTransportでかつ、
     * destination が E と一致することに注意 
     */
    @Override
    public ChannelTransport<E> getLowerTransport() {
        @SuppressWarnings("unchecked")
        ChannelTransport<E> lower = (ChannelTransport<E>) lowerTrans;
        return lower;
    }

    @Override
    public Channel<E> newChannel(ObjectId sender, ObjectId receiver, E dst,
            boolean isDuplex, int timeout) throws ProtocolUnsupportedException,
            IOException {
        Channel<E> lowerCh = getLowerTransport().newChannel(transId, dst,
                isDuplex, timeout);
        OneToOneChannel<E> ch = new OneToOneChannel<E>(lowerCh, this, sender,
                receiver, dst);
        if (lowerCh instanceof ChannelImpl) {
            ch.setChannelNo(lowerCh.getChannelNo());
        }
        putCh(lowerCh, ch);
        return ch;
    }

    protected OneToOneChannel<E> putCh(Channel<E> lowerCh, OneToOneChannel<E> ch) {
        synchronized (relatedChMap) {
            return relatedChMap.put(lowerCh, ch);
        }
    }

    protected void removeCh(Channel<E> lowerCh) {
        synchronized (relatedChMap) {
            relatedChMap.remove(lowerCh);
        }
    }

    protected OneToOneChannel<E> getCh(Channel<E> lowerCh) {
        synchronized (relatedChMap) {
            return relatedChMap.get(lowerCh);
        }
    }

    public boolean onAccepting(Channel<E> lowerCh) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        if (!this.isActive) {
            logger.info("receive event purged");
            return false;
        }
        return true;
    }

    public void onClosed(Channel<E> lowerCh) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        if (!this.isActive) {
            logger.info("receive event purged");
            return;
        }
        ChannelImpl<E> ch = getCh(lowerCh);
        if (ch == null) {
            // 制御用等に使われているlowerChは当該channelとの紐付けがないため、無視される
            return;
        }
        ch.close();
        // ** raise notification
        // このTransportが作成したchannelについては無視する
        if (!ch.remoteObjectId.equals(transId)) {
            ChannelListener<E> listener = getChannelListener(ch.localObjectId);
            if (listener != null)
                listener.onClosed(ch);
        }
    }

    public void onFailure(Channel<E> lowerCh, Exception cause) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        if (!this.isActive) {
            logger.info("receive event purged");
            return;
        }
        ChannelImpl<E> ch = getCh(lowerCh);
        if (ch == null) {
            // 制御用等に使われているlowerChは当該channelとの紐付けがないため、無視される
            return;
        }
        // ** raise notification
        // このTransportが作成したchannelについては無視する
        if (!ch.remoteObjectId.equals(transId)) {
            ChannelListener<E> listener = getChannelListener(ch.localObjectId);
            if (listener != null)
                listener.onFailure(ch, cause);
        }
    }

    // *** about message sending ***

    /*
     * サブクラスで送信時のロジックを埋めるための用いる。
     * 返り値には変換後のmsgを返す。
     * nullを返すと処理を中断する
     */
    protected Object _preSend(ObjectId sender, ObjectId receiver,
            E dst, Object msg) throws IOException {
        return msg;
    };
    
    protected void lowerSend(ObjectId sender, ObjectId receiver,
            E dst, NestedMessage nmsg) throws ProtocolUnsupportedException,
            IOException {
        @SuppressWarnings("unchecked")
        ChannelTransport<E> lower = (ChannelTransport<E>) lowerTrans;
        lower.send(sender, receiver, dst, nmsg);
    }

    protected void lowerChSend(Channel<E> ch, NestedMessage nmsg) throws IOException {
        ch.send(nmsg);
    }
    
    @Override
    public void send(ObjectId sender, ObjectId receiver, E dst, Object msg)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        // サブクラスで定義する送信前処理
        Object _msg = _preSend(sender, receiver, dst, msg);
        // _msgがnullの場合は処理を中断
        if (_msg == null) return;
        // msgをネストさせる
        NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                getEndpoint(), _msg);
        // 下位層のsendを呼ぶ
        lowerSend(transId, transId, dst, nmsg);
    }

    protected void chSend(OneToOneChannel<E> ch, Object msg)
            throws IOException {
        logger.trace("ENTRY:");

        // サブクラスで定義する送信前処理
        Object _msg = _preSend(ch.localObjectId, ch.remoteObjectId, ch.remote, msg);
        // _msgがnullの場合は処理を中断
        if (_msg == null) return;
        // msgをネストさせる
        NestedMessage nmsg = new NestedMessage(ch.localObjectId,
                ch.remoteObjectId, getPeerId(), getEndpoint(), _msg);
        // 下位層のchannel.sendを呼ぶ
        lowerChSend(ch.lowerCh, nmsg);
    }

    // *** about message receiving ***

    /*
     * サブクラスで受信時のロジックを埋めるための用いる。
     * 返り値には変換後のmsgを返す。
     * nullを返すと処理を中断する
     */
    protected Object _postReceive(ObjectId sender, ObjectId receiver,
            E src, Object msg) {
        return msg;
    }

    @SuppressWarnings("unchecked")
    private OneToOneChannel<E> newAcceptCh(Channel<E> lowerCh, NestedMessage nmsg) {
        OneToOneChannel<E> ch = new OneToOneChannel<E>(lowerCh, nmsg.srcPeerId, this,
                nmsg.receiver, nmsg.sender, (E) nmsg.src);
        ch.setChannelNo(lowerCh.getChannelNo());
        if (putCh(lowerCh, ch) != null) {
            logger.error("invaild lower channel accepted");
        }
        return ch;
    }

    public void onReceive(Channel<E> lowerCh) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        if (!this.isActive) {
            logger.info("receive msg purged");
            return;
        }
        NestedMessage nmsg = null;
        OneToOneChannel<E> ch = null;
        synchronized (lowerCh) {
            /*
             * lowerCh.receive()で受理したメッセージを、ch.putReceiveQueueへ渡す順序を
             * 保存するため、synchronized にしている
             */
            if (lowerCh.isClosed()) return; // 既にcloseされていたらなにもしない。
            nmsg = (NestedMessage) lowerCh.receive();
            if (nmsg == null) {
                logger.error("null message received");
                return;
            }
            ch = _putReceiveQueue(lowerCh, nmsg);
        }
        if (ch != null && nmsg != null) {
            _onReceive(ch, nmsg);
        }
    }
    
    // Put the received nested message on the corresponding channel queue.
    // Returns true if successfully finished.
    @SuppressWarnings("unchecked")
    protected OneToOneChannel<E> _putReceiveQueue(Channel<E> lowerCh, NestedMessage nmsg) {
        /*
         * TODO think!
         * この処理では、下位から受信したメッセージを一回lowerCh.receiveしてから、
         * このTransportの持つChannelのBlockingQueueにputしている。
         * この場合は、下位層のBlockingQueueを共有することが効率的であるが、要検討
         */
        logger.trace("ENTRY:");
        OneToOneChannel<E> ch = getCh(lowerCh);
        if (ch == null) {
            // channel未登録であることから、accept channelの処理を行う
            ch = newAcceptCh(lowerCh, nmsg);
            // listenerにdispatchする
            ChannelListener<E> listener = getChannelListener(nmsg.receiver);
            if (listener != null) {
                if (!listener.onAccepting(ch)) {
                    // listenerにacceptを拒否された場合
                    removeCh(lowerCh);
                    /*
                     * TODO 単純にメッセージを破棄する。
                     * acceptの拒否を送信元に送る必要があるが、このためには、newChannel時に
                     * 制御メッセージを交換する必要があるため、ペンディングにしておく。
                     */
                    return null;
                }
            }
        }
        if (lowerCh.isClosed()) return null;
        // サブクラスで定義する受信後処理を呼ぶ
        Object _msg = _postReceive(nmsg.sender, nmsg.receiver, (E) nmsg.src,
                nmsg.getInner());
        // _msgがnullの場合は処理を中断
        if (_msg == null) return null;
        // chにinner msgをputする
        ch.putReceiveQueue(_msg);
        return ch;
    }

    protected void _onReceive(Channel<E> ch, NestedMessage nmsg) {
        if (ch != null) {
            // listenerにdispatchする
            ChannelListener<E> listener = getChannelListener(nmsg.receiver);
            if (listener != null) {
                listener.onReceive(ch);
            }
        }
    }

    public void onReceive(Transport<E> lowerTrans, ReceivedMessage rmsg) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        if (!this.isActive) {
            logger.info("received msg purged {}", rmsg);
            return;
        }
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        _onReceive(lowerTrans, nmsg);
    }

    @SuppressWarnings("unchecked")
    protected void _onReceive(Transport<E> lowerTrans, NestedMessage nmsg) {
        logger.trace("ENTRY:");
        // サブクラスで定義する受信後処理を呼ぶ
        Object _msg = _postReceive(nmsg.sender, nmsg.receiver, (E) nmsg.src,
                nmsg.getInner());
        // _msgがnullの場合は処理を中断
        if (_msg == null) return;
        // listenerにdispatchする
        TransportListener<E> listener = getListener(nmsg.receiver);
        if (listener != null) {
            listener.onReceive(this, new ReceivedMessage(nmsg.sender,
                    nmsg.src, _msg));
        }
    }
}
