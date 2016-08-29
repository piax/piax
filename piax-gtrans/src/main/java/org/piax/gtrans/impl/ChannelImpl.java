/*
 * ChannelImpl.java - An abstract class of Channel implementations.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * $Id: ChannelImpl.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.NetworkTimeoutException;

/**
 * Channelオブジェクトを実装するための部品として使用するabstractクラス
 */
public abstract class ChannelImpl<E extends Endpoint> implements Channel<E> {
    
    /**
     * Channelをcloseする際に、ブロック中のreceiveメソッドを正常に終了させるためのpoisonオブジェクト 
     */
    private static final Object END_OF_MESSAGE = new Object();
    private static final Object NULL_MESSAGE = new Object();
    
    protected final PeerId creator;
    protected final ChannelTransport<E> mother;
    protected final ObjectId localObjectId;
    protected final ObjectId remoteObjectId;
    protected final E remote;
    protected final boolean isDuplex;
    private int channelNo;
    
    private final BlockingQueue<Object> rcvQueue;
    
    /**
     * ChannelImplオブジェクトがアクティブな状態であることを示す。
     * close() が呼ばれた場合はfalseとなる。
     */
    private volatile boolean isActive = true;

    /**
     * メッセージ送信時にChannel生成をするためのコンストラクタ。
     * 
     * @param mother このChannelオブジェクトの生成主となるChannelTransportオブジェクト
     * @param localObjectId　ローカル側に位置するエンティティのobject ID
     * @param remoteObjectId リモート側に位置するエンティティのobject ID
     * @param remote リモート側のEndpoint
     */
    ChannelImpl(ChannelTransport<E> mother, ObjectId localObjectId,
            ObjectId remoteObjectId, E remote) {
        this(null, mother, localObjectId, remoteObjectId, remote, true);
    }
    
    /**
     * メッセージ送信時にChannel生成をするためのコンストラクタ。
     * 
     * @param mother このChannelオブジェクトの生成主となるChannelTransportオブジェクト
     * @param localObjectId　ローカル側に位置するエンティティのobject ID
     * @param remoteObjectId リモート側に位置するエンティティのobject ID
     * @param remote リモート側のEndpoint
     * @param isDuplex 双方向指定
     */
    ChannelImpl(ChannelTransport<E> mother, ObjectId localObjectId,
            ObjectId remoteObjectId, E remote, boolean isDuplex) {
        this(null, mother, localObjectId, remoteObjectId, remote, isDuplex);
    }

    /**
     * メッセージ受信時にChannel生成をするためのコンストラクタ。
     * creatorには元のchannelを生成したpeerのpeer IDをセットする。
     * 
     * @param creater 元のchannelを生成したpeerのpeer ID
     * @param mother このChannelオブジェクトの生成主となるChannelTransportオブジェクト
     * @param localObjectId　ローカル側に位置するエンティティのobject ID
     * @param remoteObjectId リモート側に位置するエンティティのobject ID
     * @param remote リモート側のEndpoint
     */
    ChannelImpl(PeerId creater, ChannelTransport<E> mother,
            ObjectId localObjectId, ObjectId remoteObjectId, E remote) {
        this(creater, mother, localObjectId, remoteObjectId, remote, true);
    }
    
    /**
     * メッセージ受信時にChannel生成をするためのコンストラクタ。
     * creatorには元のchannelを生成したpeerのpeer IDをセットする。
     * 
     * @param creater 元のchannelを生成したpeerのpeer ID
     * @param mother このChannelオブジェクトの生成主となるChannelTransportオブジェクト
     * @param localObjectId　ローカル側に位置するエンティティのobject ID
     * @param remoteObjectId リモート側に位置するエンティティのobject ID
     * @param remote リモート側のEndpoint
     * @param isDuplex 双方向指定
     */
    ChannelImpl(PeerId creator, ChannelTransport<E> mother,
            ObjectId localObjectId, ObjectId remoteObjectId, E remote,
            boolean isDuplex) {
        this.creator = creator;
        this.localObjectId = localObjectId;
        this.remoteObjectId = remoteObjectId;
        this.mother = mother;
        this.remote = remote;
        this.isDuplex = !mother.supportsDuplex() ? false : isDuplex;
        rcvQueue = new LinkedBlockingQueue<Object>();
    }
    
    public void close() {
        isActive = false;
        try {
            rcvQueue.clear();
            rcvQueue.put(END_OF_MESSAGE);
        } catch (InterruptedException e) {
            /*
             * interrupted状態はクリアされている。
             *  戻り先で確認できるように、再びinterrupted状態にする。
             */
            Thread.currentThread().interrupt();
        }
    }

    public boolean isClosed() {
        return !isActive;
    }
    
    /**
     * Channelオブジェクトがアクティブな状態であるかどうかをチェックする。
     * close() が呼ばれてインアクティブな状態である場合は、IllegalStateExceptionがthrowされる。
     * サブクラスの場合も含め、close() の後に呼び出されては困る場合のメソッド呼び出しの際のチェックに用いる。
     * 
     * @throws IllegalStateException Channelオブジェクトがインアクティブな状態である場合
     */
    protected void checkActive() throws IllegalStateException {
        if (!isActive)
            throw new IllegalStateException("this channel " 
                    + mother.getTransportId() + "(" + channelNo + ")"
                    + " is already closed");
    }

    public TransportId getTransportId() {
        return mother.getTransportId();
    }

    /**
     * Channelが内部的に持つ番号をセットする。
     * 
     * @param channelNo Channelが内部的に持つ番号
     */
    void setChannelNo(int channelNo) {
        checkActive();
        this.channelNo = channelNo;
    }

    public int getChannelNo() {
        return channelNo;
    }

    public E getLocal() {
        return mother.getEndpoint();
    }

    public ObjectId getLocalObjectId() {
        return localObjectId;
    }
    
    public E getRemote() {
        return remote;
    }
    
    public ObjectId getRemoteObjectId() {
        return remoteObjectId;
    }
    
    public boolean isDuplex() {
        return isDuplex;
    }
    
    public boolean isCreatorSide() {
        return creator == null;
    }
    
    /**
     * rcvQueueに受信メッセージをputする。
     * このメソッドは、motherとなるTransportの受信部分からディスパッチされて呼び出される。
     * 
     * @param msg 受信メッセージ
     */
    protected void putReceiveQueue(Object msg) {
        try {
            if (msg == null) {
                rcvQueue.put(NULL_MESSAGE);
            } else {
                rcvQueue.put(msg);
            }
        } catch (InterruptedException ignore) {
        }
    }

    public Object receive() {
        try {
            return receive(0);
        } catch (NetworkTimeoutException ignore) {
            return null;
        }
    }

    public Object receive(int timeout) throws NetworkTimeoutException {
        try {
            Object msg = rcvQueue.poll(timeout, TimeUnit.MILLISECONDS);
            if (msg == END_OF_MESSAGE) return null;
            if (msg == NULL_MESSAGE) return null;
            if (msg == null) {
                throw new NetworkTimeoutException("ch.receive timed out");
            }
            return msg;
        } catch (InterruptedException e) {
            /*
             * interrupted状態はクリアされている。
             *  戻り先で確認できるように、再びinterrupted状態にする。
             */
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{channelNo=" + channelNo
                + ", creator=" + creator + ", local=" + getLocal()
                + ", localObjectId=" + localObjectId + ", remote=" + remote
                + ", remoteObjectId=" + remoteObjectId + "}";
    }
}
