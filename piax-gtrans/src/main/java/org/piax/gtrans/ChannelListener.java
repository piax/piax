/*
 * ChannelListener.java - The listener interface of channel
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: ChannelListener.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans;

import org.piax.common.Endpoint;

/**
 * ChannelListenerの持つメソッドを定義する。
 */
public interface ChannelListener<E extends Endpoint> {
    
    /**
     * Channelが通信相手によって、生成された際に呼び出されるメソッド。
     * 通信相手によるChannelの生成を受理する場合はtrue、拒否する場合はfalseを返す。
     * 
     * @param channel Channelオブジェクト
     * @return 通信相手によるChannelの生成を受理する場合はtrue、拒否する場合はfalse
     */
    boolean onAccepting(Channel<E> channel);

    /**
     * Channelが通信相手によって、closeされた際に呼び出されるメソッド。
     * この時点で、内部処理によって、channelがcloseされているので、close()を呼び出す必要はない。
     * 
     * @param channel Channelオブジェクト
     */
    void onClosed(Channel<E> channel);
    
    /**
     * Channelが予期せぬ例外によって、切断された際に呼び出されるメソッド。
     * 
     * @param channel Channelオブジェクト
     * @param cause Channelが切断された原因となる例外
     */
    void onFailure(Channel<E> channel, Exception cause);

    /**
     * Channelがメッセージを受信した際に呼び出されるメソッド。
     * 受信メッセージは、channelのreceiveメソッドを呼び出すことにより取得される。
     * 
     * @param channel Channelオブジェクト
     */
    void onReceive(Channel<E> channel);
}
