/*
 * Channel.java - Channel interface for the generic transport.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;

/**
 * Channel interface for the generic transport.
 */
public interface Channel<E extends Endpoint> {
    
    /**
     * Channelをcloseする。
     * なお、closeメソッドは多重に呼ばれても問題は起こらない。
     */
    void close();
    
    boolean isClosed();

    /**
     * このChannelオブジェクトを所有するTransportオブジェクトのtransport IDを返す。
     * 
     * @return このChannelオブジェクトを所有するTransportオブジェクトのtransport ID
     */
    TransportId getTransportId();
    
    /**
     * このChannelが内部的に持つ番号を返す。
     * 
     * @return このChannelが内部的に持つ番号
     */
    int getChannelNo();
    
    /**
     * このChannelのローカル側のEndpointを返す。
     * 
     * @return ローカル側のEndpoint
     */
    E getLocal();
    
    /**
     * このChannelのローカル側に位置するエンティティのobject IDを返す。
     * 
     * @return ローカル側の端点に位置するエンティティのobject ID
     */
    ObjectId getLocalObjectId();

    /**
     * このChannelのリモート側のEndpointを返す。
     * 
     * @return リモート側のEndpoint
     */
    E getRemote();
    
    /**
     * このChannelのリモート側に位置するエンティティのobject IDを返す。
     * 
     * @return リモート側の端点に位置するエンティティのobject ID
     */
    ObjectId getRemoteObjectId();
    
    /**
     * このChannelが双方向通信可能かどうかを判定する。
     * 双方向通信可能な場合はtrueが返される。
     * 
     * @return このChannelが双方向通信可能な場合はtrue、それ以外はfalse
     */
    boolean isDuplex();
    
    /**
     * このChannelオブジェクトを持つTransportオブジェクトによってChannelが生成されたかどうかを判定する。
     * こちら側のTransportオブジェクトによってChannelが生成された場合はtrueが返される。
     * 
     * @return こちら側のTransportオブジェクトによってChannelが生成された場合はtrue、
     *          それ以外はfalse
     */
    boolean isCreatorSide();
    
    /**
     * Channelにmsgにより指定されたメッセージを送信する。
     * 
     * @param msg 送信メッセージ
     * @throws IOException I/Oエラーが発生した場合
     */
    void send(Object msg) throws IOException;
    
    /**
     * Channelから直ちにメッセージを受信する。
     * Channelがメッセージを受信していない場合は、nullが返される。
     * 
     * @return 受信メッセージ、受信していない場合はnull
     */
    Object receive();
    
    /**
     * Channelからメッセージを受信する。
     * Channelがメッセージを受信していない場合は、指定されたtimeoutの時間、受信するまでブロックする。
     * timeoutをすぎた場合はNetworkTimeoutExceptionがthrowされる。
     * 
     * @param timeout 受信のための待機時間（msec）
     * @return 受信メッセージ
     * @throws NetworkTimeoutException 待機時間が経過した場合
     */
    Object receive(int timeout) throws NetworkTimeoutException;
}
