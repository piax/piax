/*
 * TransportListener.java - A listener for Transport
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: TransportListener.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

import org.piax.common.Destination;

/**
 * A listener interface for Transport.
 */
public interface TransportListener<D extends Destination> {
    
    /**
     * Transportオブジェクトがメッセージを受信した際に呼び出されるメソッド。
     * 受信メッセージは、送信元の情報などを付加したReceivedMessageオブジェクトの形で取得される。
     * 
     * @param trans Transportオブジェクト
     * @param rmsg ReceivedMessageオブジェクト
     */
    void onReceive(Transport<D> trans, ReceivedMessage rmsg);
}
