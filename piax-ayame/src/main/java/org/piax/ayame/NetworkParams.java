/*
 * NetworkParams.java - Parameters 
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

public class NetworkParams {
    /** default ack timeout */
    public final static long ACK_TIMEOUT = 4 * 1000;

    /** default reply timeout */
    public final static long REPLY_TIMEOUT = 8 * 1000;

    /** max wait time before sending ACK message */
    public final static long SEND_ACK_TIME = 2 * 1000;

    static {
        assert SEND_ACK_TIME < ACK_TIMEOUT;
        assert ACK_TIMEOUT < REPLY_TIMEOUT;
    }

    /**
     * 片方向ネットワーク遅延時間のベース値．
     * 実際の片方向遅延はこの値の2倍（star topologyを想定）．
     * 実ネットワーク上での片方向遅延が 20msec を仮定
     */
    public static long HALFWAY_DELAY = 20 / 2;
    public static long ONEWAY_DELAY = HALFWAY_DELAY * 2;
}
