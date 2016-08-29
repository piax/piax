/*
 * GTransConfigValues.java - configuration values for GTRANS
 * 
 * Copyright (c) 2015 National Institute of Information and 
 * Communications Technology
 * Copyright (c) 2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: GTransConfigValues.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

public class GTransConfigValues {
    public static final byte VERSION_NO = 30; 
    public static final short MSG_MAGIC = 12367 + VERSION_NO;
    public static final String REPO_PATH = ".piax";
    
//    public static long relaySwapInterval = 1 * 60 * 60 * 1000; // 1 hour

    // separator
    public static String ID_PATH_SEPARATOR = ":";
    
    // timeout
    public static int newChannelTimeout = 3000;
    public static int rpcTimeout = 10 * 1000;
//    public static int callMultiTimeout = 120 * 1000;
    public static int futureQueueGetNextTimeout = 15 * 1000;

    /**
     * スレッドプールの終了時にアクティブなスレッドの終了を待機する最長時間
     */
//    public static long MAX_WAIT_TIME_FOR_TERMINATION = 100L;

    // ReturnSet capacity
//    public static int defaultReturnSetCapacity = 30;
  
    public static int MAX_RECEIVER_THREAD_SIZE = 5000;
    
    /** ピアが一時的に保持できるChannel数の上限 */
    public static int MAX_CHANNELS = 100000;

    /**
     * MTU
     */
    public static int MAX_PACKET_SIZE = 1400;

    /**
     * socketのsend/receive時に用いる内部バッファのサイズ。
     * デフォルトでは128/256KB。TCP/UDP共通に用いる。
     */
    public static int SOCKET_SEND_BUF_SIZE = 128*1024;
    public static int SOCKET_RECV_BUF_SIZE = 256*1024;
    
    /**
     * send/replyで送受信可能なメッセージ長の上限。
     * UdpTransportServiceで行っているフラグメント数の最大値より、
     * メッセージ長の最大は、47MBまで設定可能であるが、
     * 大きなサイズのメッセージを分割しないで送受信することは、
     * メッセージのロスと遅延の点でよくない。
     * ここでは、現実的な上限値として1MBとしておく。
     * （実際のメッセージは、100KB以下にしておくべきである）
     * <p>
     * MTU(1450) * 32767 = 47,512,150 
     */
    public static int MAX_MSG_SIZE = 10 * 1000 * 1000 + 500;
    
    public static boolean ALLOW_REF_SEND_IN_BASE_TRANSPORT = false;
    
    /**
     * TcpTransport内のread用のbyte列のバッファサイズ。
     */
    public static int TCP_READ_BUF_LEN = 256 * 1024;
    
    /*
     * enable/disableの通知機能を持つInetTransportの設定値
     */
    public static boolean USE_INET_MON = false;
    public static long INET_CHECK_INTERVAL = 500;
    
    /*
     * HandoverTransportの設定値
     */
    public static boolean HT_DEFAULT_PIGGYBACK_MODE = true;
    public static int HT_BROADCAST_LOCATOR_INTERVAL = 5 * 60 * 1000;
}
