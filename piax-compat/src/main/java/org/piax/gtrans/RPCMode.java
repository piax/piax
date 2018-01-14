/*
 * RPCMode.java - RPC modes.
 * 
 * Copyright (c) 2015 PIAX develoment team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RPCInvoker.java 1076 2014-07-11 03:27:43Z sho $
 */

package org.piax.gtrans;

/**
 * RPCのモードを示すEnumeration型 
 */
public enum RPCMode {
    /**
     * 呼び出し方は呼ばれる方のRemoteCallableアノテーションの
     * 引数により決定される。
     * 引数指定がSYNCならば同期呼び出し、ONEWAYならばoneway呼び出しとなる。
     */
    AUTO,
    /**
     * 同期呼び出し
     */
    SYNC,
    /**
     * 非同期呼び出し
     */
    ASYNC,
    /**
     * oneway呼び出し
     */
    ONEWAY
}
