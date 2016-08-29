/*
 * RPCHook.java - A hook of RPC
 * 
 * Copyright (c) 2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id$
 */
package org.piax.gtrans;

import org.piax.common.ObjectId;

/**
 * A hook of RPC
 */
public abstract class RPCHook {
    public static RPCHook hook = null;

    public enum CallType {
        SYNC, ONEWAY, DC_SYNC, DC_ASYNC, DC_ONEWAY
    }

    public class RValue {
        public String method;
        public Object[] args;

        public RValue(String m, Object[] a) {
            method = m;
            args = a;
        }
    }

    public static final String HOOK_METHOD = "!";

    public abstract RValue callerHook(CallType type,
            ObjectId targetId,
            String target, String method, Object[] args);

    public abstract RValue calleeHook(String method, Object[] args);
}
