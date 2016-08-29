/*
 * ChordSharpIf.java - A RPC interface of multi-key Chord##.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id$
 */
package org.piax.gtrans.ov.szk;

import org.piax.gtrans.RPCException;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.NoSuchKeyException;
import org.piax.gtrans.ov.ring.RingIf;
import org.piax.gtrans.ov.szk.ChordSharpVNode.FTEntrySet;

/**
 * multi-key Chord# RPC interface
 */
public interface ChordSharpIf extends RingIf {
    @RemoteCallable
    FTEntrySet getFingers(DdllKey key, int x, int y, int k, FTEntrySet given)
            throws RPCException, NoSuchKeyException;

    @RemoteCallable
    FTEntry[][] getFingerTable(DdllKey key) throws RPCException,
            NoSuchKeyException;
}
