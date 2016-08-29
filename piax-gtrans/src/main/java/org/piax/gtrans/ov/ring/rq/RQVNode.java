/*
 * RQVNode.java - A virtual node implementation of range query overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import org.piax.common.Endpoint;
import org.piax.gtrans.ov.ring.RingManager;
import org.piax.gtrans.ov.ring.RingVNode;

public class RQVNode<E extends Endpoint> extends RingVNode<E> {
    public final QueryStore store = new QueryStore();

    public RQVNode(RingManager<E> p2p, Comparable<?> rawkey) {
        super(p2p, rawkey);
   }
}
