/*
 * FloodingNodeIf.java - A flooding node API.
 * 
 * Copyright (c) 2012-2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: FloodingNodeIf.java 1171 2015-05-18 14:07:32Z teranisi $
 */

package org.piax.gtrans.ov.flood;

import java.util.List;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.impl.NestedMessage;

/**
 * A flooding node API.
 */
public interface FloodingNodeIf<D extends Destination> extends RPCIf {
    
    @RemoteCallable
    List<RemoteValue<?>> request(List<Endpoint> visited, Destination dst,
            NestedMessage nmsg) throws RPCException;
}
