/*
 * DiscoveryListener.java - A listener for discovery
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DiscoveryListener.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans;

import org.piax.common.Endpoint;

/**
 * A listener for discovery
 */
public interface DiscoveryListener<E extends Endpoint> {
    void onDiscovered(PeerInfo<E> peer, boolean isNew);
    void onFadeout(PeerInfo<E> peer);
}
