/*
 * Discoverable.java - An interface for objects on discoveries
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Discoverable.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans;

import java.util.List;

import org.piax.common.Endpoint;

/**
 * An interface for objects on discoveries
 */
public interface Discoverable<E extends Endpoint> {
    void fin();
    void register(PeerInfo<E> info);
    void unregister(PeerInfo<E> info);
    boolean addDiscoveryListener(DiscoveryListener<E> listener);
    boolean removeDiscoveryListener(DiscoveryListener<E> listener);
    List<PeerInfo<E>> getAvailablePeerInfos();
    void setExpireTime(long period);

    void scheduleDiscovery(long delay, long period);
    void cancelDiscovery();
}
