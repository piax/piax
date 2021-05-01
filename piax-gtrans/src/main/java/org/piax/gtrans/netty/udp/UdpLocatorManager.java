/*
 * UdpLocatorManager.java - manages UDP Locator 
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty.udp;

import org.piax.gtrans.netty.NettyLocator;

public interface UdpLocatorManager {
    public NettyLocator getPrimaryLocator(Comparable<?> key);
    public NettyLocator[] locatorCandidates(Comparable<?> key);
    public String dump();
}
