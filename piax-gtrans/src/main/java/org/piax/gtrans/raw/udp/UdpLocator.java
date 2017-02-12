/*
 * UdpLocator.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: UdpLocator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.raw.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.piax.common.PeerId;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.RawTransport;

/**
 * UDPのためのPeerLocatorを表現するクラス。
 * 
 * 
 */
public class UdpLocator extends InetLocator {
    private static final long serialVersionUID = 1L;

    public UdpLocator(InetSocketAddress addr) {
        super(addr);
    }
    
    public UdpLocator(String spec) {
        super();
        String specs[] = spec.split(":");
        addr = new InetSocketAddress(specs[1], Integer.parseInt(specs[2]));
    }

    @Override
    public RawTransport<UdpLocator> newRawTransport(PeerId peerId)
            throws IOException {
        return new UdpTransport(peerId, this);
    }

    @Override
    public void serialize(ByteBuffer bb) {
        // XXX No need to implement because it is embedded.
    }
}
