/*
 * Endpoint.java - A class that corresponds to an endpoint.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Endpoint.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;

/**
 * A class that corresponds to an endpoint.
 */
public interface Endpoint extends Key {
    public static Endpoint newEndpoint(String spec) throws ProtocolUnsupportedException {
        String specs[] = spec.split(":");
        if (specs[0].equals("-tcp")) {
            return new TcpLocator(spec);
        }
        else if (specs[0].equals("-udp")) {
            return new UdpLocator(spec);
        }
        else {
            return NettyEndpoint.newEndpoint(spec);
        }
    }
    public static void main(String args[]) throws Exception {
        Endpoint ep = Endpoint.newEndpoint("id:-1.0:tcp:localhost:12367");
        System.out.println(ep);
    }
}
