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

/**
 * A class that corresponds to an endpoint.
 */
public interface Endpoint extends Key {
    public static Endpoint newEndpoint(String spec) throws ProtocolUnsupportedException {
        if (spec == null) {
            throw new ProtocolUnsupportedException("The endpoint is not specified.");
        }
        Endpoint ret = EndpointParser.parse(spec);
        if (ret == null) {
            throw new ProtocolUnsupportedException(EndpointParser.getSpec(spec) + " is not supported.");
        }
        return ret;
    }
    
    default public Endpoint newSameTypeEndpoint(String spec) throws ProtocolUnsupportedException {
        if (spec == null) {
            throw new ProtocolUnsupportedException("The endpoint is not specified.");
        }
        return newEndpoint(spec);
    }

    public static void main(String args[]) throws Exception {
        Endpoint ep = Endpoint.newEndpoint("id:-1.0:tcp:localhost:12367");
        System.out.println(ep + " "+ ep.getClass());
        ep = Endpoint.newEndpoint("-tcp:localhost:12367");
        System.out.println(ep + " "+ ep.getClass());
        ep = Endpoint.newEndpoint("tcp:localhost:12367");
        System.out.println(ep + " "+ ep.getClass());
        ep = Endpoint.newEndpoint("ssl:localhost:12367");
        System.out.println(ep + " "+ ep.getClass());
        ep = Endpoint.newEndpoint("udt:localhost:12367");
        System.out.println(ep + " "+ ep.getClass());
    }
}
