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

import org.piax.gtrans.UnavailableEndpointError;

/**
 * A class that corresponds to an endpoint.
 */
public interface Endpoint extends Key {
    public static Endpoint newEndpoint(String spec) {
        if (spec == null) {
            throw new UnavailableEndpointError("The endpoint is not specified.");
        }
        Endpoint ret = EndpointParser.parse(spec);
        if (ret == null) {
            throw new UnavailableEndpointError(EndpointParser.getSpec(spec) + " is not supported.");
        }
        return ret;
    }
    
    default public Endpoint newSameTypeEndpoint(String spec) {
        if (spec == null) {
            throw new UnavailableEndpointError("The endpoint is not specified.");
        }
        return newEndpoint(spec);
    }
}
