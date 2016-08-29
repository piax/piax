/*
 * VarDestinationPair.java - A var destination factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: VarDestinationPair.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import java.io.Serializable;

import org.piax.common.Destination;

/**
 * A var destination factory class of DCL.
 */
public class VarDestinationPair implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String var;
    public final Destination destination;

    public VarDestinationPair(String var, Destination destination) {
        this.var = var;
        this.destination = destination;
    }
    
    @Override
    public String toString() {
        return var + "/" + destination;
    }
}
