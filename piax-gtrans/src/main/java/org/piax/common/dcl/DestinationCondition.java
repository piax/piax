/*
 * DestinationCondition.java - A destination specified by conditions
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DestinationCondition.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import java.util.ArrayList;
import java.util.List;

import org.piax.common.Destination;
import org.piax.common.dcl.parser.ParseException;

/**
 * A destination specified by conditions
 */
public class DestinationCondition implements Destination {
    private static final long serialVersionUID = 1L;

    final List<VarDestinationPair> ands;
    
    DestinationCondition(List<VarDestinationPair> ands) {
        this.ands = ands;
    }
    
    public synchronized Destination getPredicate() throws ParseException {
        if (ands.size() != 1) {
            throw new ParseException("destination should be single predicate");
        }
        VarDestinationPair pair = getFirst();
        if (!"_".equals(pair.var)) {
            throw new ParseException("var name should be \"_\"");
        }
        return pair.destination;
    }

    public synchronized VarDestinationPair getFirst() {
        if (ands.size() == 0) return null;
        return ands.get(0);
    }
    
    public synchronized List<VarDestinationPair> getSeconds() {
        if (ands.size() == 0) return null;
        return new ArrayList<VarDestinationPair>(ands.subList(1, ands.size()));
    }

    @Override
    public String toString() {
        return ands.toString();
    }
}
