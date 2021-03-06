/*
 * PredicateFactory.java - A predicate factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: PredicateFactory.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.dcl;

/**
 * A predicate factory class of DCL.
 */
public class PredicateFactory extends DestinationFactory {
    String var;
    
    PredicateFactory() {
    }

    @Override
    public void add(Object element) {
        if (var == null) {
            var = (String) element;
        }
        else {
            destination = convert(element);
        }
    }

    @Override
    public Object getDstCond() {
        return new VarDestinationPair(var, destination);
    }
}
