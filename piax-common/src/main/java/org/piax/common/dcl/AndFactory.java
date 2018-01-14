/*
 * AndFactory.java -  An AND factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: AndFactory.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import java.util.ArrayList;
import java.util.List;

/**
 * A DstCond of ANDs
 */
public class AndFactory implements DCLFactory {
    final List<VarDestinationPair> ands = new ArrayList<VarDestinationPair>();
    
    @Override
    public void add(Object element) {
        ands.add((VarDestinationPair) element);
    }

    @Override
    public Object getDstCond() {
        return ands;
    }
}
