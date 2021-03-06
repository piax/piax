/*
 * DCLFactory.java - The DCL factory interface
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DCLFactory.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.dcl;

/**
 * The DCL interface
 */
public interface DCLFactory {
    void add(Object element);
    Object getDstCond();
}
