/*
 * ObjectId.java - A class that corresponds to an identifier of a object.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: ObjectId.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;


/**
 * A class that corresponds to an identifier of a object.
 */
public class ObjectId extends Id implements ComparableKey<Id>{
    private static final long serialVersionUID = 1L;
    
    public ObjectId(byte[] bytes) {
        super(bytes);
    }

    public ObjectId(String str) {
        super(str);
    }
}
