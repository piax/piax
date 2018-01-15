/*
 * ServiceId.java - A class that corresponds to an identifier of a service.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: ServiceId.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;


/**
 * A class that corresponds to an identifier of a service.
 */
public class ServiceId extends ObjectId {
    private static final long serialVersionUID = 1L;

    public ServiceId(byte[] bytes) {
        super(bytes);
    }

    public ServiceId(String str) {
        super(str);
    }
}
