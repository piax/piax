/*
 * RemoteCallable.java - Annotations for the remote call methods.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RemoteCallable.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotations for the remote call methods
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RemoteCallable {
    public enum Type {
        SYNC,
        ONEWAY
    }

    Type value() default Type.SYNC;
}
