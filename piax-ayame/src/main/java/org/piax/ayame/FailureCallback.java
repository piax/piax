/*
 * FailureCallback.java - FailureCallback interface
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

@FunctionalInterface
public interface FailureCallback {
    void run(EventException e);
}
