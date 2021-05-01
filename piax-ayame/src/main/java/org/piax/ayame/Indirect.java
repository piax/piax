/*
 * Indirect.java - A class to define a local variable
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

/**
 * a class to define a local variable that is modified in lambda expressions.
 *  
 * @param <U> type of the local variable
 */
public class Indirect<U> {
    public U val;
    public Indirect(U val) {
        this.val = val;
    }
    public Indirect() {
        // empty
    }
    @Override
    public String toString() {
        return val == null ? "null" : val.toString();
    }
}
