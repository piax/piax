/*
 * ReturnValue.java - A container of return value.
 * 
 * Copyright (c) 2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RemoteValue.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * A container of return value.
 */
public class ReturnValue<V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private V value;
    private Throwable exception;
        
    public ReturnValue(V value) {
        this(value,null);
    }

    public ReturnValue(Throwable exception) {
        this(null,exception);
    }
    
    public ReturnValue(V value, Throwable exception) {
        this.value = value;
        this.exception = exception;
    }

    public V get() throws InvocationTargetException {
        if (exception != null) {
            throw new InvocationTargetException(exception);
        }
        return value;
    }

    public V getValue() {
        return value;
    }

    public Throwable getException() {
        return exception;
    }
    
    public void setException(Throwable t) {
        exception = t;
    }

    private boolean equals(Object a, Object b) {
        return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || !(o instanceof ReturnValue)) return false;
        @SuppressWarnings("unchecked")
        ReturnValue<V> r = (ReturnValue<V>) o;
        return  equals(value, r.value) &&
                equals(exception, r.exception);
    }

    @Override
    public int hashCode() {
        return  (value == null ? 0 : value.hashCode()) +
                (exception == null ? 0 : exception.hashCode());
    }

    @Override
    public String toString() {
        String v = (exception == null) ? " val=" + value :
            " excep=" + exception;
        return "[" + v + "]";
    }
}
