/*
 * RemoteValue.java - A container of the remote value.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
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

import org.piax.common.Endpoint;

/**
 * A container of the remote value.
 */
public class RemoteValue<V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Endpoint peer;
    private V value;
    private Throwable exception = null;
    
    /**
     * value以外にセットしたいoptionalなオブジェクトのための変数。
     * equalsの比較対象には含まれない。
     */
    private Object option = null;
    transient int hash;
    
    public RemoteValue(Endpoint peer) {
        this.peer = peer;
    }
    
    public RemoteValue(Endpoint peer, V value) {
        this.peer = peer;
        this.value = value;
    }

    public RemoteValue(Endpoint peer, Throwable exception) {
        this.peer = peer;
        this.value = null;
        this.exception = exception;
    }
    
    public RemoteValue(Endpoint peer, V value, Throwable exception) {
        this.peer = peer;
        this.value = value;
        this.exception = exception;
    }

    public Endpoint getPeer() {
        return peer;
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

    public void setOption(Object option) {
        this.option = option;
    }

    public Object getOption() {
        return option;
    }
    
    private boolean equals(Object a, Object b) {
        return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || !(o instanceof RemoteValue)) return false;
        if (hashCode() != o.hashCode()) return false;
        @SuppressWarnings("unchecked")
        RemoteValue<V> r = (RemoteValue<V>) o;
        return equals(peer, r.peer) &&
                equals(value, r.value) &&
                equals(exception, r.exception);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = 31 * (peer == null ? 0 : peer.hashCode()) + 
                    (value == null ? 0 : value.hashCode()) +
                    (exception == null ? 0 : exception.hashCode());
        }
        return hash;
    }

    @Override
    public String toString() {
        String v = (exception == null) ? " val=" + value :
            " excep=" + exception;
        return "[peer=" + peer + v + "]";
    }
}
