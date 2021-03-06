/*
 * ControlMessage.java - Control Message for Netty
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.netty;

import java.io.Serializable;

public class ControlMessage<E extends NettyEndpoint> implements Serializable {
    private static final long serialVersionUID = 4729231253864270776L;
    public final ControlType type;
    final E source;
    final E dest;
    final Object arg;
    
    public enum ControlType {
        ATTEMPT, ACK, NACK, // Locator
        UPDATE, INIT, WAIT,
        CLOSE // for upper layer
    }
    public ControlMessage(ControlType type, E source, E dest, Object arg) {
        this.type = type;
        this.source = source;
        this.dest = dest;
        this.arg = arg;
    }

    public ControlType getType() {
        return type;
    }

    public Object getArg() {
        return arg;
    }

    public E getSource() {
        return source;
    }
    
    public String toString() {
        return "[ControlMessage: " + type + "]";
    }
}
