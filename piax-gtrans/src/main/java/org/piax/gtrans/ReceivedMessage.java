/*
 * ReceivedMessage.java - A class of received message.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $ObjectId: ReceivedMessage.java 607 2012-10-31 13:35:46Z yos $
 */

package org.piax.gtrans;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;

/**
 * A class of received message.
 */
public class ReceivedMessage {
    final ObjectId sender;
    final Endpoint src;
    Object msg;

    /**
     * @param sender
     * @param src
     * @param msg
     */
    public ReceivedMessage(ObjectId sender, Endpoint src, Object msg) {
        this.sender = sender;
        this.src = src;
        this.msg = msg;
    }
    
    public ObjectId getSender() {
        return sender;
    }
    
    public Endpoint getSource() {
        return src;
    }

    public Object getMessage() {
        return msg;
    }
    
    public void setMessage(Object msg) {
        this.msg = msg;
    }
}
