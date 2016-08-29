/*
 * MessageId.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: MessageId.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.io.Serializable;

import org.piax.common.PeerId;

/**
 * 発行されたメッセージのユニーク性を保証するためのID
 * 
 * 
 */
public class MessageId implements Serializable {
    private static final long serialVersionUID = 1L;

    // TODO こういうmsgNoもpeerを終了させた場合はsaveした方がよい
    private static long _msgNo = 0;
    public static synchronized MessageId newMessageId(PeerId origin) {
        if (origin == null)
            throw new IllegalArgumentException("origin PeerId should not be null");
        return new MessageId(origin, _msgNo++);
    }
    
    public final PeerId origin;
    public final long msgNo;

    private MessageId(PeerId origin, long msgNo) {
        this.origin = origin;
        this.msgNo = msgNo;
    }
    
    @Override
    public int hashCode() {
        return origin.hashCode() ^ (int) msgNo;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof MessageId))
            return false;
        MessageId _obj = (MessageId) obj;
        return origin.equals(_obj.origin)
                && msgNo == _obj.msgNo;
    }
    
    @Override
    public String toString() {
        return origin + "#" + msgNo;
    }
}
