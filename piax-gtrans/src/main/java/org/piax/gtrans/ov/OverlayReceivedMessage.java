/*
 * OverlayReceivedMessage.java - A received message class for overlays.
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

package org.piax.gtrans.ov;

import java.util.Collection;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.gtrans.ReceivedMessage;

/**
 * A received message class for overlays.
 */
public class OverlayReceivedMessage<K extends Destination> extends ReceivedMessage {
    
    final Collection<K> matchedKeys;

    public OverlayReceivedMessage(ObjectId sender, Endpoint src,
            Collection<K> matchedKeys, Object msg) {
        super(sender, src, msg);
        this.matchedKeys = matchedKeys;
    }
    
    public Collection<K> getMatchedKeys() {
        return matchedKeys;
    }
}
