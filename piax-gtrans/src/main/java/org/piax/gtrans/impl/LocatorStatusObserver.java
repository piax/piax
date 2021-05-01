/*
 * LocatorStatusObserver.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: LocatorStatusObserver.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.impl;

import org.piax.common.Endpoint;
import org.piax.gtrans.PeerLocator;

/**
 * PeerLocatorの状態通知を受理するために定義されるインタフェース。
 * <p>
 * 受理したPeerLocatorの変更は、適切なタイミングで、
 * <code>HandoverTransport</code>オブジェクトに渡す必要がある。
 * </p>
 * 
 */
public interface LocatorStatusObserver {
    void onEnabled(Endpoint loc, boolean isNew);
    void onFadeout(Endpoint loc, boolean isFin);
    
    /**
     * PeerLocatorの変更通知を受理する。
     * oldLocからnewLocに変化があったことを受理するためのメソッド。
     * 
     * @param oldLoc the old locator
     * @param newLoc the new locator to be changed.
     */
    void onChanging(PeerLocator oldLoc, PeerLocator newLoc);
    void onHangup(PeerLocator loc, Exception cause);
}
