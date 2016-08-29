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

package org.piax.gtrans.raw;

import org.piax.common.PeerLocator;

/**
 * PeerLocatorの状態通知を受理するために定義されるインタフェース。
 * <p>
 * <code>受理したPeerLocatorの変更は、適切なタイミングで、
 * <code>HandoverTransport</code>オブジェクトに渡す必要がある。
 * 
 * 
 */
public interface LocatorStatusObserver {
    void onEnabled(PeerLocator loc, boolean isNew);
    void onFadeout(PeerLocator loc, boolean isFin);
    
    /**
     * PeerLocatorの変更通知を受理する。
     * oldLocからnewLocに変化があったことを受理するためのメソッド。
     */
    void onChanging(PeerLocator oldLoc, PeerLocator newLoc);
    void onHangup(PeerLocator loc, Exception cause);
}
