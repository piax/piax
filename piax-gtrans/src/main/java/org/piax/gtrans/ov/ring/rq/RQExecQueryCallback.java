/*
 * RQExecQueryCallback.java - RQExecQueryCallback implementation.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: SGExecQueryCallback.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import org.piax.gtrans.RemoteValue;

/**
 * The interface of exec-query callback.
 */
public interface RQExecQueryCallback {
    RemoteValue<?> rqExecQuery(Comparable<?> key, Object query);
}
