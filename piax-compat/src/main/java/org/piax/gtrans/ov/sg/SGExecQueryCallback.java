/*
 * SGExecQueryCallback.java - SGExecQueryCallback implementation of SkipGraph.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: SGExecQueryCallback.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.gtrans.ov.sg;

import org.piax.gtrans.RemoteValue;

/**
 * The interface of exec-query callback.
 */
public interface SGExecQueryCallback {
    RemoteValue<?> sgExecQuery(Comparable<?> key, Object query);
}
