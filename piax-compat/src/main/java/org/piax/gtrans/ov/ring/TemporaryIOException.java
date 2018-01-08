/*
 * TemporaryIOException.java - A kind of IOException where you can retry
 * some time later.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring;

import java.io.IOException;

/**
 * a kind of IOException where you can retry some time later.
 */
public class TemporaryIOException extends IOException {
    public TemporaryIOException(String message) {
        super(message);
    }
}
