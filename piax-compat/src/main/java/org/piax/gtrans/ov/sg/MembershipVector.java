/*
 * MembershipVector.java - MembershipVector implementation of SkipGraph.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.sg;

import org.piax.common.Id;

/**
 * a membership vector
 */
public class MembershipVector extends Id {
    private static final long serialVersionUID = 1L;
    public static int BYTE_LENGTH = 16;

    public MembershipVector() {
        super(newRandomBytes(BYTE_LENGTH));
    }
}
