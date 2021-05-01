/*
 * PeerId.java - An identifier of a peer.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: PeerId.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

/*
 * A immutable class that represents the identifier of a peer.
 */
public class PeerId extends Id implements Endpoint, ComparableKey<Id> {
    private static final long serialVersionUID = 1L;
    public static final int DEFAULT_BYTE_LENGTH = 16;

    protected final int type; // 0 .. minus infinity, 1 plus infinity, 2 normal;

    public static final PeerId MINUS_INFINITY = SpecialId.newSpecialId(0);
    public static final PeerId PLUS_INFINITY = SpecialId.newSpecialId(1);
    private static final String MINUS_INFINITY_STRING = "-infinity";
    private static final String PLUS_INFINITY_STRING = "+infinity";

    boolean isPlusInfinity() {
        return type == 1;
    }
    boolean isMinusInfinity() {
        return type == 0;
    }

    public static final class SpecialId extends PeerId {
        private static final long serialVersionUID = 1L;
        private SpecialId(int b, int type) {
           super(new byte[]{(byte) b}, type);
        }
        public static SpecialId newSpecialId(int type) {
           SpecialId id = new SpecialId(0, type);
           return id;
        }
    }

    public static PeerId newId() {
        return new PeerId(newRandomBytes(DEFAULT_BYTE_LENGTH));
    }

    public PeerId(Id id) {
        super(id.getBytes());
        type = 2;
    }

    public PeerId(byte[] bytes, int type) {
        super(bytes);
        this.type = type;
    }

    public PeerId(byte[] bytes) {
        super(bytes);
        this.type = 2;
    }

    public PeerId(String str) {
        super(str);
        type = 2;
    }

    @Override
    public boolean equals(Object id) {
        if (isPlusInfinity() && !((id instanceof PeerId) && ((PeerId)id).isPlusInfinity())) {
            return false;
        }
        if (isMinusInfinity() && !((id instanceof PeerId) && ((PeerId)id).isMinusInfinity())) {
            return false;
        }
        return super.equals(id);
    }

    @Override
    public int compareTo(Id id) {
        if (this.equals(id)) {
            return 0;
        }
        if (isPlusInfinity() || (id instanceof PeerId) && ((PeerId)id).isMinusInfinity()) {
            return +1;
        }
        if ((id instanceof PeerId) && ((PeerId)id).isPlusInfinity() || isMinusInfinity()) {
            return -1;
        }
        return super.compareTo(id);
    }

    @Override
    public String toString() {
        if (isMinusInfinity()) {
            return MINUS_INFINITY_STRING;
        }
        if (isPlusInfinity()) {
            return PLUS_INFINITY_STRING;
        }
        return super.toString();
    }
}
