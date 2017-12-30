/*
 * UniqId.java - A unique identifier for peers.
 * 
 * Copyright (c) 2009-2015 PIAX develoment team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: UniqId.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.io.ObjectStreamException;

import org.piax.common.ComparableKey;
import org.piax.common.Endpoint;
import org.piax.common.Id;

public class UniqId extends Id implements Endpoint, ComparableKey<Id> {
    private static final long serialVersionUID = 1L;
    public static final int DEFAULT_BYTE_LENGTH = 16;

    int type = 2; // 0 .. minus infinity, 1 plus infinity, 2 normal;

    public static final UniqId MINUS_INFINITY = SpecialId.newSpecialId(0);
    public static final UniqId PLUS_INFINITY = SpecialId.newSpecialId(1);
    private static final String MINUS_INFINITY_STRING = "-infinity";
    private static final String PLUS_INFINITY_STRING = "+infinity";

    boolean isPlusInfinity() {
        return type == 1;
    }
    boolean isMinusInfinity() {
        return type == 0;
    }
    
    public static final class SpecialId extends UniqId {
        private static final long serialVersionUID = 1L;
        private SpecialId(int b) {
           super(new byte[]{(byte) b});
        }
        public static SpecialId newSpecialId(int type) {
           SpecialId id = new SpecialId(0);
           id.type = type;
           return id;
        }
    }

    public static UniqId newId() {
        return new UniqId(newRandomBytes(DEFAULT_BYTE_LENGTH));
    }

    public UniqId(Id id) {
        super(id.getBytes());
        type = 2;
    }

    public UniqId(byte[] bytes) {
        super(bytes);
        type = 2;
    }

    public UniqId(String str) {
        super(str);
        type = 2;
    }

    @Override
    public boolean equals(Object id) {
        if (isPlusInfinity() && !((id instanceof UniqId) && ((UniqId)id).isPlusInfinity())) {
            return false;
        }
        if (isMinusInfinity() && !((id instanceof UniqId) && ((UniqId)id).isMinusInfinity())) {
            return false;
        }
        return super.equals(id);
    }
    
    @Override
    public int compareTo(Id id) {
        if (this.equals(id)) {
            return 0;
        }
        if (isPlusInfinity() || (id instanceof UniqId) && ((UniqId)id).isMinusInfinity()) {
            return +1;
        }
        if ((id instanceof UniqId) && ((UniqId)id).isPlusInfinity() || isMinusInfinity()) {
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
