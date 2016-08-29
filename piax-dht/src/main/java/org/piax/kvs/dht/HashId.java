/*
 * HashId.java - A hashed id for DHT.
 * 
 * Copyright (c) 2012-2015 PIAX development team
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
 * $Id: HashId.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.kvs.dht;

import org.piax.common.Id;
import org.piax.common.ObjectId;
import org.piax.common.wrapper.NamedKey;

/**
 * A hashed ID of DHT.
 */
public class HashId extends NamedKey<Id> {
    private static final long serialVersionUID = 1L;
    public static HashId newId(ObjectId name, int len) {
        Id id = Id.newId(len);
        return new HashId(name, id);
    }
    
    HashId(ObjectId name, Id id) {
        super(name, id);
        key.setHexString();
    }

    HashId(ObjectId name, byte[] bytes) {
        super(name, new Id(bytes));
        key.setHexString();
    }
}
