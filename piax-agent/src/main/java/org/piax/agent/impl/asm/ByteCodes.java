/*
 * ByteCodes.java - A Container of class bytecodes.
 * 
 * Copyright (c) 2009-2015 PIAX development team
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
 */
/*
 * Revision History:
 * ---
 * 
 * $Id: ByteCodes.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.agent.impl.asm;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.piax.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Container of class bytecodes.
 */
public class ByteCodes implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(ByteCodes.class);
    
    /*--- nested classes ---*/
    
    private static class Entry implements Serializable {
        private static final long serialVersionUID = 1L;
        byte[] digest;
        byte[] code;
        
        Entry(byte[] code) {
            this.code = code;
            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("SHA");
            } catch (NoSuchAlgorithmException e) {
                logger.error("SHA is not supported in this JVM");
                System.exit(-1);
            }
            digest = md.digest(code);
        }
        
        @Override
        public String toString() {
            return ByteUtil.bytes2Hex(digest) + "(randSize: " + code.length + ")"; 
        }
    }
    
    /*--- instance fields ---*/

    private final Map<String, Entry> _codes;
    
    /*--- constructors ---*/

    public ByteCodes() {
        _codes = new HashMap<String, Entry>();
    }
    
    /*--- instance methods ---*/

    public synchronized void put(String className, byte[] code) {
        if (className == null) {
            throw new IllegalArgumentException("Class name is null");
        }
        if (code == null) {
            throw new IllegalArgumentException("Class code is null");
        }
        _codes.put(className, new Entry(code));
    }
    
    private boolean acceptable0(Set<Map.Entry<String, Entry>> entries) {
        for (Map.Entry<String, Entry> entry : entries) {
            byte[] thisDigest = getCodeDigest(entry.getKey());
            if (thisDigest != null) {
                // same entry!
                if (!MessageDigest.isEqual(thisDigest, entry.getValue().digest)) {
                    // conflict
                    return false;
                }
            }
        }
        return true;
    }
    
    public synchronized boolean addCodesIfAcceptable(ByteCodes codes) {
        if (codes == null) {
            throw new IllegalArgumentException("ByteCodes is null");
        }
        Set<Map.Entry<String, Entry>> entries = codes._codes.entrySet();
        if (!acceptable0(entries)) {
            return false;
        }
        this._codes.putAll(codes._codes);
        return true;
    }
    
    public synchronized boolean acceptable(ByteCodes codes) {
        if (codes == null) {
            throw new IllegalArgumentException("ByteCodes is null");
        }
        Set<Map.Entry<String, Entry>> entries = codes._codes.entrySet();
        return acceptable0(entries);
    }
    
    public synchronized boolean contains(String className) {
        if (className == null) {
            throw new IllegalArgumentException("Class name is null");
        }
        return _codes.containsKey(className);
    }

    public synchronized byte[] get(String className) {
        if (className == null) {
            throw new IllegalArgumentException("Class name is null");
        }
        Entry ent = _codes.get(className);
        return (ent == null) ? null : ent.code;
    }

    public synchronized byte[] getCodeDigest(String className) {
        if (className == null) {
            throw new IllegalArgumentException("Class name is null");
        }
        Entry ent = _codes.get(className);
        return (ent == null) ? null : ent.digest;
    }
    
    public synchronized Set<String> getClassNames() {
        return _codes.keySet();
    }
    
    @Override
    public synchronized Object clone() {
        ByteCodes newOne = new ByteCodes();
        newOne._codes.putAll(this._codes);
        return newOne;
    }
    
    public synchronized int size() {
        return _codes.size();
    }
    
    @Override
    public String toString() {
        Map<String, Entry> sortedCodes = new TreeMap<String, Entry>();
        sortedCodes.putAll(_codes);
        Set<Map.Entry<String, Entry>> ents = sortedCodes.entrySet();
        
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Entry> ent : ents) {
            sb.append("\n" + ent.getKey() + " \t" + ent.getValue().toString());
        }
        return sb.toString();
    }
}
