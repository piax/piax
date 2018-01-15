/*
 * Register.java -  A wrapper for Maps.
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
 * $Id: Register.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 */
@SuppressWarnings("serial")
public class Register<K, V> implements Serializable {
    private static final int DEFAULT_INIT_ENTRY_NUM = 2;
    private static int initEntNum = DEFAULT_INIT_ENTRY_NUM;
    
    public static void setInitEntNum(int initNum) {
        initEntNum = initNum;
    }
    
    /*
     * as short use of memory, use List<V> instead of Set<V>
     */
    private final Map<K, List<V>> regMap;
    
    /*
     * TODO if have time, implement this as subclass of AbstractMap
     * and also the iterator
     */
    
    public Register() {
        regMap = new HashMap<K, List<V>>();
    }
    
    public void clear() {
        regMap.clear();
    }
    
    public Set<K> keySet() {
        return regMap.keySet();
    }
    
    public boolean containsPair(K key, V obj) {
        if (!regMap.containsKey(key)) {
            return false;
        }
        List<V> objs = regMap.get(key);
        return objs.contains(obj);
    }
    
    public boolean containsKey(K key) {
        return regMap.containsKey(key);
    }
    
    public int numKeys() {
        return regMap.size();
    }
    
    public int numValues(K key) {
        List<V> vals = getValues(key);
        return (vals == null ? 0 : vals.size());
    }
    
    public boolean add(K key, V obj) {
        List<V> objs = regMap.get(key);
        if (objs == null) {
            objs = new ArrayList<V>(initEntNum);
            objs.add(obj);
            regMap.put(key, objs);
            return true;
        } else {
            if (objs.contains(obj)) {
                // if already obj was registered
                return false;
            }
            return objs.add(obj);
        }
    }
    
    public boolean remove(K key, V obj) {
        List<V> objs = regMap.get(key);
        if (objs == null) {
            return false;
        }
    
        if (!objs.remove(obj)) {
            return false;
        }
        if (objs.size() == 0) {
            regMap.remove(key);
        }
        return true;
    }
    
    public List<V> remove(K key) {
        return regMap.remove(key);
    }
    
    public boolean testChange(K oldKey, K newKey, V obj) {
        return !oldKey.equals(newKey)
            && containsPair(oldKey, obj)
            && !containsPair(newKey, obj);
    }
    
    public boolean change(K oldKey, K newKey, V obj) {
        if (!testChange(oldKey, newKey, obj)) {
            return false;
        }
        remove(oldKey, obj);
        add(newKey, obj);
        return true;
    }
    
    /*
     * as short use of mem, returns List<V>
     */
    public List<V> getValues(K key) {
        List<V> vals = regMap.get(key);
        if (vals == null) vals = Collections.emptyList();
        return vals;
    }
    
    @Override
    public String toString() {
        return regMap.toString();
    }
}
