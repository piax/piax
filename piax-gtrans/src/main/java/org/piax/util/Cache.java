/*
 * Cache.java - A class for cache handling
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
 */
/*
 * Revision History:
 * ---
 * 
 * $Id: Cache.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * 
 */
public class Cache<K, V> {
    
    static class LRUQueue<E> {
        
        static class Entry<E> {
            E element;
            Entry<E> next;
            Entry<E> prev;
    
            Entry(E element, Entry<E> next, Entry<E> prev) {
                this.element = element;
                this.next = next;
                this.prev = prev;
            }
        }

        Entry<E> header = new Entry<E>(null, null, null);
        int size = 0;
        
        LRUQueue() {
            header.next = header.prev = header;
        }

        private Entry<E> addBefore(E o, Entry<E> e) {
            Entry<E> newEntry = new Entry<E>(o, e, e.prev);
            newEntry.prev.next = newEntry;
            newEntry.next.prev = newEntry;
            size++;
            return newEntry;
        }

        E remove(Entry<E> e) {
            if (e == header)
                throw new NoSuchElementException();
    
            E result = e.element;
            e.prev.next = e.next;
            e.next.prev = e.prev;
            e.next = e.prev = null;
            e.element = null;
            size--;
            return result;
        }

        void reOffer(Entry<E> e) {
            if (e == header)
                throw new NoSuchElementException();
    
            // remove Entry
            e.prev.next = e.next;
            e.next.prev = e.prev;
            // add Entry
            e.next = header;
            e.prev = header.prev;
            e.prev.next = e;
            e.next.prev = e;
        }
        
        E peek() {
            if (size==0)
                return null;
    
            return header.next.element;
        }

        E poll() {
            if (size == 0)
                return null;
            return remove(header.next);
        }
        
        Entry<E> offer(E o) {
            return addBefore(o, header);
        }
        
        int size() {
            return size;
        }

        void clear() {
            Entry<E> e = header.next;
            while (e != header) {
                Entry<E> next = e.next;
                e.next = e.prev = null;
                e.element = null;
                e = next;
            }
            header.next = header.prev = header;
            size = 0;
        }
    }
    
    static class MapValue<S, E> {
        S refer;
        E queEntry;
        MapValue(S refer, E queEntry) {
            this.refer = refer;
            this.queEntry = queEntry;
        }
    }

    /** Maximum number of entries to cache */
    private final int maxNum;
    
    /** A map contains entries */
    private final Map<K, MapValue<SoftReference<V>, LRUQueue.Entry<K>>> map;
    
    /** A queue represents LRU */
    private final LRUQueue<K> queue;
    
    public Cache(int maxNum) {
        if (maxNum < 1) {
            throw new IllegalArgumentException("Negative max number of entries");
        }
        queue = new LRUQueue<K>();
        map = new HashMap<K, MapValue<SoftReference<V>, LRUQueue.Entry<K>>>();
        this.maxNum = maxNum;
    }

    public synchronized int size() {
        return map.size();
    }
    
    public synchronized V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException("key should not be null");
        }

        MapValue<SoftReference<V>, LRUQueue.Entry<K>> mval = map.get(key);
        if (mval == null) 
            return null;
        
        V value = mval.refer.get();
        if (value == null) {
            // SoftReference was invalidated, remove the entry
            map.remove(key);
            queue.remove(mval.queEntry);
            return null;
        }
        // re-offer
        queue.reOffer(mval.queEntry);
        return value;
    }
    
    public synchronized V put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("key and value should not be null");
        }

        V old = null;
        SoftReference<V> refer = new SoftReference<V>(value);
        MapValue<SoftReference<V>, LRUQueue.Entry<K>> mval = map.get(key);
        if (mval != null) {
            // found the old value exist
            old = mval.refer.get();
            mval.refer = refer;
            // re-offer
            queue.reOffer(mval.queEntry);
            return old;
        }

        if (map.size() >= maxNum) {
            // remove the most stale entry
            K stale = queue.poll();
            map.remove(stale);
        }
        LRUQueue.Entry<K> newEntry = queue.offer(key);
        map.put(key, new MapValue<SoftReference<V>, LRUQueue.Entry<K>>
                (refer, newEntry));
        return null;
    }
    
    public synchronized V remove(K key) {
        if (key == null) {
            throw new IllegalArgumentException("key should not be null");
        }

        MapValue<SoftReference<V>, LRUQueue.Entry<K>> mval = map.remove(key);
        if (mval == null) 
            return null;
        
        V value = mval.refer.get();
        queue.remove(mval.queEntry);
        return value;
    }
    
    public synchronized void clear() {
        queue.clear();
        map.clear();
    }
}
