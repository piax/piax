/*
 * StrictMap.java - A wrapper for a map.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class StrictMap<K, V> {
    @SuppressWarnings("rawtypes")
    private final Map map;

    @SuppressWarnings("rawtypes")
    public StrictMap(Map baseMap) {
        this.map = baseMap;
    }

    public void clear() {
        map.clear();
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    public boolean containsValue(V value) {
        return map.containsValue(value);
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    public V get(K key) {
        return (V) map.get(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public V put(K key, V value) {
        return (V) map.put(key, value);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    public V remove(K key) {
        return (V) map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public Collection<V> values() {
        return map.values();
    }
    
    public String toString() {
        return map.toString();
    }
}
