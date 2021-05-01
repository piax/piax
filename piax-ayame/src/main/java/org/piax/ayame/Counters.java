/*
 * Counters.java - A class for counters 
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 

package org.piax.ayame;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Counters {
    private Map<String, Integer> map = new HashMap<>();
    
    public void add(String name, int delta) {
        int v =  map.computeIfAbsent(name, s -> 0);
        map.put(name, v + delta);
    }

    public Integer get(String name) {
        Integer rc = map.get(name);
        if (rc == null) {
            return 0;
        }
        return rc;
    }

    public Set<String> keys() {
        return map.keySet();
    }

    public Set<Map.Entry<String, Integer>> entrySet() {
        return map.entrySet();
    }

    public void clear() {
        map.clear();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
