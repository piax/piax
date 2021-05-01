/*
 * MultiStatSet.java - A Set of multiple stats
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame.sim.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.piax.ayame.Counters;

public class MultiStatSet {
    Map<String, StatSet> stats = new ConcurrentSkipListMap<>();

    public StatSet get(String key) {
        return stats.computeIfAbsent(key, k -> new StatSet());
    }
    
    public void addCounters(int index, Counters c) {
        for (Map.Entry<String, Integer> ent: c.entrySet()) {
            StatSet set = get(ent.getKey());
            set.getStat(index).addSample(ent.getValue());
        }
    }

    public void printBasicStatAll() {
        for (Map.Entry<String, StatSet> ent: stats.entrySet()) {
            String key  = ent.getKey();
            StatSet set = ent.getValue();
            set.printBasicStat(key);
        }
    }
}
