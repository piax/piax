package org.piax.ayame.sim.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.piax.ayame.Counter;

public class MultiStatSet {
    Map<String, StatSet> stats = new ConcurrentSkipListMap<>();

    public StatSet get(String key) {
        return stats.computeIfAbsent(key, k -> new StatSet());
    }
    
    public void addCounter(int index, Counter c) {
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
