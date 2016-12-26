package ocu.p2p.stat;

import java.util.concurrent.ConcurrentSkipListMap;

public class StatSet {
    ConcurrentSkipListMap<Integer, Stat> statset =
            new ConcurrentSkipListMap<>();

    public Stat getStat(int key) {
        Stat s = statset.get(key);
        if (s == null) {
            s = new Stat();
            statset.put(key, s);
        }
        return s;
    }

    public void printBasicStat(String title) {
        System.out.println("#begin#" + title);
        Stat.printBasicStatHeader();
        statset.entrySet().stream().forEach(ent -> {
            ent.getValue().printBasicStatBody(ent.getKey());
        });
        System.out.println("#end");
    }

    public void outputFreqDist(String title, int step) {
        statset.entrySet().stream().forEach(ent -> {
            int key = ent.getKey();
            ent.getValue().outputFreqDist(title + "-" + key, step, false);
        });
    }
    
    public int lastKey() {
        return statset.lastKey();
    }
}
