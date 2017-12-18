package ocu.p2p.stat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

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
            ent.getValue().outputFreqDist(title + "-" + key, step);
        });
    }

    public void printCSV(String title) {
        System.out.println("#begin#" + title);
        ArrayList<String> names = new ArrayList<>();
        ArrayList<ArrayList<Double>> lists = new ArrayList<>();
        statset.entrySet().stream().forEach(ent -> {
            names.add(Integer.toString(ent.getKey()));
            lists.add(ent.getValue().list);
        });
        // header
        System.out.println(String.join(",", names));
        // data
        int max = lists.stream()
                .map(list -> list.size())
                .max(Comparator.naturalOrder())
                .orElse(0);
        for (int i = 0; i < max; i++) {
            int i0 = i;
            String out = lists.stream()
                    .map(list -> i0 < list.size()
                            ? Double.toString(list.get(i0)) : "")
                .collect(Collectors.joining(","));
            System.out.println(out);
        }
        System.out.println("#end");
    }
    
    public int lastKey() {
        return statset.lastKey();
    }
}
