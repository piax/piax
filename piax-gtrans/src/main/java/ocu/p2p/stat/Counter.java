package ocu.p2p.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Counter {
    private Map<String, Integer> map = new HashMap<>();
    
    public void add(String name, int delta) {
        int v =  map.computeIfAbsent(name, s -> 0);
        map.put(name, v + delta);
    }
    
    public Integer get(String name) {
        return map.get(name);
    }

    public Set<String> keys() {
        return map.keySet();
    }

    public Set<Map.Entry<String, Integer>> entrySet() {
        return map.entrySet();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
