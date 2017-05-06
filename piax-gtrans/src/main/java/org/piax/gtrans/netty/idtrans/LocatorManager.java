package org.piax.gtrans.netty.idtrans;

import java.util.HashMap;

public class LocatorManager {
    // a map of
    // primarykey hash code -> primarykey with locator info
    HashMap<PrimaryKey, PrimaryKey> map;
    
    public LocatorManager() {
        map = new HashMap<>();
    }

    public void register(PrimaryKey key) {
        synchronized(map) {
            if (map.get(key) == null) {
                map.put(key, key);
            }
        }
    }

    public PrimaryKey get(PrimaryKey key) {
        synchronized(map) {
            PrimaryKey ret = map.get(key);
            if (ret == null) {
                return key;
            }
            return ret;
        }
    }
    /*
     * 
     */
    public PrimaryKey registerAndGet(PrimaryKey key) {
        PrimaryKey got = null;
        synchronized(map) {
            got = map.get(key);
            if (got == null) {
                map.put(key, key);
                got = key;
            } else {
                if (key.getLocatorVersion() > got.getLocatorVersion()) {
                    got.setNeighbors(key.getNeighbors());
                    map.put(key, got);
                }
            }
        }
        return got;
    }

    public int size() {
        return map.size();
    }
    
    public void fin() {
        map.clear();
    }
    
    @Override
    public String toString() {
        String ret = "";
        for (PrimaryKey p : map.values()) {
            ret += p + ":" + p.getNeighbors() + "\n";
        }
        return ret;
    }
}
