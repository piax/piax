package org.piax.gtrans.netty.idtrans;

import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.PeerLocator;
import org.piax.gtrans.netty.NettyLocator;

public class LocatorManager {
    // a map of primarykey hash code -> primarykey with locator info
    ConcurrentHashMap<PrimaryKey, PrimaryKey> map;
    ConcurrentHashMap<PeerLocator, PrimaryKey> reverseMap;
    
    public LocatorManager() {
        map = new ConcurrentHashMap<>();
        reverseMap = new ConcurrentHashMap<>();
    }

    public PrimaryKey updateAndGet(PrimaryKey primaryKey) {
        PrimaryKey got, ret = primaryKey;
        if (primaryKey.getLocator() != null) {
            synchronized(reverseMap) {
                got = reverseMap.get(primaryKey.getLocator());
                if (got == null) {
                    reverseMap.put(primaryKey.getLocator(), primaryKey);
                }
                else {
                    if (!got.getLocator().equals(primaryKey.getLocator())) {
                        if (primaryKey.getLocatorVersion() > got.getLocatorVersion()) {
                            // replace the corresponding key.
                            reverseMap.remove(got.getLocator()); // existing entry
                            reverseMap.put(primaryKey.getLocator(), primaryKey);
                        }
                        else {
                            // already newest.
                            ret = got;
                        }
                    }
                }
            }
        }
        if (primaryKey.getRawKey() != null) {
            synchronized(map) {
                got = map.get(primaryKey);
                if (got == null) {
                    map.put(primaryKey, primaryKey);
                    got = primaryKey;
                    ret = got;
                } else {
                    if (primaryKey.getLocatorVersion() > got.getLocatorVersion()) {
                        got.setLocator(primaryKey.getLocator());
                        got.setNeighbors(primaryKey.getNeighbors());
                        map.put(primaryKey, got);
                    }
                    else {
                        ret = got;
                    }
                }
            }
        }
        return ret;
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

    public void updateKey(NettyLocator direct, PrimaryKey primaryKey) {
        reverseMap.put(direct, primaryKey);
        PrimaryKey got = map.get(primaryKey);
        if (primaryKey.getLocatorVersion() > got.getLocatorVersion()) {
            got.setLocator(direct);
            got.setNeighbors(primaryKey.getNeighbors());
        }
    }
    
    public PrimaryKey reverseGet(NettyLocator direct) {
        return reverseMap.get(direct);
    }
}
