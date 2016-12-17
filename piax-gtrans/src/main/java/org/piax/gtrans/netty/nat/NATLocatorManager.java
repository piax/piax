package org.piax.gtrans.netty.nat;

import java.util.HashMap;

import org.piax.common.Id;

public class NATLocatorManager {
    HashMap<Id,NettyNATLocator> map;
    
    public NATLocatorManager() {
        map = new HashMap<>();
    }

    public void register(NettyNATLocator locator) {
        if (map.get(locator.id) == null) {
            map.put(locator.id, locator);
        }
    }
    
    public NettyNATLocator get(NettyNATLocator locator) {
        NettyNATLocator ret = map.get(locator.id);
        if (ret == null) {
            return locator;
        }
        return ret;
    }
    /*
     * 
     */
    public NettyNATLocator getRegister(NettyNATLocator locator) {
        NettyNATLocator l = map.get(locator.id);
        if (l == null) {
            map.put(locator.id, locator);
            l = locator;
        }
        else {
            if (locator.getRawChannelLocators().size() > l.getRawChannelLocators().size()) {
                l.updateRawChannelLocators(locator);
            }
        }
        return l;
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
        for (NettyNATLocator l : map.values()) {
            ret += l + "\n";
        }
        return ret;
    }
}
