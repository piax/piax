package org.piax.gtrans.netty.udp.direct;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyLocator.TYPE;
import org.piax.gtrans.netty.udp.UdpLocatorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyLocatorManager implements UdpLocatorManager {
    // the key must be unique for each node.
    ConcurrentHashMap<Comparable<?>, KeyLocatorEntry> map;
    protected static final Logger logger = LoggerFactory.getLogger(KeyLocatorManager.class);
    public KeyLocatorManager() {
        map = new ConcurrentHashMap<>();
    }
    
    String inets2str(InetAddress[] inets) {
        String ret="";
        for (InetAddress inet : inets) {
            ret += "inet:" + (inet.getHostAddress() + " ");
        }
        return ret;
    }

    public void register(Comparable<?>key, InetAddress[] candidates, int port, boolean active) {
        KeyLocatorEntry kle = map.get(key);
        if (kle != null) {
            logger.debug("merging: {} : {} -> {}", active, key, inets2str(candidates));
            kle.merge(new KeyLocatorEntry(key, candidates, port, true), active);
        }
        else {
            logger.debug("putting: {} -> {} ", key, inets2str(candidates));
            map.put(key, new KeyLocatorEntry(key, candidates, port, active));
        }
    }

    public void register(Comparable<?>key, InetAddress[] candidates, int port) {
        register(key, candidates, port, false);
    }

    public void register(Comparable<?>key, NettyLocator[] candidates) {
        KeyLocatorEntry kle = map.get(key);
        if (kle != null) {
            logger.debug("merging: {} -> {}", key, candidates);
            kle.merge(new KeyLocatorEntry(key, candidates), false);
        }
        else {
            logger.debug("putting: {} -> {}", key, candidates);
            map.put(key, new KeyLocatorEntry(key, candidates));
        }
    }

    public void registerActive(Comparable<?>key, NettyLocator candidate) {
        InetAddress candidates[] = new InetAddress[1];
        try {
            candidates[0] = InetAddress.getByName(candidate.getHost());
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        register(key, candidates, candidate.getPort(), true);
    }

    public void registerActive(Comparable<?>key, InetSocketAddress candidate) {
        InetAddress candidates[] = new InetAddress[1];
        candidates[0] = candidate.getAddress();
        register(key, candidates, candidate.getPort(), true);
    }

    public void accessed(Comparable<?>key, NettyLocator loc) {
        KeyLocatorEntry kle = map.get(key);
        if (kle != null) {
            kle.accessed(loc);
        }
        kle.sort();
    }

    public LocatorEntry nextTrialCandidate(Comparable<?> key) {
        KeyLocatorEntry kle = map.get(key);
        if (kle != null) {
            return kle.nextTrialCandidate();
        }
        return null;
    }

    public List<LocatorEntry> candidates(Comparable<?> key) {
        KeyLocatorEntry kle = map.get(key);
        if (kle != null) {
            return kle.candidates();
        }
        return null;
    }
    
    @Override
    public NettyLocator[] locatorCandidates(Comparable<?> key) {
        KeyLocatorEntry kle = map.get(key);
        if (kle != null) {
            NettyLocator[] locs = new NettyLocator[kle.candidates().size()];
            for (int i = 0; i < kle.candidates().size(); i++) {
                locs[i] = kle.candidates().get(i).locator;
            }
            return locs;
        }
        return null;
    }

    static public class LocatorEntry {
        public NettyLocator locator;
        public long lastAccessed; // the last accessed time when locator is confirmed as alive.
        public long rtt;
        public Comparable<?> source;
        public String toString() {
            return locator + "/lastAccessed=" + lastAccessed + "/rtt=" + rtt + "/src=" + source;
        }
    }

    static public class KeyLocatorEntry {
        final public Comparable<?> key;
        final public List<LocatorEntry> entries;

        public KeyLocatorEntry(Comparable<?> key) {
            this.key = key;
            entries = new ArrayList<>();
        }

        public KeyLocatorEntry(Comparable<?> key, NettyLocator[] candidates) {
            this.key = key;
            entries = new ArrayList<>();
            for (NettyLocator loc: candidates) {
                entries.add(new LocatorEntry() {{
                    this.locator = loc;
                    this.lastAccessed = -1;
                    this.rtt = Long.MAX_VALUE;
                }});
            }
            /* in order of 
            Collections.sort(addrs, (x, y) -> {
                return ((Long)x.lastAccessed).compareTo(y.lastAccessed);
            }); */
        }
        
        LocatorEntry getPrimaryEntry() {
            return entries.get(0);
        }
        
        public NettyLocator[] getLocatorsArray() {
            NettyLocator[] ret = new NettyLocator[entries.size()];
            for (int i = 0; i < entries.size(); i++) {
                ret[i] = entries.get(i).locator;
            }
            return ret;
        }

        public KeyLocatorEntry(Comparable<?> key, InetAddress[] candidates, int port, boolean active) {
            this.key = key;
            entries = new ArrayList<>();
            for (InetAddress addr : candidates) {
                entries.add(new LocatorEntry() {{
                    this.locator = new NettyLocator(TYPE.UDP, addr, port);
                    this.lastAccessed = active ? System.currentTimeMillis() : -1;
                    this.rtt = Long.MAX_VALUE;
                }});
            }
            /* in order of 
            Collections.sort(addrs, (x, y) -> {
                return ((Long)x.lastAccessed).compareTo(y.lastAccessed);
            }); */
        }

        public void sort() {
            Collections.sort(entries, (x, y) -> {
                int cmp = 0;
                if ((cmp = ((Long)x.rtt).compareTo(y.rtt)) != 0) { // most small rtt
                    return cmp;
                }
                return ((Long)y.lastAccessed).compareTo(x.lastAccessed); // the older, the earlier retried.
            });
        }

        public LocatorEntry accessed(NettyLocator loc) {
            for (LocatorEntry addr : entries) {
                if (loc.equals(addr.locator)) {
                    addr.lastAccessed = System.currentTimeMillis();
                    return addr;
                }
            }
            return null;
        }

        public LocatorEntry accessed(NettyLocator loc, long rtt) {
            for (LocatorEntry addr : entries) {
                if (loc.equals(addr.locator)) {
                    addr.lastAccessed = System.currentTimeMillis();
                    addr.rtt = rtt;
                    return addr;
                }
            }
            return null;
        }

        public void add(InetSocketAddress addr) {
            entries.add(new LocatorEntry() {{
                this.locator = new NettyLocator(TYPE.UDP,addr.getAddress(), addr.getPort());
                this.lastAccessed = -1;
                this.rtt = Long.MAX_VALUE;
            }});
        }

        // get the locator entry candidate to try.
        public LocatorEntry nextTrialCandidate() {
            sort();
            assert (entries.size() != 0);
            return entries.get(entries.size() - 1);
        }

        public List<LocatorEntry> candidates() {
            return entries;
        }

        public void merge(KeyLocatorEntry ent, boolean active) {
            assert(key.equals(ent.key)) : "not match: " + key + "!=" + ent.key;
            List<LocatorEntry> missing = null;
            for (LocatorEntry oaddr : ent.entries) {
                boolean found = false;
                for (LocatorEntry addr : entries) {
                    if (addr.locator.equals(oaddr.locator)) {
                        found = true;
                       /*if (oaddr.lastAccessed > addr.lastAccessed) {
                           addr.lastAccessed = oaddr.lastAccessed; // XXX is this OK? maybe NO because other node can access but myself cannot access
                       }*/
                        if (active) {
                            addr.lastAccessed = System.currentTimeMillis(); // active entry.
                        }
                       break;
                    }
                }
                if (!found) { // oaddr is a new entry
                    if (missing == null) {
                        missing = new ArrayList<>();
                    }
                    if (active) {
                        oaddr.lastAccessed = System.currentTimeMillis(); // active entry.
                    }
                    else {
                        oaddr.lastAccessed = -1; // not accessed entry.
                    }
                    missing.add(oaddr);
                }
                else {
                    
                }
            }
            if (missing != null) {
                entries.addAll(missing);
            }
            sort();
            logger.debug("after merged({}):{}", key, entries);
        }
        
        public String toString() {
            return "key=" + key + ",ents=" + entries;
        }
    }

    @Override
    public NettyLocator getPrimaryLocator(Comparable<?> key) {
        KeyLocatorEntry kle = map.get(key);
        return kle == null ? null : kle.getPrimaryEntry().locator;
    }
    
    public NettyLocator[] getLocatorsArray(Comparable<?> key) {
        KeyLocatorEntry kle = map.get(key);
        return kle == null ? null : kle.getLocatorsArray();
    }
    
    public KeyLocatorEntry getKeyLocatorEntry(Comparable<?> key) {
        return map.get(key);
    }
    
    public String dump() {
        String ret = "----entries begin----";
        for (Entry<Comparable<?>, KeyLocatorEntry> ent : map.entrySet()) {
            ret += (ent.getKey() + "->" + ent.getValue());
        }
        ret += ("----entries end----");
        return ret;
    }
    
    static public void main(String args[]) throws Exception {
        KeyLocatorManager klm = new KeyLocatorManager();
        
        InetAddress[] candidates = new InetAddress[4];
        candidates[0] = InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 0, 1});
        candidates[1] = InetAddress.getByAddress(new byte[] {(byte)172, (byte)16, 1, 1});
        candidates[2] = InetAddress.getByAddress(new byte[] {(byte)172, (byte)17, 1, 105});
        candidates[3] = InetAddress.getLoopbackAddress();
        klm.register(1, candidates, 10080);
        klm.accessed(1, new NettyLocator(candidates[0], 10080));
        Thread.sleep(10);
        klm.accessed(1, new NettyLocator(candidates[1], 10080));
        klm.accessed(1, new NettyLocator(candidates[3], 10080));
        System.out.println(klm.candidates(1));

        InetAddress[] candidates2 = new InetAddress[4];
        candidates2[0] = InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 0, 1});
        candidates2[1] = InetAddress.getByAddress(new byte[] {(byte)172, (byte)16, 1, 1});
        candidates2[2] = InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, 1, 1});
        candidates2[3] = InetAddress.getLoopbackAddress();
        klm.register(1, candidates2, 10080);
        Thread.sleep(10);
        System.out.println(klm.candidates(1));
        klm.accessed(1, new NettyLocator(candidates[0], 10080));
        System.out.println(klm.candidates(1));
        klm.accessed(1, new NettyLocator(candidates[3], 10080));
        System.out.println(klm.candidates(1));
    }
}
