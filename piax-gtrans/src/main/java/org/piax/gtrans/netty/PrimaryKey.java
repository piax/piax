package org.piax.gtrans.netty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.piax.common.ComparableKey;
import org.piax.common.wrapper.BooleanKey;
import org.piax.common.wrapper.DoubleKey;
import org.piax.common.wrapper.StringKey;
import org.piax.util.KeyComparator;

public class PrimaryKey implements ComparableKey<PrimaryKey>, NettyEndpoint {
    private static final long serialVersionUID = -8338701357931025730L;

    public class LocatorEntry implements Serializable {
        private static final long serialVersionUID = 8001237385879970460L;
        public NettyLocator locator;
        // The flag that indicates the locator is already visited. 
        // valid only when the key is specified as the destination.
        public boolean flag;
        public LocatorEntry(NettyLocator locator) {
            this.locator = locator;
            this.flag = false;
        }
    }

    ComparableKey<?> key;
    private static final KeyComparator keyComp = KeyComparator.getInstance();

    // direct locator;
    private NettyLocator locator;
    // indirect locators;
    private List<LocatorEntry> neighbors;
    long version;

    // a widlcard constructor.
    // at least one PeerLocator is required
    public PrimaryKey(NettyLocator locator) {
        key = null;
        this.locator = locator;
        version = System.currentTimeMillis();
    }

    public NettyLocator getLocator() {
        return locator;
    }

    public void setLocator(NettyLocator locator) {
        this.locator = locator;
    }
    
    public void setNeighborLocators(List<NettyLocator> locators) {
        neighbors = new ArrayList<LocatorEntry>();
        for (NettyLocator locator : locators) {
            neighbors.add(new LocatorEntry(locator));
        }
    }

    public void addNeighbor(NettyLocator locator) {
        neighbors = new ArrayList<LocatorEntry>();
        neighbors.add(new LocatorEntry(locator));
        version = System.currentTimeMillis();
    }

    public void setNeighbors(List<LocatorEntry> entries) {
        this.neighbors = entries;
        version = System.currentTimeMillis();
    }

    public List<LocatorEntry> getNeighbors() {
        return this.neighbors;
    }
    
    public ComparableKey<?> getRawKey() {
        return key;
    }

    public PrimaryKey(ComparableKey<?> key) {
        this.key = key;
        this.locator = null;
        this.neighbors = null;
        version = System.currentTimeMillis();
    }

    public PrimaryKey(ComparableKey<?> key, NettyLocator locator) {
        this.key = key;
        this.locator = locator;
        this.neighbors = null;
        version = System.currentTimeMillis();
    }
    
    // A suger constructor.
    public PrimaryKey(Comparable<?> o) {
        if (o instanceof String) {
            key = new StringKey((String)o);
        }
        else if (o instanceof Double) {
            key = new DoubleKey((Double) o);
        }
        else if (o instanceof Boolean) {
            key = new BooleanKey((Boolean) o);
        }
        version = System.currentTimeMillis();
        // ... any other key type.
    }

    @Override
    public int compareTo(PrimaryKey o) {
        if (key == null || o.key == null) {
            return 0; // is this OK?
        }
        return keyComp.compare(key, o.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (getClass() != o.getClass())
            return false;
        return key.equals(((PrimaryKey) o).key);
    }

    @Override
    public String toString() {
        return key.toString();
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public String getKeyString() {
        return key.toString();
    }
    
    public long getLocatorVersion() {
        return version;
    }
}
