package org.piax.gtrans.netty.idtrans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.piax.common.ComparableKey;
import org.piax.common.Endpoint;
import org.piax.common.PeerLocator;
import org.piax.common.wrapper.BooleanKey;
import org.piax.common.wrapper.DoubleKey;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.util.KeyComparator;

public class PrimaryKey implements ComparableKey<PrimaryKey>, NettyEndpoint {
    private static final long serialVersionUID = -8338701357931025730L;
    static public int MAX_NEIGHBORS = 30;
    
    public class NeighborEntry implements Serializable {
        private static final long serialVersionUID = 8001237385879970460L;
        public PrimaryKey key;
        // The flag that indicates the endpoint is already visited. 
        // this value is updated iff the key is used as a destination.
        public boolean visited;
        // The flag that indicates the channel to this endpoint is alive or not. 
        // this value is updated by the peer which has this key.
        public boolean alive;
        public NeighborEntry(PrimaryKey key) {
            this.key = key;
            this.visited = false;
            this.alive = true;
        }
    }

    // neighbor's neighbors are transient. (to keep the endpoint size small)  
    PrimaryKey cloneForSend() {
        PrimaryKey ret = new PrimaryKey(key, locator);
        ret.neighbors = neighbors.stream()
                .map(e -> {e.key.setNeighbors(null); return e;})
                .collect(Collectors.toList());
        return ret;
    }

    ComparableKey<?> key;
    private static final KeyComparator keyComp = KeyComparator.getInstance();

    // self locator;
    private NettyLocator locator;

    // neighbors;
    private List<NeighborEntry> neighbors;
    long version;

    // a widlcard constructor.
    // at least one PeerLocator is required
    public PrimaryKey(Endpoint seed) {
        key = null;
        if (seed instanceof NettyLocator) {
            this.locator = (NettyLocator)seed;
        }
        else {
            //XX
        }
        version = System.currentTimeMillis();
    }

    public NettyLocator getLocator() {
        return locator;
    }

    public void setLocator(NettyLocator locator) {
        this.locator = locator;
    }

    public void addNeighbor(PrimaryKey key) {
        neighbors = new ArrayList<NeighborEntry>();
        neighbors.add(new NeighborEntry(key));
        version = System.currentTimeMillis();
    }

    public void setNeighbors(List<NeighborEntry> eps) {
        this.neighbors = eps;
        version = System.currentTimeMillis();
    }

    public List<NeighborEntry> getNeighbors() {
        return this.neighbors;
    }

    public ComparableKey<?> getRawKey() {
        return key;
    }

    private NeighborEntry getNeighborEndpointEntry(PrimaryKey key) {
        return neighbors.stream().filter(e -> e.key.equals(key)).findFirst().orElse(null);
    }

    public void markNeighborClosed(PrimaryKey key) {
        NeighborEntry e = getNeighborEndpointEntry(key);
        e.alive = false;
    }

    public void markNeighborAlive(PrimaryKey key) {
        NeighborEntry e = getNeighborEndpointEntry(key);
        e.alive = true;
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
    public PrimaryKey newSameTypeEndpoint(String spec) throws ProtocolUnsupportedException {
        Endpoint ep = Endpoint.newEndpoint(spec);
        if (ep instanceof PeerLocator) {
            ep = new PrimaryKey(ep);
        }
        if (!(ep instanceof PrimaryKey)) {
            throw new ProtocolUnsupportedException("primary key or locator expected.");
        }
        return (PrimaryKey)ep;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if ((key == null) && o instanceof PeerLocator) { // WILDCARD
            return locator.equals(o);
        }
        if (getClass() != o.getClass())
            return false;
        if ((key == null) || (((PrimaryKey)o).key == null)) { // WILDCARD
            return ((PrimaryKey)o).locator.equals(locator);
        }
        return key.equals(((PrimaryKey) o).key);
    }

    @Override
    public String toString() {
        if (key == null) // wildcard
            return "WILDCARD";
        return key.toString();
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public int getPort() {
        return locator.getPort();
    }

    @Override
    public String getHost() {
        return locator.getHost();
    }

    @Override
    public String getKeyString() {
        return key.toString();
    }
    
    public long getLocatorVersion() {
        return version;
    }

}
