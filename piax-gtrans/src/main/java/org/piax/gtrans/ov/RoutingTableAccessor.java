package org.piax.gtrans.ov;

import java.util.Set;

import org.piax.common.Key;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ov.ddll.Link;

public interface RoutingTableAccessor {

    default public Link[] getAll() throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    
    default public Set<Key> keySet() throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    }
    
    default public Link getLocal(Comparable<?> key) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    default public Link getRight(Comparable<?> key) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    }; // a.k.a. successor
    default public Link getLeft(Comparable<?> key) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    }; // a.k.a. predecessor
    default public Link getRight(Comparable<?> key, int level) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    default public Link getLeft(Comparable<?> key, int level) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    
    default public int getHeight(Comparable<?> key) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    // Getting redundant links
    default public Link[] getRights(Comparable<?> key) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    default public Link[] getLefts(Comparable<?> key) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    default public Link[] getRights(Comparable<?> key, int level) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
    default public Link[] getLefts(Comparable<?> key, int level) throws ProtocolUnsupportedException {
        throw new ProtocolUnsupportedException();
    };
}
