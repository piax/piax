package org.piax.gtrans.async;

public abstract class NodeFactory {
    public abstract NodeImpl createNode(int latency, int id);
    public abstract String name();
}
