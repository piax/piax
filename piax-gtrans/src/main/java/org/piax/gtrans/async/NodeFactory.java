package org.piax.gtrans.async;

import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class NodeFactory {
    public abstract NodeImpl createNode(DdllKey key, int latency);
    public abstract String name();
}
