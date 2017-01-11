package org.piax.gtrans.async;

import java.io.Serializable;

public class NodeAndIndex {
    public final FTEntryIndex index;
    public final Node node;
    public NodeAndIndex(Node node, FTEntryIndex index) {
        this.index = index;
        this.node = node;
    }

    @Override
    public String toString() {
        return node.toString();
    }

    public interface FTEntryIndex extends Serializable {
    }
}
