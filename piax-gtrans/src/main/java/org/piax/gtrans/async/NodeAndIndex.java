package org.piax.gtrans.async;

public class NodeAndIndex {
    // index = 0 -> local entry
    // index > 0 -> FFT[index - 1]
    // index < 0 -> BFT[-index - 1]
    public final int index;
    public final Node node;
    public NodeAndIndex(Node node, int index) {
        this.index = index;
        this.node = node;
    }
    @Override
    public String toString() {
        return node.toString();
    }
}