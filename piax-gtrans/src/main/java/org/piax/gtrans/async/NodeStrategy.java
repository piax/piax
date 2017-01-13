package org.piax.gtrans.async;

import java.util.Arrays;
import java.util.List;

import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class NodeStrategy {
    protected LocalNode n;

    public void setupNode(LocalNode node) {
        this.n = node;
    }

    public Node getPredecessor() {
        return n.pred;
    }

    public Node getSuccessor() {
        return n.succ;
    }

    public Node getLocalLink() {
        return n;
    }

    public List<Node> getAllLinks2() {
        Node[] a = {
                getSuccessor(), n, getPredecessor()
        };
        return Arrays.asList(a);
    }

    public boolean isResponsible(DdllKey key) {
        return Node.isIn2(key, n.key, getSuccessor().key);
    }

    public abstract String toStringDetail();

    public abstract void initInitialNode();

    public abstract void joinAfterLookup(LookupDone lookupDone,
            Runnable success, FailureCallback failure);

    public void leave(Runnable success) {
        throw new UnsupportedOperationException("leave is not implemented");
    }

    public abstract void handleLookup(Lookup lookup);

    public int getMessages4Join() {
        throw new UnsupportedOperationException("getMessages4Join is not implemented");
    }
}
