package org.piax.gtrans.ov.async.suzaku;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.gtrans.async.FTEntry;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.async.rq.RQConditionalAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FingerTables {
    private static final Logger logger = LoggerFactory.getLogger(FingerTables.class);
    final FingerTable forward;
    final FingerTable backward;
    Set<Node> reversePointers = new HashSet<>();
    LocalNode n;

    public FingerTables(LocalNode n) {
        this.n = n;
        forward = new FingerTable(this, n, false);
        if (SuzakuStrategy.USE_BFT) {
            backward = new FingerTable(this, n, true);
        } else {
            backward = null;
        }
    }

    FTEntry getFTEntry(Node node) {
        Optional<FTEntry> match = stream()
                .filter(ent -> ent != null && ent.getNode() == node)
                .findAny();
        return match.orElse(null);
    }

    void replace(Node node, FTEntry repl) {
        forward.replace(node, repl);
        backward.replace(node, repl);
        logger.debug("FT replaced: {} -> {}\n{}", node, repl, n.toStringDetail());
    }

    Stream<FTEntry> stream() {
        if (SuzakuStrategy.USE_BFT) {
            return Stream.concat(forward.stream(), backward.stream());
        } else {
            return forward.stream();
        }
    }

    /**
     * 自ノードの reverse pointer で，[myKey, successorKey] に含まれるものを削除 
     */
    void sanitizeRevPtrs() {
        Set<Node> set = reversePointers.stream()
                .filter(x -> !Node.isOrdered(n.key, x.key, n.succ.key))
                .collect(Collectors.toSet());
        reversePointers = set;
    }

    public void addReversePointer(Node node) {
        if (node == n) {
            return;
        }
        if (SuzakuStrategy.DEBUG_REVPTR) {
            logger.debug("{}: add revptr! ", node);
        }
        this.reversePointers.add(node);
    }

    public void removeReversePointer(Node node) {
        boolean rc = this.reversePointers.remove(node);
        if (SuzakuStrategy.NOTIFY_WITH_REVERSE_POINTER.value() && !rc) {
            logger.debug("{}: removeRP does not exist: {}\n{}", n, node,
                    n.toStringDetail());
        }
    }
}
