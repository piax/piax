package org.piax.gtrans.ov.async.suzaku;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;

public class FingerTables {
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
        System.out.println("FT replaced: " + node + " -> " + repl + "\n"
                + n.toStringDetail());
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
            System.out.println(n + ": add revptr! " + node);
        }
        this.reversePointers.add(node);
    }

    public void removeReversePointer(Node node) {
        boolean rc = this.reversePointers.remove(node);
        if (SuzakuStrategy.NOTIFY_WITH_REVERSE_POINTER.value() && !rc) {
            System.out.println(n + ": removeRP does not exist: " + node + "\n"
                    + n.toStringDetail());
        }
    }
}
