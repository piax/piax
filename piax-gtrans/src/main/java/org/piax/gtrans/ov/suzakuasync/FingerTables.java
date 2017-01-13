package org.piax.gtrans.ov.suzakuasync;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.util.ConcurrentReferenceHashMap;

public class FingerTables {
    final FingerTable forward;
    final FingerTable backward;
    Set<Node> reversePointers = new HashSet<>();
    LocalNode n;

    private Map<Node, FTEntry> entMap = new ConcurrentReferenceHashMap<>(
            16, ConcurrentReferenceHashMap.ReferenceType.WEAK,
            ConcurrentReferenceHashMap.ReferenceType.WEAK);

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
        FTEntry ent = entMap.get(node);
        if (ent == null) {
            ent = new FTEntry(node);
            entMap.put(node, ent);
        }
        return ent;
    }

    FTEntry getFTEntry(Node node, List<Node> nbrs) {
        FTEntry ent = entMap.get(node);
        if (ent == null) {
            ent = new FTEntry(node);
            ent.setNbrs(nbrs.toArray(new Node[0]));
            entMap.put(node, ent);
        }
        return ent;
    }

    void replace(FTEntry ent, FTEntry repl) {
        forward.replace(ent, repl);
        backward.replace(ent, repl);
    }

    Stream<FTEntry> stream() {
        Stream.Builder<FTEntry> s = Stream.builder();
        int fsize = forward.getFingerTableSize();
        for (int i = FingerTable.LOCALINDEX; i < fsize; i++) {
            s.add(forward.getFTEntry(i));
        }
        int bsize = backward.getFingerTableSize();
        for (int i = 0; i < bsize; i++) {
            s.add(backward.getFTEntry(i));
        }
        return s.build();
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
