package org.piax.gtrans.async;

import java.util.HashMap;
import java.util.Map;

public abstract class LatencyProvider {
    abstract int latency(Node a, Node b);

    public static class StarLatencyProvider extends LatencyProvider {
        Map<Node, Integer> map = new HashMap<>();
        
        public void add(Node node, int latency) {
            map.put(node, latency);
        }

        int latency(Node a, Node b) {
            Integer l1 = map.get(a);
            Integer l2 = map.get(b);
            //double jitter = 1.0 + (Sim.rand.nextDouble()
            //* 2 * NetworkParams.JITTER.value()) - NetworkParams.JITTER.value();
            return l1 + l2;
        }
    }
}
