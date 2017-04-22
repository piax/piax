package org.piax.gtrans.async;

import java.util.HashMap;
import java.util.Map;

public abstract class LatencyProvider {
    abstract long latency(Node a, Node b);

    public static class StarLatencyProvider extends LatencyProvider {
        Map<Node, Long> map = new HashMap<>();
        
        public void add(Node node, long latency) {
            map.put(node, latency);
        }

        long latency(Node a, Node b) {
            Long l1 = map.get(a);
            Long l2 = map.get(b);
            //double jitter = 1.0 + (Sim.rand.nextDouble()
            //* 2 * NetworkParams.JITTER.value()) - NetworkParams.JITTER.value();
            return l1 + l2;
        }
    }
}
