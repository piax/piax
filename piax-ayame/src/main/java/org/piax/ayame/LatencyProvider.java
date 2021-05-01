/*
 * LatencyProvider.java - A class to provide latency
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

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
