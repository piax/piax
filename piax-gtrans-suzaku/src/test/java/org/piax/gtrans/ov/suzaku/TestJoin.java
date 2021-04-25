package org.piax.gtrans.ov.suzaku;

import org.junit.jupiter.api.Test;

public class TestJoin {
    String host = "localhost";
    int port_base = 12367;
    //@Test
    public void ManyJoinTest() throws Exception {
        int nodes = 200;
        String seed = "tcp:" + host + ":" + port_base;
        for (int i = 0; i < nodes; i++) {
            new Suzaku<>("id:*:tcp:" + host + ":" + (port_base + i)).join(seed);
        }
    }
}
