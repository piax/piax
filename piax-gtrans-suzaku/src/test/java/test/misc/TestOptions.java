package test.misc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.piax.common.Config;
import org.piax.common.wrapper.DoubleKey;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.ov.suzaku.Suzaku;

class TestOptions {
    @Test
    void testArgs() throws Exception {
        Properties p = new Properties();
        p.setProperty("org.piax.gtrans.ov.Overlay.DEFAULT_ENDPOINT", "id:2.0:tcp:localhost:12367");
        p.setProperty("org.piax.gtrans.ov.Overlay.DEFAULT_SEED", "tcp:localhost:12367");
        p.setProperty("org.piax.gtrans.TransOptions.DEFAULT_RESPONSE_TYPE", "AGGREGATE"); 
        p.setProperty("org.piax.gtrans.ov.suzaku.Suzaku.EXEC_ASYNC", "true"); 
        Config.load(p);
        try (Suzaku<?,?> s = new Suzaku<>()) {
            s.join();
            assertTrue(((PrimaryKey)s.getEndpoint()).getRawKey().equals(new DoubleKey(2.0)));
        }
    }
}
