package org.piax.gtrans.netty;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.piax.gtrans.netty.kryo.KryoUtil;

class TestKryoUtil {

    static class TestClass {
        String x;
        int y;
        public TestClass() {
            x = "x";
            y = 100;
        }
    }
    @Test
    void test() {
        assertThrows(IllegalArgumentException.class, () -> {
                KryoUtil.getRegistrationId(TestClass.class);});
        KryoUtil.register(TestClass.class);
        assertThrows(IllegalArgumentException.class, () -> {
                KryoUtil.getRegistrationId(TestClass.class);});
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            assertThrows(IllegalArgumentException.class, () -> {
                KryoUtil.getRegistrationId(TestClass.class);});
            KryoUtil.encode(new TestClass(), 100, 100);
            KryoUtil.encode(new TestClass(), 100, 100);
        }).start();
        KryoUtil.encode(new TestClass(), 100, 100);
        assertTrue(KryoUtil.getRegistrationId(TestClass.class) != -1);
        KryoUtil.encode(new TestClass(), 100, 100);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
        }
    }

}
