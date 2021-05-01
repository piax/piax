package org.piax.gtrans.ov.suzaku;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventExecutor;
import org.piax.common.Destination;
import org.piax.common.wrapper.StringKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestSuzakuKeyAsync {
    private static final Logger logger = LoggerFactory.getLogger(TestSuzakuKeyAsync.class);
    CompletableFuture<Boolean> f = null;
    boolean result;
    List<String> results;

    //@Test
    public void addKeyInTimer() throws Exception {
        Suzaku<Destination, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<Destination, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
        result = false;
        results = new ArrayList<>();
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s1.setListener((tr, msg) -> {
                results.add((String) msg.getMessage());
            });
            System.out.println(s1.getKeys());
            s2.send(new StringKey("key"), "ABC");
            Thread.sleep(500);
            assertEquals(results.size(), 0);
            EventExecutor.sched("key", 10, () -> {
                f = s1.addKeyAsync(new StringKey("key"));
                f.whenComplete((ret, ex) -> {
                    result = ret;
                    System.out.println(s1.getKeys());
                });
            });
            Thread.sleep(500);
            s2.send(new StringKey("key"), "ABC");
            Thread.sleep(500);
            assertTrue(result);
            assertEquals(results.size(), 1);
            assertEquals(results.get(0), "ABC");
        } finally {
            s1.close();
            s2.close();
        }
    }
    
    

    //@Test
    public void removeKeyInTimer() throws Exception {
        Suzaku<Destination, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<Destination, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
        result = false;
        results = new ArrayList<>();
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s1.setListener((tr, msg) -> {
                results.add((String) msg.getMessage());
            });
            s1.addKey(new StringKey("key"));
            System.out.println(s1.getKeys());
            s2.send(new StringKey("key"), "ABC");
            Thread.sleep(500);
            assertEquals(results.size(), 1);
            EventExecutor.sched("keyremover", 10, () -> {
                f = s1.removeKeyAsync(new StringKey("key"));
                f.whenComplete((ret, ex) -> {
                    result = ret;
                    System.out.println(s1.getKeys());
                });
            });
            Thread.sleep(500);
            s2.send(new StringKey("key"), "ABC");
            Thread.sleep(500);
            assertTrue(result);
            assertEquals(results.size(), 1);
            assertEquals(results.get(0), "ABC");
        } finally {
            s1.close();
            s2.close();
        }
    }
}
