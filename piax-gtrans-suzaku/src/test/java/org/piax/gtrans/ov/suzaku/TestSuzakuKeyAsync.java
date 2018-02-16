package org.piax.gtrans.ov.suzaku;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.ov.ddll.DdllStrategy;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.Lower;
import org.piax.common.subspace.LowerUpper;
import org.piax.common.wrapper.DoubleKey;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RequestTransport.Response;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.Transport;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestSuzakuKeyAsync {
    private static final Logger logger = LoggerFactory.getLogger(TestSuzakuKeyAsync.class);
	CompletableFuture<Boolean> f = null;
	boolean result;
	List<String> results;
    @Test
    public void addKeyInTimer() throws Exception {
        Suzaku<Destination, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<Destination, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
		result = false;
		results = new ArrayList<>();
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s1.setListener((tr, msg) -> {
            		results.add((String)msg.getMessage());
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
        }
        finally {
            s1.close();
            s2.close();
        }
    }
    
    @Test
    public void removeKeyInTimer() throws Exception {
        Suzaku<Destination, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<Destination, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
		result = false;
		results = new ArrayList<>();
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s1.setListener((tr, msg) -> {
            		results.add((String)msg.getMessage());
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
        }
        finally {
            s1.close();
            s2.close();
        }
    }
}
