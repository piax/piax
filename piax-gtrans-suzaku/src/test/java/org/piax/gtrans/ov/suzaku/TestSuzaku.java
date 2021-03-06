package org.piax.gtrans.ov.suzaku;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
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

class TestSuzaku {
    private static final Logger logger = LoggerFactory.getLogger(TestSuzaku.class);
    @Test
    public void minimalSendTest() throws Exception {
        Suzaku<StringKey, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<StringKey, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s2.addKey(new StringKey("hello"));
            AtomicBoolean received = new AtomicBoolean(false);
            s2.setListener((szk, msg) -> {
                received.set(true);
                assertTrue(msg.getMessage().equals("world"));
            });
            s1.send(new StringKey("hello"), "world");
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
        finally {
            s1.close();
            s2.close();
        }
    }
    
    @Test
    public void addDelSendTest() throws Exception {
        Suzaku<StringKey, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<StringKey, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s2.addKey(new StringKey("hello"));
            s2.removeKey(new StringKey("hello"));
            s2.addKey(new StringKey("hello"));
            AtomicBoolean received = new AtomicBoolean(false);
            s2.setListener((szk, msg) -> {
                received.set(true);
                assertTrue(msg.getMessage().equals("world"));
            });
            s1.send(new StringKey("hello"), "world");
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
        finally {
            s1.close();
            s2.close();
        }
    }

    @Test
    public void joinToFailedNetTest() throws Exception {
        DdllStrategy.pingPeriod.set(3000);
        Suzaku<StringKey, StringKey> s1 = new Suzaku<>("id:pid1:tcp:localhost:12367");
        Suzaku<StringKey, StringKey> s2 = new Suzaku<>("id:pid2:tcp:localhost:12368");
        Suzaku<StringKey, StringKey> s3 = new Suzaku<>("id:pid3:tcp:localhost:12369");
        try {
            s1.join("id:pid1:tcp:localhost:12367");
            s2.join("id:pid1:tcp:localhost:12367");
            // to cause a failure
            s2.getBaseTransport().fin();
            //Thread.sleep(1000);
            s3.join("id:pid1:tcp:localhost:12367");
            s3.addKey(new StringKey("hello"));
            AtomicBoolean received = new AtomicBoolean(false);
            s3.setListener((szk, msg) -> {
                received.set(true);
                assertTrue(msg.getMessage().equals("world"));
            });
            s1.send(new StringKey("hello"), "world");
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
        finally {
            s1.close();
            s2.close();
            s3.close();
        }
    }
    
    @Test
    public void noResponseNoRetransRequestTest() throws Exception {
        try(Suzaku<StringKey, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
            Suzaku<StringKey, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");){
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s1.addKey(new StringKey("hello"));
            s1.setRequestListener((szk, msg) -> {
                return msg.getMessage() + "2";
            });
            AtomicBoolean res = new AtomicBoolean(false);
            AtomicInteger count = new AtomicInteger(0);
            s1.requestAsync(new StringKey("hello"), "world",
                    (ret, e)-> { // receive response
                        logger.info("got: {}", ret);
                        res.set(Response.EOR.equals(ret));
                        count.incrementAndGet();
                    },
                    new TransOptions(ResponseType.NO_RESPONSE, RetransMode.NONE));
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(res.get());
            assertTrue(count.get() == 1);
        }
    }

    @Test
    public void minimalRequestTest() throws Exception {
        Suzaku<StringKey, StringKey> s1 = new Suzaku<>("tcp:localhost:12367");
        Suzaku<StringKey, StringKey> s2 = new Suzaku<>("tcp:localhost:12368");
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s2.addKey(new StringKey("hello"));
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(new StringKey("hello"), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
        finally {
            s2.close();
            s1.close();
        }
    }

    @Test
    public void wildcardJoinTest() throws Exception {
        Suzaku<StringKey, StringKey> s1 = new Suzaku<>("id:p1:tcp:localhost:12367");
        Suzaku<StringKey, StringKey> s2 = new Suzaku<>("id:p2:tcp:localhost:12368");
        try {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(new StringKey("p2"), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
        finally {
            s1.close();
            s2.close();
        }
    }
    
    @Test
    public void udpJoinTest() throws Exception {
        try (
                Suzaku<StringKey, StringKey> s1 = new Suzaku<>("udp:p1");
                Suzaku<StringKey, StringKey> s2 = new Suzaku<>("udp:p2:12368");
                ){
            s1.join("udp:*:localhost:12367");
            s2.join("udp:*:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(new StringKey("p2"), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
    }
    
    @Test
    public void udpJoinTest2() throws Exception {
        try (
                Suzaku<StringKey, StringKey> s1 = new Suzaku<>("udp:p1");
                Suzaku<StringKey, StringKey> s2 = new Suzaku<>("udp:p2:12368");
                Suzaku<StringKey, StringKey> s3 = new Suzaku<>("udp:p3:12369");
                Suzaku<StringKey, StringKey> s4 = new Suzaku<>("udp:p4:12370");
                ){
            s1.join("udp:*:localhost:12367");
            s2.join("udp:*:localhost:12367");
            s3.join("udp:*:localhost:12367");
            s4.join("udp:*:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(new StringKey("p2"), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
    }

    @Test
    public void udpJoinTest3() throws Exception {
        try (
                Suzaku<Destination, StringKey> s1 = new Suzaku<>("udp:*:12371");
                Suzaku<Destination, StringKey> s2 = new Suzaku<>("udp:*:12372");
                Suzaku<Destination, StringKey> s3 = new Suzaku<>("udp:*:12373");
                ){
            s1.join("udp:*:localhost:12371");
            s2.join("udp:*:localhost:12371");
            s3.join("udp:*:localhost:12371");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(s2.getPeerId(), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
    }

    @Test
    public void peerIdRequestTest() throws Exception {
        try (
                // * means use the peerId as a primary key.
                Suzaku<Destination, ComparableKey<?>> s1 = new Suzaku<>("id:*:tcp:localhost:12367");
                Suzaku<Destination, ComparableKey<?>> s2 = new Suzaku<>("id:*:tcp:localhost:12368");
        ) {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(s2.getPeerId(), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
    }

    @Test
    public void lowerTransportTest() throws Exception {
        try (
                // * means use the peerId as a primary key.
                Suzaku<Destination, ComparableKey<?>> s1 = new Suzaku<>("id:*:tcp:localhost:12367");
                Suzaku<Destination, ComparableKey<?>> s2 = new Suzaku<>("id:*:tcp:localhost:12368");
                ) {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            AtomicBoolean received = new AtomicBoolean(false);
            s2.getLowerTransport().setListener(new ObjectId("test"), (trans, rmsg)->{
                if (rmsg.getMessage().equals("world")) {
                    received.set(true);
                }
            });
            
            // XXX explain why this requires type cast.
            ((Transport<PrimaryKey>) s1.getLowerTransport()).sendAsync(new ObjectId("test"), 
                    new PrimaryKey(s2.getPeerId()), "world", new TransOptions());
            Thread.sleep(1000); // unless this line, finishes immediately.
            //assertTrue(received.get());
        }
    }

    @Test
    public void tryWithResourcesTest() {
        try (
            Suzaku<StringKey, StringKey> s1 = new Suzaku<>("id:p1:tcp:localhost:12367");
            Suzaku<StringKey, StringKey> s2 = new Suzaku<>("id:p2:tcp:localhost:12368");
        ) {
            s1.join("tcp:localhost:12367");
            s2.join("tcp:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(new StringKey("p2"), "world",
                    (ret, e)-> { // receive response
                        if (e != null) {
                            e.printStackTrace();
                        }
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            //throw new IOException("test!");
            //Thread.sleep(1000); // unless this line, finishes immediately.
        }
        catch (Exception e) {
            //e.printStackTrace();
        }
        try (Suzaku<StringKey, StringKey> s3 = new Suzaku<StringKey, StringKey>("id:p1:tcp:localhost:12367")) {
            // ensure the address is not already in use.
            assertTrue(s3.isUp());
        }
        catch (Exception e) {
           // 
        }
    }

    @Test
    public void minimalRequestIdTest() throws Exception {
        Suzaku<StringKey, StringKey> s1 = new Suzaku<>("id:p1:tcp:localhost:12367");
        Suzaku<StringKey, StringKey> s2 = new Suzaku<>("id:p2:tcp:localhost:12368");
        try {
            s1.join("id:p1:tcp:localhost:12367");
            s2.join("id:p1:tcp:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            AtomicBoolean received = new AtomicBoolean(false);
            s1.requestAsync(new StringKey("p2"), "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            received.set(true);
                            assertTrue(ret.equals("world2"));
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received.get());
        }
        finally {
            s1.close();
            s2.close();
        }
    }

    @Test
    public void minimalRangeRequestIdTest() throws Exception {
        Suzaku<KeyRange<DoubleKey>, DoubleKey> s1 = new Suzaku<>("id:0.0:tcp:localhost:12367");
        Suzaku<KeyRange<DoubleKey>, DoubleKey> s2 = new Suzaku<>("id:0.5:tcp:localhost:12368");
        Suzaku<KeyRange<DoubleKey>, DoubleKey> s3 = new Suzaku<>("id:0.7:tcp:localhost:12369");
        try {
            s1.join("id:0.0:tcp:localhost:12367");
            s2.join("id:0.0:tcp:localhost:12367");
            s3.join("id:0.0:tcp:localhost:12367");
            s2.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "2";
            });
            s3.setRequestListener((szk, msg) -> { // make a response
                return msg.getMessage() + "3";
            });
            AtomicBoolean received2 = new AtomicBoolean(false);
            AtomicBoolean received3 = new AtomicBoolean(false);
            s1.requestAsync(new KeyRange<DoubleKey>(new DoubleKey(0.2), true, new DoubleKey(0.8), true),
                    "world",
                    (ret, e)-> { // receive response
                        if (ret != Response.EOR) {
                            if (ret.equals("world2")) {
                                received2.set(true);
                            }
                            if (ret.equals("world3")) {
                                received3.set(true);
                            }
                        }
                    });
            Thread.sleep(1000); // unless this line, finishes immediately.
            assertTrue(received2.get() && received3.get());
        }
        finally {
            s1.close();
            s2.close();
            s3.close();
        }
    }
    
    @SuppressWarnings("unchecked")
    //@Test XXX fails if executed on command line.
    public void SuzakuRoutingTableTest() throws Exception {
        int numOfPeers = 32;
        // get peers
        Endpoint loc = null;
        Suzaku<Destination, ComparableKey<?>> trs[] = new Suzaku[numOfPeers];
        Peer peers[] = new Peer[numOfPeers];
        for (int i = 0; i < numOfPeers; i++) {
            PeerId pid = new PeerId("p" + i);
            Endpoint l = Endpoint.newEndpoint("tcp:localhost:" + (12367 + i));
            if (loc == null) {
                loc = l; // remember for seed;
            }
            trs[i] = new Suzaku<Destination, ComparableKey<?>>(
                    (peers[i] = Peer.getInstance(pid))
                            .newBaseChannelTransport(l));
            int x = i;
            trs[i].setListener(new OverlayListener<Destination, ComparableKey<?>>() {
                public void onReceive(
                        Overlay<Destination, ComparableKey<?>> overlay,
                        OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                }

                public FutureQueue<?> onReceiveRequest(
                        Overlay<Destination, ComparableKey<?>> overlay,
                        OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                    try {
                        DoubleKey key = new DoubleKey((double) x + numOfPeers);
                        if (!overlay.getKeys().contains(key)) {
                            logger.debug("adding key" + key);
                            overlay.addKey(key);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return overlay.singletonFutureQueue("recv" + x);
                }
            });
        }
        try {
            for (int i = 0; i < numOfPeers; i++) {
                trs[i].join(loc);
                trs[i].addKey(new DoubleKey((double) i));
            }

            for (int i = 0; i < numOfPeers; i++) {
                logger.debug("size=" + trs[i].getAll().length);
                logger.debug("height="
                        + trs[i].getHeight(new DoubleKey((double) i)));
                for (int j = 0; j < trs[i].getHeight(new DoubleKey((double) i)); j++) {
                    int rkeys = trs[i].getRights(new DoubleKey((double) i), j).length;
                    for (int k = 0; k < rkeys; k++) {
                        logger.debug("rights"
                                + trs[i].getLocal(new DoubleKey((double) i)).key
                                + "["
                                + j
                                + "]="
                                + trs[i].getRights(new DoubleKey((double) i), j)[k].key);
                    }
                    int lkeys = trs[i].getLefts(new DoubleKey((double) i), j).length;
                    for (int k = 0; k < lkeys; k++) {
                        logger.debug("lefts"
                                + trs[i].getLocal(new DoubleKey((double) i)).key
                                + "["
                                + j
                                + "]="
                                + trs[i].getLefts(new DoubleKey((double) i), j)[k].key);
                    }
                }
                if (i != numOfPeers - 1) {
                    assertTrue(((DoubleKey) (trs[i].getRight(new DoubleKey(
                            (double) i)).key.getRawKey())).getKey() == (double) i + 1);
                }
                if (i != 0) {
                    assertTrue(((DoubleKey) (trs[i].getLeft(new DoubleKey(
                            (double) i)).key.getRawKey())).getKey() == (double) i - 1);
                }
            }

        } finally {
            for (int i = 0; i < numOfPeers; i++) {
                peers[i].fin();
            }
        }
    }
    
//    @Test
    public void lowerIdTest() throws Exception {
        AtomicBoolean received1 = new AtomicBoolean(false);
        AtomicBoolean received2 = new AtomicBoolean(false);
        try (
                Overlay<LowerUpper, DoubleKey> ov1 = new Suzaku<>("id:0.0:tcp:localhost:12367");
                Overlay<LowerUpper, DoubleKey> ov2 = new Suzaku<>("id:0.2:tcp:localhost:12368");
             ) {
            ov1.setListener((trans, rmsg) -> {
                logger.debug("emu recv1:" + rmsg.getMessage());
                received1.set(rmsg.getMessage().equals("recv"));
            });
            ov2.setListener((trans, rmsg) -> {
                try {
                    received2.set(rmsg.getMessage().equals("data"));
                    trans.send(new Lower<DoubleKey>(false, new DoubleKey(0.1), 1), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            });
            assertTrue(ov1.join("tcp:localhost:12367"));
            assertTrue(ov2.join("tcp:localhost:12367"));

            Thread.sleep(500);

            ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.6), 1), "data");
            Thread.sleep(1000);
            assertTrue(received2.get(), "SG2 receive failed");
            assertTrue(received1.get(), "SG1 receive failed");
        }
    }

    @Test
    public void lowerEqualsIdTest() throws Exception {
        AtomicBoolean received1 = new AtomicBoolean(false);
        AtomicBoolean received2 = new AtomicBoolean(false);
        AtomicBoolean received3 = new AtomicBoolean(false);
        try (
                Overlay<LowerUpper, DoubleKey> ov1 = new Suzaku<>("id:0.0:tcp:localhost:12367");
                Overlay<LowerUpper, DoubleKey> ov2 = new Suzaku<>("id:0.2:tcp:localhost:12368");
                Overlay<LowerUpper, DoubleKey> ov3 = new Suzaku<>("id:0.2:tcp:localhost:12369");
             ) {
            ov1.setListener((trans, rmsg) -> {
                received1.set(true);
            });
            ov2.setListener((trans, rmsg) -> {
                received2.set(true);
            });
            ov3.setListener((trans, rmsg) -> {
                received3.set(true);
            });
            assertTrue(ov1.join("tcp:localhost:12367"));
            assertTrue(ov2.join("tcp:localhost:12367"));
            assertTrue(ov3.join("tcp:localhost:12367"));

            Thread.sleep(500);

            ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.2), 1), "data");
            Thread.sleep(1000);
            assertFalse(received3.get(), "ov3 received falsely");
            assertFalse(received2.get(), "ov2 received falsely");
            assertTrue(received1.get(), "ov1 receive failed");
        }
    }
    
        //private static final Logger logger = LoggerFactory
        //        .getLogger(TestAddRemoveKey.class);
        int k = 3;
        String host = "localhost";
        int port_base = 12367;

        public void prepareNodes(ArrayList<Suzaku<Destination,ComparableKey<?>>> s, int nodes, String services[]) {
            try {
                String seed = "tcp:" + host + ":" + port_base;
                String netIdBase = "net";
                for (int i = 0; i < nodes; i++) {
                    s.add(new Suzaku<>("id:" + netIdBase + (i / 2)  + ".subnet" + (i % 2) + ".host" + i + ":tcp:" + host + ":" + (port_base + i * 10)));
                    s.get(i).join(seed);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        public void cleanNodes(ArrayList<Suzaku<Destination,ComparableKey<?>>> s) {
            try {
                for (int i = s.size() - 1; i >= 0; i--) {
                    s.get(i).leave();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        //@Test
        public void AddRemoveKeyTest() {
            AddRemoveKeyTest(3);
        }

        public void AddRemoveKeyTest(int nodes) {
            String keyname = "sport";
            ArrayList<Suzaku<Destination,ComparableKey<?>>> s = new ArrayList<Suzaku<Destination,ComparableKey<?>>>();
            prepareNodes(s, nodes, new String[] { keyname });
            try {
                for (int j = 0; j < 1000;j++) {
                    //System.out.println("loop " + j);
                    for (int i = 0; i < nodes; i++) {
                        //System.out.println("kns" + i + ": trying to discover " + k + " services");
                        //System.out.println("kns" + i + ": trying to unregister service");
                        s.get(i).removeKey(new StringKey(keyname));
                        //kns.get(i).discover(tofind, k, 0, kns.get(i).getEndpoint());
                        //System.out.println("kns" + i + ": trying to reregister service");
                        s.get(i).addKey(new StringKey(keyname));
                        //System.out.println("sleeping 1 sec");
                        //Thread.sleep(1000);
                    }
                }
                logger.info("test completed.");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cleanNodes(s);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }



}
