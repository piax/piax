package test.trans;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Id;
import org.piax.common.Key;
import org.piax.common.Location;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.common.attribs.RowData;
import org.piax.common.dcl.DCLTranslator;
import org.piax.common.dcl.DestinationCondition;
import org.piax.common.subspace.GeoRectangle;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.LowerUpper;
import org.piax.common.wrapper.DoubleKey;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.combined.CombinedOverlay;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.ov.flood.SimpleFlooding;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.szk.Suzaku;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.util.KeyComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOverlay {

    boolean received1, received2, received3;
    boolean send_recv1, send_recv2, send_recv3;
    private static final Logger logger = 
            LoggerFactory.getLogger(TestTransport.class);
    static int portNumber = 12366;
    static int seq = 0;
    
    public String newId() {
    	return "id" + (seq++);
    }
    
    @Test
    public void LLNetTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport("udp",
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport("udp",
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport("udp",
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<GeoRegion, Location> tr1, tr2, tr3;
        tr1 = new LLNet(new MSkipGraph<Destination, ComparableKey<?>>(bt1));
        tr2 = new LLNet(new MSkipGraph<Destination, ComparableKey<?>>(bt2));
        tr3 = new LLNet(new MSkipGraph<Destination, ComparableKey<?>>(bt3));

        received2 = false;
        received3 = false;
        
        tr2.setListener(new OverlayListener<GeoRegion, Location>() {
        		@Override
            public void onReceive(Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                logger.debug("llnet tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }

            // unused
            @Override
            public void onReceive(Transport<GeoRegion> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr3.setListener(new OverlayListener<GeoRegion, Location>() {
            public void onReceive(Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    	Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                logger.debug("llnet tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }

            // unused
            public void onReceive(Transport<GeoRegion> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr1.join(loc);
        tr2.join(loc);
        tr3.join(loc);
        Thread.sleep(500);
        tr1.addKey(new Location(1.0, 1.0));
        tr2.addKey(new Location(2.0, 2.0));
        tr3.addKey(new Location(3.0, 3.0));
        Thread.sleep(500);

        List<Object> l = Arrays.asList(tr1.request(new GeoRectangle(2, 2, 1, 1), "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 receive failed", received3);

        received1 = false;
        received2 = false;
        received3 = false;
        List<Object> l2 = Arrays.asList(tr1.request(new GeoRectangle(2, 2, 0.5, 0.5), "req").getAllValues());
        received2 = l2.contains("recv2");
        received3 = l2.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("rect(point(0, 0), 5, 5)", "req");
        Thread.sleep(1000);
        assertTrue("SG2 receive failed", send_recv2);
        assertTrue("SG3 receive failed", send_recv3);
        
        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void LLNetOnSuzakuTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app1");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(null, new TransportId("udp1"),
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(null, new TransportId("udp1"),
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(null, new TransportId("udp1"),
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<GeoRegion, Location> tr1, tr2, tr3;
        tr1 = new LLNet(new TransportId("llnet1"), 
                new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mskip1"), bt1));
        tr2 = new LLNet(new TransportId("llnet1"), 
                new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mskip1"), bt2));
        tr3 = new LLNet(new TransportId("llnet1"), 
                new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mskip1"), bt3));

        received2 = false;
        received3 = false;
        
        tr2.setListener(new OverlayListener<GeoRegion, Location>() {
            public void onReceive(Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                logger.debug("llnet tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }

            // unused
            public void onReceive(Transport<GeoRegion> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr3.setListener(new OverlayListener<GeoRegion, Location>() {
            public void onReceive(Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<GeoRegion, Location> overlay,
                    OverlayReceivedMessage<Location> rmsg) {
                logger.debug("llnet tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }

            // unused
            public void onReceive(Transport<GeoRegion> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr1.join(loc);
        tr2.join(loc);
        tr3.join(loc);
        Thread.sleep(500);
        tr1.addKey(new Location(1.0, 1.0));
        
        tr2.addKey(new Location(2.0, 2.0));
        tr3.addKey(new Location(3.0, 3.0));
        Thread.sleep(500);

        List<Object> l = Arrays.asList(tr1.request(new GeoRectangle(2, 2, 1, 1), "req").getAllValues());
        //List<Object> l = Arrays.asList(tr1.request("rect(point(2, 2), 1, 1)", "req"));
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 receive failed", received3);

        received1 = false;
        received2 = false;
        received3 = false;
        List<Object> l2 = Arrays.asList(tr1.request(new GeoRectangle(2, 2, 0.5, 0.5), "req").getAllValues());
        received2 = l2.contains("recv2");
        received3 = l2.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("rect(point(0, 0), 5, 5)", "req");
        Thread.sleep(1000);
        assertTrue("SG2 receive failed", send_recv2);
        assertTrue("SG3 receive failed", send_recv3);
        
        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void DOLRTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<Key, Key> tr1, tr2, tr3;
        tr1 = new DOLR<Key>(
                new MSkipGraph<Destination, ComparableKey<?>>(
                        bt1));
        tr2 = new DOLR<Key>( 
                new MSkipGraph<Destination, ComparableKey<?>>(
                        bt2));
        tr3 = new DOLR<Key>( 
                new MSkipGraph<Destination, ComparableKey<?>>(
                        bt3));

        received2 = false;
        received3 = false;
        
        tr2.setListener(new OverlayListener<Key, Key>() {
            public void onReceive(Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("dolr tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }
            
            // unused
            public void onReceive(Transport<Key> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr3.setListener(new OverlayListener<Key, Key>() {
            public void onReceive(Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("dolr tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }
            
            // unused
            public void onReceive(Transport<Key> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr1.join(loc);
        tr2.join(loc);
        tr3.join(loc);
        Thread.sleep(500);
        tr1.addKey(new StringKey("tera"));
        tr2.addKey(new StringKey("ishi"));
        tr3.addKey(new StringKey("yos"));
        Thread.sleep(500);

        // It fails!
        List<Object> l = Arrays.asList(tr1.request(new StringKey("ishi"), "req").getAllValues());

        // Following is OK!
        // List<Object> l = Arrays.asList(tr1.request(new ObjectId("app"), new
        // Ranges(new Range<WrappedKey<String>>(new
        // WrappedKey<String>("ishi"))), "req", 2000));
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);

        received1 = false;
        received2 = false;
        received3 = false;
        List<Object> l2 = Arrays.asList(tr1.request(new StringKey("yos"), "req").getAllValues());
        received2 = l2.contains("recv2");
        received3 = l2.contains("recv3");

        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 receive failed", received3);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("\"ishi\"", "req");
        Thread.sleep(1000);
        assertTrue("SG2 receive failed", send_recv2);
        assertTrue("SG3 falsely received", !send_recv3);
        
        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void DOLROnSuzakuTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<Key, Key> tr1, tr2, tr3;
        tr1 = new DOLR<Key>(new TransportId("dolr"), 
                new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mcs"), bt1));
        tr2 = new DOLR<Key>(new TransportId("dolr"), 
                new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mcs"), bt2));
        tr3 = new DOLR<Key>(new TransportId("dolr"), 
                new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mcs"), bt3));

        received2 = false;
        received3 = false;
        
        tr2.setListener(new OverlayListener<Key, Key>() {
            public void onReceive(Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("dolr tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }
            
            // unused
            public void onReceive(Transport<Key> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr3.setListener(new OverlayListener<Key, Key>() {
            public void onReceive(Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Key, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("dolr tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }
            
            // unused
            public void onReceive(Transport<Key> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr1.join(loc);
        tr2.join(loc);
        tr3.join(loc);
        Thread.sleep(500);
        tr1.addKey(new StringKey("tera"));
        tr2.addKey(new StringKey("ishi"));
        tr3.addKey(new StringKey("yos"));
        Thread.sleep(5000);

        List<Object> l = Arrays.asList(tr1.request(new StringKey("ishi"), "req").getAllValues());

        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);

        received1 = false;
        received2 = false;
        received3 = false;
        List<Object> l2 = Arrays.asList(tr1.request(new StringKey("yos"), "req").getAllValues());
        received2 = l2.contains("recv2");
        received3 = l2.contains("recv3");

        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 receive failed", received3);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("\"ishi\"", "req");
        Thread.sleep(1000);
        assertTrue("SG2 receive failed", send_recv2);
        assertTrue("SG3 falsely received", !send_recv3);
        
        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void MaxLessThanOnSuzakuTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<LowerUpper, DoubleKey> ov1, ov2, ov3;
        
        ov1 = new Suzaku<LowerUpper, DoubleKey>(bt1);
        ov2 = new Suzaku<LowerUpper, DoubleKey>(bt2);
        ov3 = new Suzaku<LowerUpper, DoubleKey>(bt3);

        received2 = false;
        received3 = false;
        
        ov2.setListener(new OverlayListener<LowerUpper, DoubleKey>() {
			@Override
			public void onReceive(
					Overlay<LowerUpper, DoubleKey> overlay,
					OverlayReceivedMessage<DoubleKey> rmsg) {
				logger.debug("OnReceive O");
				send_recv2 = true;
			}

			@Override
			public FutureQueue<?> onReceiveRequest(
					Overlay<LowerUpper, DoubleKey> overlay,
					OverlayReceivedMessage<DoubleKey> rmsg) {
				logger.debug("2.0 matched:" + rmsg.getMatchedKeys());
				FutureQueue<?> fq = new FutureQueue();
				fq.add(new RemoteValue(overlay.getPeerId(), "recv2"));
				fq.setEOFuture();
                return fq;
			}

			@Override
			public void onReceive(Transport<LowerUpper> trans,
					ReceivedMessage rmsg) {
			}
        });

        ov3.setListener(new OverlayListener<LowerUpper, DoubleKey>() {
    			@Override
    			public void onReceive(
    					Overlay<LowerUpper, DoubleKey> overlay,
    					OverlayReceivedMessage<DoubleKey> rmsg) {
    				send_recv3 = true;
    			}

    			@Override
    			public FutureQueue<?> onReceiveRequest(
    					Overlay<LowerUpper, DoubleKey> overlay,
    					OverlayReceivedMessage<DoubleKey> rmsg) {
    				logger.debug("3.0 matched:" + rmsg.getMatchedKeys());
    				FutureQueue<?> fq = new FutureQueue();
    				fq.add(new RemoteValue(overlay.getPeerId(), "recv3"));
    				fq.setEOFuture();
                    return fq;
    			}

    			@Override
    			public void onReceive(Transport<LowerUpper> trans,
    					ReceivedMessage rmsg) {
    			}
        });

        ov1.join(loc);
        ov2.join(loc);
        ov3.join(loc);
        Thread.sleep(500);
        ov1.addKey(new DoubleKey(1.0));
        ov2.addKey(new DoubleKey(2.0));
        ov3.addKey(new DoubleKey(3.0));
        logger.debug("sleeping 10 secs");
        Thread.sleep(10000);

        DoubleKey k = new DoubleKey(2.5);
        KeyRange<?> range = new KeyRange(
                KeyComparator.getMinusInfinity(DoubleKey.class), false,
                k, true);
        LowerUpper dst = new LowerUpper(range, false, 1);
        List<Object> l = Arrays.asList(ov1.request(dst, "req", new TransOptions(100000)).getAllValues());

        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);
        
        k = new DoubleKey(3.5);
        range = new KeyRange(
                KeyComparator.getMinusInfinity(DoubleKey.class), false,
                k, true);
        dst = new LowerUpper(range, false, 1);
        l = Arrays.asList(ov1.request(dst, "req").getAllValues());

        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 not received", received3);

        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void MaxLessThanOnSGTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<LowerUpper, DoubleKey> ov1, ov2, ov3;
        
        ov1 = new MSkipGraph<LowerUpper, DoubleKey>(bt1);
        ov2 = new MSkipGraph<LowerUpper, DoubleKey>(bt2);
        ov3 = new MSkipGraph<LowerUpper, DoubleKey>(bt3);

        received2 = false;
        received3 = false;
        
        ov2.setListener(new OverlayListener<LowerUpper, DoubleKey>() {
			@Override
			public void onReceive(Transport<LowerUpper> trans,
					ReceivedMessage rmsg) {
				// TODO Auto-generated method stub
			}

			@Override
			public void onReceive(
					Overlay<LowerUpper, DoubleKey> overlay,
					OverlayReceivedMessage<DoubleKey> rmsg) {
				send_recv2 = true;
			}

			@Override
			public FutureQueue<?> onReceiveRequest(
					Overlay<LowerUpper, DoubleKey> overlay,
					OverlayReceivedMessage<DoubleKey> rmsg) {
				logger.debug("2.0 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
			}

        });

        ov3.setListener(new OverlayListener<LowerUpper, DoubleKey>() {
			@Override
			public void onReceive(Transport<LowerUpper> trans,
					ReceivedMessage rmsg) {
			}

			@Override
			public void onReceive(
					Overlay<LowerUpper, DoubleKey> overlay,
					OverlayReceivedMessage<DoubleKey> rmsg) {
				send_recv3 = true;
			}

			@Override
			public FutureQueue<?> onReceiveRequest(
					Overlay<LowerUpper, DoubleKey> overlay,
					OverlayReceivedMessage<DoubleKey> rmsg) {
				logger.debug("3.0 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
			}

        });

        ov1.join(loc);
        ov2.join(loc);
        ov3.join(loc);
        Thread.sleep(500);
        ov1.addKey(new DoubleKey(1.0));
        ov2.addKey(new DoubleKey(2.0));
        ov3.addKey(new DoubleKey(3.0));

        DoubleKey k = new DoubleKey(2.5);
        KeyRange<?> range = new KeyRange(
                KeyComparator.getMinusInfinity(DoubleKey.class), false,
                k, true);
        LowerUpper dst = new LowerUpper(range, false, 1);
        List<Object> l = Arrays.asList(ov1.request(dst, "req").getAllValues());

        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);
        
        k = new DoubleKey(3.5);
        range = new KeyRange(
                KeyComparator.getMinusInfinity(DoubleKey.class), false,
                k, true);
        dst = new LowerUpper(range, false, 1);
        l = Arrays.asList(ov1.request(dst, "req").getAllValues());

        received2 = l.contains("recv2");
        received3 = l.contains("recv3");

        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 not received", received3);

        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void FloodTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        Overlay<Destination, Key> tr1, tr2, tr3;
        tr1 = new SimpleFlooding<Destination, Key>(
                        new TransportId("flood"), bt1);
        tr2 = new SimpleFlooding<Destination, Key>(
                        new TransportId("flood"), bt2);
        tr3 = new SimpleFlooding<Destination, Key>(
                        new TransportId("flood"), bt3);
        received2 = false;
        received3 = false;

        tr2.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("flood tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }

        });

        tr3.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("flood tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });

        tr1.join(loc);
        tr2.join(loc);
        tr3.join(loc);
        Thread.sleep(500);
        tr1.addKey(new StringKey("tera"));
        tr2.addKey(new StringKey("ishi"));
        tr3.addKey(new StringKey("yos"));
        Thread.sleep(500);

        // SimpleFlooding fails at first.
        List<Object> l = Arrays.asList(tr2.request(new StringKey("ishi"), "req").getAllValues());
        l = Arrays.asList(tr3.request(new StringKey("ishi"), "req").getAllValues());
        l = Arrays.asList(tr1.request(new StringKey("ishi"),"req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);

        received1 = false;
        received2 = false;
        received3 = false;
        List<Object> l2 = Arrays.asList(tr1.request(new StringKey("yos"), "req").getAllValues());
        received3 = l2.contains("recv3");
        received3 = l2.contains("recv3");
        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 receive failed", received3);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("\"ishi\"", "req");
        Thread.sleep(1000);
        assertTrue("SG2 receive failed", send_recv2);
        assertTrue("SG3 falsely received", !send_recv3);
        
        p1.fin();
        p2.fin();
        p3.fin();
    }

    @Test
    public void CombinedFloodTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        CombinedOverlay tr1, tr2, tr3;
        SimpleFlooding<Destination, Key> f1, f2, f3;

        tr1 = new CombinedOverlay(p1, new TransportId("co"));
        tr2 = new CombinedOverlay(p2, new TransportId("co"));
        tr3 = new CombinedOverlay(p3, new TransportId("co"));

        f1 = new SimpleFlooding<Destination, Key>(new TransportId("flood"), bt1);
        f2 = new SimpleFlooding<Destination, Key>(new TransportId("flood"), bt2);
        f3 = new SimpleFlooding<Destination, Key>(new TransportId("flood"), bt3);

        tr1.declareAttrib("age");
        tr2.declareAttrib("age");
        tr3.declareAttrib("age");

        tr1.bindOverlay("age", new TransportIdPath("flood"));
        tr2.bindOverlay("age", new TransportIdPath("flood"));
        tr3.bindOverlay("age", new TransportIdPath("flood"));

        f1.join(loc);
        f2.join(loc);
        f3.join(loc);
        Thread.sleep(500);

        tr1.newRow(new Id("low1")).setAttrib("age", 5);
        tr2.newRow(new Id("low2")).setAttrib("age", 4);
        tr3.newRow(new Id("low3")).setAttrib("age", 3);

        received2 = false;
        received3 = false;

        tr2.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("comb tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }

        });

        tr3.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("comb tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }
            
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }

        });

        DCLTranslator parser = new DCLTranslator();
        DestinationCondition dst = parser.parseDCL("age in (3..)");
        
        tr2.request(dst, "req");
        tr1.request(dst, "req");
        List<Object> l = Arrays.asList(tr3.request(dst, "req").getAllValues());

        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("age in (..3]", "req");
        Thread.sleep(1000);
        assertTrue("SG2 falsely received", !send_recv2);
        assertTrue("SG3 receive failed", send_recv3);

        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void CombinedComblexTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        CombinedOverlay tr1, tr2, tr3;
        MSkipGraph<Destination, ComparableKey<?>> m1, m2, m3;

        tr1 = new CombinedOverlay(p1, new TransportId("co"));
        tr2 = new CombinedOverlay(p2, new TransportId("co"));
        tr3 = new CombinedOverlay(p3, new TransportId("co"));

        m1 = new MSkipGraph<Destination, ComparableKey<?>>(new TransportId("mskip"), bt1);
        m2 = new MSkipGraph<Destination, ComparableKey<?>>(new TransportId("mskip"), bt2);
        m3 = new MSkipGraph<Destination, ComparableKey<?>>(new TransportId("mskip"), bt3);
        
        LLNet l1 = new LLNet(new TransportId("llnet"), m1);
        LLNet l2 = new LLNet(new TransportId("llnet"), m2);
        LLNet l3 = new LLNet(new TransportId("llnet"), m3);
        
        DOLR<Key> d1 =  new DOLR<Key>(new TransportId("dolr"), m1);
        DOLR<Key> d2 =  new DOLR<Key>(new TransportId("dolr"), m2);
        DOLR<Key> d3 =  new DOLR<Key>(new TransportId("dolr"), m3);

        tr1.declareAttrib("age", Integer.class);
        tr2.declareAttrib("age", Integer.class);
        tr3.declareAttrib("age", Integer.class);
        
        tr1.declareAttrib("home_loc", Location.class);
        tr2.declareAttrib("home_loc", Location.class);
        tr3.declareAttrib("home_loc", Location.class);
        
        tr1.declareAttrib("name", String.class);
        tr2.declareAttrib("name", String.class);
        tr3.declareAttrib("name", String.class);
        
        tr1.declareAttrib("hobby", String.class);
        tr2.declareAttrib("hobby", String.class);
        tr3.declareAttrib("hobby", String.class);

        tr1.bindOverlay("age", new TransportIdPath("mskip"));
        tr2.bindOverlay("age", new TransportIdPath("mskip"));
        tr3.bindOverlay("age", new TransportIdPath("mskip"));

        tr1.bindOverlay("home_loc", new TransportIdPath("llnet"));
        tr2.bindOverlay("home_loc", new TransportIdPath("llnet"));
        tr3.bindOverlay("home_loc", new TransportIdPath("llnet"));
        
        tr1.bindOverlay("name", new TransportIdPath("dolr"));
        tr2.bindOverlay("name", new TransportIdPath("dolr"));
        tr3.bindOverlay("name", new TransportIdPath("dolr"));

        RowData r1 = tr1.newRow(new Id("low1"));
        r1.setAttrib("age", 5, true);
        r1.setAttrib("home_loc", new Location(130, 30));
        r1.setAttrib("name", "tera", true);
        r1.setAttrib("hobby", "baseball");
        
        RowData r2 = tr2.newRow(new Id("low1"));
        r2.setAttrib("age", 6, true);
        r2.setAttrib("home_loc", new Location(131, 31));
        r2.setAttrib("name", "yos", true);
        r2.setAttrib("hobby", "programming");
        
        RowData r3 = tr3.newRow(new Id("low1"));
        r3.setAttrib("age", 7, true);
        r3.setAttrib("home_loc", new Location(131, 31));
        r3.setAttrib("name", "ishi", true);
        r3.setAttrib("hobby", "ski");
        
        l1.join(loc);
        l2.join(loc);
        l3.join(loc);
        
        d1.join(loc);
        d2.join(loc);
        d3.join(loc);
        
        Thread.sleep(500);

        received2 = false;
        received3 = false;

        tr2.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("comb tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }

        });

        tr3.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("comb tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }

        });

        List<Object> l = Arrays.asList(tr1.request("age in (..7)", "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);
        
        received2 = false;
        received3 = false;
        
        l = Arrays.asList(tr1.request("name eq \"ishi\"", "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 receive failed", received3);

        received2 = false;
        received3 = false;
                        
        l = Arrays.asList(tr1.request("age in (3..8) and hobby eq \"ski\"", "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG3 receive failed", received3);
        assertTrue("SG2 falsely received", !received2);

        send_recv2 = false;
        send_recv3 = false;
        tr1.send("age in (3..8) and hobby eq \"ski\"", "req");
        Thread.sleep(1000);
        assertTrue("SG2 falsely received", !send_recv2);
        assertTrue("SG3 receive failed", send_recv3);
      
        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void CombinedComblexOnSuzakuTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));

        // app id
        ObjectId appId = new ObjectId("app");

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", portNumber++)));

        // top level
        CombinedOverlay co1, co2, co3;
        Suzaku<Destination, ComparableKey<?>> m1, m2, m3;

        co1 = new CombinedOverlay(p1, new TransportId("co"));
        co2 = new CombinedOverlay(p2, new TransportId("co"));
        co3 = new CombinedOverlay(p3, new TransportId("co"));

        m1 = new Suzaku<Destination, ComparableKey<?>>(new TransportId("cs"), bt1);
        m2 = new Suzaku<Destination, ComparableKey<?>>(new TransportId("cs"), bt2);
        m3 = new Suzaku<Destination, ComparableKey<?>>(new TransportId("cs"), bt3);
        
        LLNet l1 = new LLNet(m1);
        LLNet l2 = new LLNet(m2);
        LLNet l3 = new LLNet(m3);
        
        DOLR<Key> d1 =  new DOLR<Key>(m1);
        DOLR<Key> d2 =  new DOLR<Key>(m2);
        DOLR<Key> d3 =  new DOLR<Key>(m3);

        co1.declareAttrib("age", Integer.class);
        co2.declareAttrib("age", Integer.class);
        co3.declareAttrib("age", Integer.class);
        
        co1.declareAttrib("home_loc", Location.class);
        co2.declareAttrib("home_loc", Location.class);
        co3.declareAttrib("home_loc", Location.class);
        
        co1.declareAttrib("name", String.class);
        co2.declareAttrib("name", String.class);
        co3.declareAttrib("name", String.class);
        
        co1.declareAttrib("hobby", String.class);
        co2.declareAttrib("hobby", String.class);
        co3.declareAttrib("hobby", String.class);

        co1.bindOverlay("age", new TransportIdPath("cs"));
        co2.bindOverlay("age", new TransportIdPath("cs"));
        co3.bindOverlay("age", new TransportIdPath("cs"));

        co1.bindOverlay("home_loc", new TransportIdPath("llnet"));
        co2.bindOverlay("home_loc", new TransportIdPath("llnet"));
        co3.bindOverlay("home_loc", new TransportIdPath("llnet"));
        
        co1.bindOverlay("name", new TransportIdPath("dolr"));
        co2.bindOverlay("name", new TransportIdPath("dolr"));
        co3.bindOverlay("name", new TransportIdPath("dolr"));

        RowData r1 = co1.newRow(new Id("low1"));
        r1.setAttrib("age", 5, true);
        r1.setAttrib("home_loc", new Location(130, 30));
        r1.setAttrib("name", "tera", true);
        r1.setAttrib("hobby", "baseball");
        
        RowData r2 = co2.newRow(new Id("low1"));
        r2.setAttrib("age", 6, true);
        r2.setAttrib("home_loc", new Location(131, 31));
        r2.setAttrib("name", "yos", true);
        r2.setAttrib("hobby", "programming");
        
        RowData r3 = co3.newRow(new Id("low1"));
        r3.setAttrib("age", 7, true);
        r3.setAttrib("home_loc", new Location(131, 31));
        r3.setAttrib("name", "ishi", true);
        r3.setAttrib("hobby", "ski");
        
        l1.join(loc);
        l2.join(loc);
        l3.join(loc);
        
        d1.join(loc);
        d2.join(loc);
        d3.join(loc);
        
        Thread.sleep(500);

        received2 = false;
        received3 = false;

        co2.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv2 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("comb tr2 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv2");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });

        co3.setListener(new OverlayListener<Destination, Key>() {
            public void onReceive(Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                send_recv3 = true;
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, Key> overlay,
                    OverlayReceivedMessage<Key> rmsg) {
                logger.debug("comb tr3 matched:" + rmsg.getMatchedKeys());
                return overlay.singletonFutureQueue("recv3");
            }
            
            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });

        List<Object> l = Arrays.asList(co1.request("age in (..7)", "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG2 receive failed", received2);
        assertTrue("SG3 falsely received", !received3);
        
        received2 = false;
        received3 = false;
        
        l = Arrays.asList(co1.request("name eq \"ishi\"", "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG2 falsely received", !received2);
        assertTrue("SG3 receive failed", received3);

        received2 = false;
        received3 = false;
                        
        l = Arrays.asList(co1.request("age in (3..8) and hobby eq \"ski\"", "req").getAllValues());
        received2 = l.contains("recv2");
        received3 = l.contains("recv3");
        assertTrue("SG3 receive failed", received3);
        assertTrue("SG2 falsely received", !received2);

        send_recv2 = false;
        send_recv3 = false;
        co1.send("age in (3..8) and hobby eq \"ski\"", "req");
        Thread.sleep(1000);
        assertTrue("SG2 falsely received", !send_recv2);
        assertTrue("SG3 receive failed", send_recv3);
      
        p1.fin();
        p2.fin();
        p3.fin();
    }

    @Test
    public void SuzakuRoutingTableTest() throws Exception {
        int numOfPeers = 32;
        // get peers
        PeerLocator loc = null;
        Suzaku<Destination, ComparableKey<?>> trs[] = new Suzaku[numOfPeers];
        Peer peers[] = new Peer[numOfPeers];
        for (int i = 0; i < numOfPeers; i++) {
            PeerLocator l = new NettyLocator(new InetSocketAddress(
                    "localhost", 12367 + i));
            if (loc == null) {
                loc = l; // remember for seed;
            }
            trs[i] = new Suzaku<Destination, ComparableKey<?>>((peers[i] = Peer
                    .getInstance(new PeerId("p" + i)))
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
                        // TODO Auto-generated catch block
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
        logger.debug("sleeping 25 sec...");
        Thread.sleep(25000);

        for (int i = 0; i < numOfPeers; i++) {
            logger.debug("size=" + trs[i].getAll().length);
            logger.debug("height=" + trs[i].getHeight(new DoubleKey((double)i)));
            for (int j = 0; j < trs[i].getHeight(new DoubleKey((double)i)); j++) {
                int rkeys = trs[i].getRights(new DoubleKey((double)i), j).length;
                for (int k = 0; k < rkeys; k++) {
                    logger.debug("rights" + trs[i].getLocal(new DoubleKey((double) i)).key + "[" + j + "]=" + trs[i].getRights(new DoubleKey((double)i), j)[k].key);
                }
                int lkeys = trs[i].getLefts(new DoubleKey((double)i), j).length;
                for (int k = 0; k < lkeys; k++) {
                    logger.debug("lefts" + trs[i].getLocal(new DoubleKey((double) i)).key + "[" + j + "]=" + trs[i].getLefts(new DoubleKey((double)i), j)[k].key);
                }
            }
            if (i != numOfPeers - 1) { 
                assertTrue(((DoubleKey)(trs[i].getRight(new DoubleKey((double)i)).key.getPrimaryKey())).getKey() == (double)i + 1);
            }
            if (i != 0) { 
                assertTrue(((DoubleKey)(trs[i].getLeft(new DoubleKey((double)i)).key.getPrimaryKey())).getKey() == (double)i - 1);
            }
        }
        
        } finally {
        for (int i = 0; i < numOfPeers; i++) {
            peers[i].fin();
        }
        }
    }
    
    @Test
    public void MSGRoutingTableTest() throws Exception {
        int numOfPeers = 32;
        // get peers
        PeerLocator loc = null;
        MSkipGraph<Destination, ComparableKey<?>> trs[] = new MSkipGraph[numOfPeers];
        Peer peers[] = new Peer[numOfPeers];
        for (int i = 0; i < numOfPeers; i++) {
            PeerLocator l = new NettyLocator(new InetSocketAddress(
                    "localhost", 12367 + i));
            if (loc == null) {
                loc = l; // remember for seed;
            }
            trs[i] = new MSkipGraph<Destination, ComparableKey<?>>((peers[i] = Peer
                    .getInstance(new PeerId("p" + i)))
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
                        // TODO Auto-generated catch block
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
            logger.debug("height=" + trs[i].getHeight(new DoubleKey((double)i)));
            for (int j = 0; j < trs[i].getHeight(new DoubleKey((double)i)); j++) {
                int rkeys = trs[i].getRights(new DoubleKey((double)i), j).length;
                for (int k = 0; k < rkeys; k++) {
                    logger.debug("rights" + trs[i].getLocal(new DoubleKey((double) i)).key + "[" + j + "]=" + trs[i].getRights(new DoubleKey((double)i), j)[k].key);
                }
                int lkeys = trs[i].getLefts(new DoubleKey((double)i), j).length;
                for (int k = 0; k < lkeys; k++) {
                    logger.debug("lefts" + trs[i].getLocal(new DoubleKey((double) i)).key + "[" + j + "]=" + trs[i].getLefts(new DoubleKey((double)i), j)[k].key);                    
                }
            }
            if (i != numOfPeers - 1) { 
                assertTrue(((DoubleKey)(trs[i].getRight(new DoubleKey((double)i)).key.getPrimaryKey())).getKey() == (double)i + 1);
            }
            if (i != 0) { 
                assertTrue(((DoubleKey)(trs[i].getLeft(new DoubleKey((double)i)).key.getPrimaryKey())).getKey() == (double)i - 1);
            }
        }
        } finally {
        for (int i = 0; i < numOfPeers; i++) {
            peers[i].fin();
        }
        }
    }
}
