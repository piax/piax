package test.trans;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.dcl.DCLTranslator;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.Lower;
import org.piax.common.subspace.LowerUpper;
import org.piax.common.wrapper.DoubleKey;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.Peer;
import org.piax.gtrans.PeerLocator;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.impl.BaseTransportMgr;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.suzaku.Suzaku;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.util.FailureSimulationChannelTransport;
import org.piax.gtrans.util.ThroughTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import test.Util;
import test.Util.Net;

public class TestTransport {
	
    boolean udp_received1, udp_received2;
    private static final Logger logger = 
            LoggerFactory.getLogger(TestTransport.class);
    
    @BeforeAll
    public static void setup() {
        BaseTransportMgr.BASE_TRANSPORT_MANAGER_CLASS.set("org.piax.gtrans.impl.DefaultBaseTransportGenerator");
    }
    
    @Test
    public void UDPTransportTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // top level
        Transport<UdpLocator> tr1 = p1.newBaseTransport(
        				new UdpLocator(
        						new InetSocketAddress("localhost", 12367)));
        
        Transport<UdpLocator> tr2 = p2.newBaseTransport(
        				new UdpLocator(
        						new InetSocketAddress("localhost", 12368)));

        udp_received1 = false;
        udp_received2 = false;
                
        tr1.setListener(new TransportListener<UdpLocator>() {
            public void onReceive(Transport<UdpLocator> trans, ReceivedMessage msg) {
                udp_received1 = "654321".equals(msg.getMessage());
            }
        });
        
        tr2.setListener(new TransportListener<UdpLocator>() {
            public void onReceive(Transport<UdpLocator> trans, ReceivedMessage msg) {
                udp_received2 = "123456".equals(msg.getMessage());
            }
        });

        tr1.send((UdpLocator) tr2.getEndpoint(), "123456");
        tr2.send((UdpLocator) tr1.getEndpoint(), "654321");

        // Channel ch = tr1.newChannel(tr2.getMyEndpoint());
        // ch.send("abcdefg".getBytes());

        // ReceivedMessage b = ch.receive(1000);
        // ch.close();
        Thread.sleep(1000);

        p1.fin();
        p2.fin();
        assertTrue(udp_received1, "UDP1 receive failed");
        assertTrue(udp_received2, "UDP2 receive failed");
        // assertTrue(ByteUtil.equals(b.getMessage(), "abcdefg".getBytes()));
    }
    
    @Test
    public void IdChannelTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // top level
        ChannelTransport<PrimaryKey> tr1 = p1.newBaseChannelTransport(
                Util.genEndpoint(Net.ID, p1.getPeerId(),
                                "localhost", 12367));
        ChannelTransport<PrimaryKey> tr2 = p2.newBaseChannelTransport(
                Util.genEndpoint(Net.ID, p2.getPeerId(),
                                "localhost", 12368));

        tr1.setChannelListener(new ChannelListener<PrimaryKey>() {
            public boolean onAccepting(Channel<PrimaryKey> channel) {
                return true;
            }

            public void onClosed(Channel<PrimaryKey> channel) {
            }

            public void onFailure(Channel<PrimaryKey> channel, Exception cause) {
            }

            public void onReceive(Channel<PrimaryKey> ch) {
                if (ch.isCreatorSide())
                    return;
                Object msg = ch.receive();
                try {
                    ch.send(msg);
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });
        
        Channel<PrimaryKey> c = tr2.newChannel(tr1.getEndpoint());
        c.send("654321");
        String mes = (String) c.receive(1000);
        assertTrue(mes.equals("654321"));
        c.close();

        p1.fin();
        p2.fin();
    }

    @Test
    public void UDPTransportChannelTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // top level
        ChannelTransport<UdpLocator> tr1 = p1.newBaseChannelTransport(
                        new UdpLocator(
                                new InetSocketAddress("localhost", 12367)));
        ChannelTransport<UdpLocator> tr2 = p2.newBaseChannelTransport(
                        new UdpLocator(
                                new InetSocketAddress("localhost", 12368)));

        tr1.setChannelListener(new ChannelListener<UdpLocator>() {
            public boolean onAccepting(Channel<UdpLocator> channel) {
                return true;
            }

            public void onClosed(Channel<UdpLocator> channel) {
            }

            public void onFailure(Channel<UdpLocator> channel, Exception cause) {
            }

            public void onReceive(Channel<UdpLocator> ch) {
                if (ch.isCreatorSide())
                    return;
                Object msg = ch.receive();
                try {
                    ch.send(msg);
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });
        
        Channel<UdpLocator> c = tr2.newChannel(tr1.getEndpoint());
        c.send("654321");
        String mes = (String) c.receive(1000);
        assertTrue(mes.equals("654321"));
        c.close();

        p1.fin();
        p2.fin();
    }
    
    boolean sg_received1, sg_received2, sg_received3;

    @Test
    public void SGOnUdpTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        //Overlay<ComparableKey<?>, ComparableKey<?>> ov1, ov2;

        Overlay<ComparableKey<?>, ComparableKey<?>> tr1 = 
        		new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt1);
        Overlay<ComparableKey<?>, ComparableKey<?>> tr2 =
        		new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt2);
  
    /*    
        TopLevelTransport<ComparableKey<?>> tr1 = new TopLevelTransport<ComparableKey<?>>(
        		TopLevelTransport.DEFAULT_APP_ID, ov1 = new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt1));
        TopLevelTransport<ComparableKey<?>> tr2 = new TopLevelTransport<ComparableKey<?>>(
        		TopLevelTransport.DEFAULT_APP_ID, ov2 = new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt2));
  */      
        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("udp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("udp recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    logger.debug("" + trans.getTransportIdPath());
                    trans.send(new DoubleKey(Double.parseDouble(
                    		(String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        logger.debug("" + tr2.getTransportIdPath());
        tr2.send(new DoubleKey(1.0), "recv");
        tr1.send(new DoubleKey(2.0), key.getKey().toString());
        //tr1.send(new DoubleKey(1.0), "recv");
//        tr2.send(new Ranges<Double>(new Range<Double>(1.0)), "recv");
//      tr1.send(new DestinationDescription("[2.0..2.0]"));
//        tr2.send(new Ranges<Integer>(new Range<Integer>(1)),
//                "654321".getBytes());
        
        Thread.sleep(100);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        sg_received1 = false;
        sg_received2 = false;
        tr1.send("[2.0..2.0]", "1.0");
        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        sg_received1 = false;
        sg_received2 = false;

        p1.fin();
        p2.fin();
    }

    @Test
    public void SGOnTcpTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (TCP)
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new TcpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new TcpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<ComparableKey<?>, ComparableKey<?>> tr1 = 
                	new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt1);
        Overlay<ComparableKey<?>, ComparableKey<?>> tr2 = 
                	new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt2);

        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("tcp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("tcp recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        tr1.send(new DoubleKey(2.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }

    @Test
    public void SGOnEmuTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (Emu)
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new EmuLocator(12367));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new EmuLocator(12368));

        // top level
        
        Overlay<ComparableKey<?>, ComparableKey<?>> tr1 = new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt1);
        Overlay<ComparableKey<?>, ComparableKey<?>> tr2 = new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(bt2);

        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("emu recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("emu recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        tr1.send(new DoubleKey(2.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }
    
    @Test
    public void SGTransportTestDirectReply() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<ComparableKey<?>, ComparableKey<?>> ov1, ov2;
        ov1 = new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(
                        new TransportId("mskip"), bt1);
        
        ov2 = new MSkipGraph<ComparableKey<?>, ComparableKey<?>>(
                        new TransportId("mskip"), bt2);

        sg_received1 = false;
        sg_received2 = false;

        ov1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("udp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        ov2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("udp recv2:" + rmsg.getMessage());
                    sg_received2 = rmsg.getMessage().equals("12345");
                    trans.send((PeerId) rmsg.getSource(), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = ov1.join(loc);
        boolean succ2 = ov2.join(loc);
        Thread.sleep(500);
        boolean succ3 = ov1.addKey(new DoubleKey(1.0));
        boolean succ4 = ov2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        ov1.send(new DoubleKey(2.0), "12345");

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }

    int response_count = 0;
    
    @Test
    public void SGOverlayRangecastTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12369)));

        // top level
        Overlay<Destination, ComparableKey<?>> tr1, tr2, tr3;
        tr1 = new MSkipGraph<Destination, ComparableKey<?>>(bt1);
        tr2 = new MSkipGraph<Destination, ComparableKey<?>>(bt2);
        tr3 = new MSkipGraph<Destination, ComparableKey<?>>(bt3);

        sg_received1 = false;
        sg_received2 = false;
        sg_received3 = false;
        response_count = 0;

        tr1.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                if (rmsg.getMessage().equals("recv")) {
                    response_count++;
                }
                sg_received1 = (response_count == 2);
            }

            public FutureQueue<?> onReceiveRequest(Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return null;
            }

            // unused
            public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
            }
        });
        
        tr2.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                try {
                    sg_received2 = true;
                    overlay.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }

            public FutureQueue<?> onReceiveRequest(Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return null;
            }

            // unused
            public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
            }
        });
        
        tr3.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                try {
                    sg_received3 = true;
                    overlay.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }

            public FutureQueue<?> onReceiveRequest(Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return null;
            }

            // unused
            public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
            }
        });

        tr1.join(loc);
        tr2.join(loc);
        tr3.join(loc);
        Thread.sleep(500);
        tr1.addKey(new DoubleKey(1.0));
        tr2.addKey(new DoubleKey(2.0));
        tr3.addKey(new DoubleKey(3.0));
        Thread.sleep(500);
        
        DoubleKey key = null;
        for (Object obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        tr1.send(new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString());

        Thread.sleep(3000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received3, "SG3 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        p2.fin();
        p3.fin();
        p1.fin();
    }
    
    @Test
    public void SGOverlayRangeRequestTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12369)));

        // top level
        Overlay<Destination, ComparableKey<?>> tr1, tr2, tr3;
        tr1 = new MSkipGraph<Destination, ComparableKey<?>>(bt1);
        tr2 = new MSkipGraph<Destination, ComparableKey<?>>(bt2);
        tr3 = new MSkipGraph<Destination, ComparableKey<?>>(bt3);

        sg_received2 = false;
        sg_received3 = false;
        
        tr2.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return overlay.singletonFutureQueue("recv2");
            }

            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });
        
        tr3.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
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
        tr1.addKey(new DoubleKey(1.0));
        tr2.addKey(new DoubleKey(2.0));
        tr3.addKey(new DoubleKey(3.0));
        Thread.sleep(500);
        
        DoubleKey key = null;
        for (Object obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        List<Object> l = Arrays.asList(tr1.request(
                new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received3, "SG3 receive failed");

        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void CSOnUdpTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<ComparableKey<?>, ComparableKey<?>> tr1 = 
                new Suzaku<ComparableKey<?>, ComparableKey<?>>(
                        bt1);
        Overlay<ComparableKey<?>, ComparableKey<?>> tr2 = 
                new Suzaku<ComparableKey<?>, ComparableKey<?>>(
                        bt2);

        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("udp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("udp recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        long start = System.currentTimeMillis();
        tr1.send(new DoubleKey(2.0), key.getKey().toString());
        logger.debug("send took:" + (System.currentTimeMillis() - start) + "(ms)");
        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        start = System.currentTimeMillis();
        tr1.send(new DoubleKey(2.0), key.getKey().toString(), new TransOptions(RetransMode.NONE));
        logger.debug("send/no-retrans took:" + (System.currentTimeMillis() - start) + "(ms)");
        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        
        start = System.currentTimeMillis();
        tr1.send(new DoubleKey(2.0), key.getKey().toString(), new TransOptions(RetransMode.FAST));
        logger.debug("send/fast-retrans took:" + (System.currentTimeMillis() - start) + "(ms)");
        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        
        start = System.currentTimeMillis();
        tr1.request(new DoubleKey(2.0), key.getKey().toString());
        logger.debug("send-sync (request) took:" + (System.currentTimeMillis() - start) + "(ms)");
//        tr2.send(new Ranges<Double>(new Range<Double>(1.0)), "recv");
//        tr1.send(new DestinationDescription("[2.0..2.0]"));
//        tr2.send(new Ranges<Integer>(new Range<Integer>(1)),
//                "654321".getBytes());
        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
        
        p1.fin();
        p2.fin();
    }

    @Test
    public void CSOnTcpTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (TCP)
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new TcpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new TcpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<ComparableKey<?>, ComparableKey<?>> tr1 = 
                new Suzaku<ComparableKey<?>, ComparableKey<?>>(bt1);
        Overlay<ComparableKey<?>, ComparableKey<?>> tr2 = 
                new Suzaku<ComparableKey<?>, ComparableKey<?>>(bt2);

        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("tcp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("tcp recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        tr1.send(new DoubleKey(2.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }

    @Test
    public void DifferentObjectIDTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (TCP)
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new TcpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new TcpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<ComparableKey<?>, ComparableKey<?>> tr1 = new Suzaku<>(bt1);
        Overlay<ComparableKey<?>, ComparableKey<?>> tr2 = new Suzaku<>(bt2);

        sg_received1 = false;
        sg_received2 = false;
        
        ObjectId oid = new ObjectId("app");
        ObjectId oid2 = new ObjectId("app2");

        tr1.setListener(oid, new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("tcp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(oid, new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("tcp recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(oid, new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(oid2, new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(oid2, new DoubleKey(2.0));

        assertTrue(succ1, "ov1 join failed");
        assertTrue(succ2, "ov2 join failed");
        assertTrue(succ3, "ov1 addKey failed");
        assertTrue(succ4, "ov2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        tr1.send(oid2, new DoubleKey(2.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(!sg_received2, "ov2 falsely received");
        assertTrue(!sg_received1, "ov1 falsely received");

        succ3 = tr1.addKey(oid, new DoubleKey(1.0));
        succ4 = tr2.addKey(oid, new DoubleKey(2.0));
        
        assertTrue(succ3, "ov1 addKey failed");
        assertTrue(succ4, "ov2 addKey failed");
        Thread.sleep(500);
        
        tr1.send(oid, new DoubleKey(2.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(sg_received2, "ov2 receive failed");
        assertTrue(sg_received1, "ov1 receive failed");
        
        p1.fin();
        p2.fin();
    }

    @Test
    public void CSOnEmuTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (Emu)
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new EmuLocator(12367));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new EmuLocator(12368));

        // top level
        Overlay<ComparableKey<?>, ComparableKey<?>> ov1, ov2;
        ov1 = new Suzaku<ComparableKey<?>, ComparableKey<?>>(
                        new TransportId("mskip"), bt1);
        ov2 = new Suzaku<ComparableKey<?>, ComparableKey<?>>(
                        new TransportId("mskip"), bt2);

        sg_received1 = false;
        sg_received2 = false;

        ov1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("emu recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        ov2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("emu recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = ov1.join(loc);
        boolean succ2 = ov2.join(loc);
        Thread.sleep(500);
        boolean succ3 = ov1.addKey(new DoubleKey(1.0));
        boolean succ4 = ov2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        DoubleKey key = null;
        for (ComparableKey<?> obj : ov1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        ov1.send(new DoubleKey(2.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }
    
    @Test
    public void IdTransTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (Emu)
        Endpoint ep;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                ep = new PrimaryKey(new DoubleKey(0.0), new NettyLocator("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new PrimaryKey(new DoubleKey(1.0), new NettyLocator("localhost", 12368)));

        // top level
        Overlay<DoubleKey, DoubleKey> ov1, ov2;
        ov1 = new Suzaku<DoubleKey, DoubleKey>(
                        new TransportId("mskip"), bt1);
        ov2 = new Suzaku<DoubleKey, DoubleKey>(
                        new TransportId("mskip"), bt2);

        sg_received1 = false;
        sg_received2 = false;

        ov1.setListener((trans, rmsg)-> {
                logger.debug("emu recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
        });

        ov2.setListener((trans, rmsg)-> {
                try {
                    logger.debug("emu recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
        });

        boolean succ1 = ov1.join(ep);
        boolean succ2 = ov2.join(ep);

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");

        DoubleKey key = null;
        for (ComparableKey<?> obj : ov1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        ov1.send(new DoubleKey(1.0), key.getKey().toString());

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }

    @Test
    public void LowerIdTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        Endpoint ep;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                ep = new PrimaryKey(new DoubleKey(0.0), new NettyLocator("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new PrimaryKey(new DoubleKey(0.5), new NettyLocator("localhost", 12368)));

        // top level
        Overlay<LowerUpper, DoubleKey> ov1, ov2;
        ov1 = new Suzaku<LowerUpper, DoubleKey>(
                        new TransportId("mskip"), bt1);
        ov2 = new Suzaku<LowerUpper, DoubleKey>(
                        new TransportId("mskip"), bt2);

        sg_received1 = false;
        sg_received2 = false;

        ov1.setListener((trans, rmsg) -> {
                logger.debug("emu recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
        });

        ov2.setListener((trans, rmsg) -> {
                try {
                    logger.debug("emu recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new Lower<DoubleKey>(false, new DoubleKey(0.1), 1), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
        });
    try {

        boolean succ1 = ov1.join(ep);
        boolean succ2 = ov2.join(ep);

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        Thread.sleep(500);
        ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.6), 1), "data");
        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");
    }
    finally {
        p1.fin();
        p2.fin();
    }
    }    

    @Test
    public void CSTransportTestDirectReply() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<ComparableKey<?>, DoubleKey> tr1 = 
                new Suzaku<ComparableKey<?>, DoubleKey>(bt1);
        Overlay<ComparableKey<?>, DoubleKey> tr2 = 
                new Suzaku<ComparableKey<?>, DoubleKey>(bt2);

        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                logger.debug("udp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<ComparableKey<?>>() {
            public void onReceive(Transport<ComparableKey<?>> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("udp recv2:" + rmsg.getMessage());
                    sg_received2 = rmsg.getMessage().equals("12345");
                    trans.send((PeerId) rmsg.getSource(), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new DoubleKey(1.0));
        boolean succ4 = tr2.addKey(new DoubleKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        tr1.send(new DoubleKey(2.0), "12345");

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }

    @Test
    public void CSOverlayRangecastTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));
        
        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12369)));

        // top level
        Overlay<Destination, ComparableKey<?>> tr1, tr2, tr3;
        tr1 = new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mskip"), bt1);
        tr2 = new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mskip"), bt2);
        tr3 = new Suzaku<Destination, ComparableKey<?>>(
                        new TransportId("mskip"), bt3);

        sg_received1 = false;
        sg_received2 = false;
        sg_received3 = false;
        response_count = 0;

        tr1.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                logger.debug("udp recv1:" + rmsg.getMessage());
                if (rmsg.getMessage().equals("recv")) {
                    response_count++;
                }
                sg_received1 = (response_count == 2);
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return null;
            }

            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });
        
        tr2.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                try {
                    sg_received2 = true;
                    overlay.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return null;
            }

            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });
        
        tr3.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                try {
                    sg_received3 = true;
                    overlay.send(new DoubleKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return null;
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
        tr1.addKey(new DoubleKey(1.0));
        tr2.addKey(new DoubleKey(2.0));
        tr3.addKey(new DoubleKey(3.0));
        Thread.sleep(500);
        
        DoubleKey key = null;
        for (Object obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        tr1.send(new DCLTranslator().parseDestination("[2.0..3.0]"), key.getKey().toString());
        //tr1.send(new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
        //                key.getKey().toString());

        Thread.sleep(2000);
        assertTrue(sg_received1, "SG2 receive failed");
        assertTrue(sg_received3, "SG3 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
        p3.fin();
    }
    
    @Test
    public void CSOverlayRangeRequestTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));
        

        // base transport
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new UdpLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12368)));
        ChannelTransport<?> bt3 = p3.newBaseChannelTransport(
                new UdpLocator(new InetSocketAddress("localhost", 12369)));

        // top level
        Overlay<Destination, ComparableKey<?>> tr1, tr2, tr3;
        tr1 = new Suzaku<Destination, ComparableKey<?>>(
                        bt1);
        tr2 = new Suzaku<Destination, ComparableKey<?>>(
                        bt2);
        tr3 = new Suzaku<Destination, ComparableKey<?>>(
                        bt3);

        sg_received2 = false;
        sg_received3 = false;
        
        tr2.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return overlay.singletonFutureQueue("recv2");
            }

            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });
        
        tr3.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
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
        tr1.addKey(new DoubleKey(1.0));
        tr2.addKey(new DoubleKey(2.0));
        tr3.addKey(new DoubleKey(3.0));
        Thread.sleep(500);
        
        DoubleKey key = null;
        for (Object obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        List<Object> l = Arrays.asList(tr1.request(
                new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received3, "SG3 receive failed");

        p1.fin();
        p2.fin();
        p3.fin();
    }
    

	@Test
    public void UDPTransportFailureChannelTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        FailureSimulationChannelTransport<UdpLocator> ft;
        
        Transport<UdpLocator> bt1 = p1.newBaseTransport(
                new UdpLocator(
                        new InetSocketAddress("localhost", 12367)));
        Transport<UdpLocator> bt2 = p2.newBaseTransport(
				new UdpLocator(
						new InetSocketAddress("localhost", 12368)));
        // top level
        ChannelTransport<UdpLocator> tr1 = 
                ft = new FailureSimulationChannelTransport<UdpLocator>(bt1);
        
        ChannelTransport<UdpLocator> tr2 =
                new FailureSimulationChannelTransport<UdpLocator>(bt2);

        tr1.setChannelListener(new ChannelListener<UdpLocator>() {
            public boolean onAccepting(Channel<UdpLocator> channel) {
                return true;
            }

            public void onClosed(Channel<UdpLocator> channel) {
            }

            public void onFailure(Channel<UdpLocator> channel, Exception cause) {
            }

            public void onReceive(Channel<UdpLocator> ch) {
                if (ch.isCreatorSide())
                    return;
                Object msg = ch.receive();
                try {
                    ch.send(msg);
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });
        ft.setErrorRate(0);
        ft.upsetTransport();
        
        Channel<UdpLocator> c = tr2.newChannel(tr1.getEndpoint());
        c.send("654321");
        String mes = (String) c.receive(1000);
        assertTrue(mes.equals("654321"));
        c.close();
        
        ft.setErrorRate(100);
        try {
        	c = tr2.newChannel(tr1.getEndpoint());
        	c.send("654321");
        	mes = (String) c.receive(1000);
        }
        catch (Exception e) {
        	mes = null;
        }
        assertTrue(mes == null);
        c.close();

        p1.fin();
        p2.fin();
    }
	
//	@Test
    public void CSOnFailureSimulationChannelTest() throws Exception {
    	// get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));
        
        PeerLocator loc;
        
        FailureSimulationChannelTransport<UdpLocator> ft;
        
        // top level
        Overlay<Destination, ComparableKey<?>> tr1, tr2, tr3;
        tr1 = new Suzaku<Destination, ComparableKey<?>>(
        				ft = new FailureSimulationChannelTransport<UdpLocator>(
        						p1.newBaseChannelTransport(loc = new UdpLocator(new InetSocketAddress("localhost", 12367)))));
        
        tr2 = new Suzaku<Destination, ComparableKey<?>>(
                        new FailureSimulationChannelTransport<UdpLocator>(
                        		p2.newBaseTransport(new UdpLocator(new InetSocketAddress("localhost", 12368)))));
        
        tr3 = new Suzaku<Destination, ComparableKey<?>>(
                        new FailureSimulationChannelTransport<UdpLocator>(
                        		p3.newBaseChannelTransport(new UdpLocator(new InetSocketAddress("localhost", 12369)))));

        sg_received2 = false;
        sg_received3 = false;
        
        tr2.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return overlay.singletonFutureQueue("recv2");
            }

            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });
        
        tr3.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
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
        tr1.addKey(new DoubleKey(1.0));
        tr2.addKey(new DoubleKey(2.0));
        tr3.addKey(new DoubleKey(3.0));
        Thread.sleep(500);
        
        DoubleKey key = null;
        for (Object obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        
        ft.setErrorRate(100);
        ft.upsetTransport();
        
        List<Object> l = Arrays.asList(tr1.request(
                new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        assertTrue(!sg_received2, "SG2 received falsely");
        assertTrue(!sg_received3, "SG3 received falsely");
        
        ft.setErrorRate(100);
        l = Arrays.asList(tr1.request(
                new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        assertTrue(!sg_received2, "SG2 received falsely");
        assertTrue(!sg_received3, "SG3 received falsely");
        
        p1.fin();
        p2.fin();
        p3.fin();    	
	}
	
//	@Test
    public void CSRunsOnFailureSimulationChannelTest() throws Exception {
    	// get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        Peer p3 = Peer.getInstance(new PeerId("p3"));
        
        PeerLocator loc;

        FailureSimulationChannelTransport<UdpLocator> ft;
        
        // top level
        Overlay<Destination, ComparableKey<?>> tr1, tr2, tr3;
        tr1 = new Suzaku<Destination, ComparableKey<?>>(
        				ft = new FailureSimulationChannelTransport<UdpLocator>(
        						p1.newBaseChannelTransport(loc = new UdpLocator(new InetSocketAddress("localhost", 12367)))));
        
        tr2 = new Suzaku<Destination, ComparableKey<?>>(
                        new FailureSimulationChannelTransport<UdpLocator>(
                        		p2.newBaseTransport(
                        				new UdpLocator(new InetSocketAddress("localhost", 12368)))));
        
        tr3 = new Suzaku<Destination, ComparableKey<?>>(
                        new FailureSimulationChannelTransport<UdpLocator>(
                        		p3.newBaseChannelTransport(
                        				new UdpLocator(new InetSocketAddress("localhost", 12369)))));

        sg_received2 = false;
        sg_received3 = false;
        
        tr2.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
                return overlay.singletonFutureQueue("recv2");
            }

            // unused
            public void onReceive(Transport<Destination> trans,
                    ReceivedMessage rmsg) {
            }
        });
        
        tr3.setListener(new OverlayListener<Destination, ComparableKey<?>>() {
            public void onReceive(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
            }

            public FutureQueue<?> onReceiveRequest(
                    Overlay<Destination, ComparableKey<?>> overlay,
                    OverlayReceivedMessage<ComparableKey<?>> rmsg) {
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
        tr1.addKey(new DoubleKey(1.0));
        tr2.addKey(new DoubleKey(2.0));
        tr3.addKey(new DoubleKey(3.0));
        Thread.sleep(500);
        
        DoubleKey key = null;
        for (Object obj : tr1.getKeys()) {
            if (obj instanceof DoubleKey) {
                key = (DoubleKey) obj;
            }
        }
        
        ft.setErrorRate(100);
        ft.upsetTransport();
        
        List<Object> l = Arrays.asList(tr2.request(
                new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received3, "SG3 receive failed");
        
        ft.setErrorRate(100);
        l = Arrays.asList(tr1.request(
                new KeyRange<DoubleKey>(new DoubleKey(2.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        assertTrue(!sg_received2, "SG2 received falsely");
        assertTrue(!sg_received3, "SG3 received falsely");
        
        Thread.sleep(20000);
        //ft.setErrorRate(0);
        ft.repairTransport();
        l = Arrays.asList(tr2.request(
                new KeyRange<DoubleKey>(new DoubleKey(1.0), new DoubleKey(3.0)),
                key.getKey().toString(), 2000).getAllValues());
        sg_received2 = l.contains("recv2");
        sg_received3 = l.contains("recv3");
        assertTrue(sg_received2, "SG2 received");
        assertTrue(sg_received3, "SG3 received");
        
        p1.fin();
        p2.fin();
        p3.fin();    	
	}
	
	@Test
    public void UDPThroughTransportTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // top level
        Transport<UdpLocator> tr1 = new ThroughTransport<UdpLocator>(
        				p1.newBaseChannelTransport(
        				new UdpLocator(
        						new InetSocketAddress("localhost", 12367))));
        
        Transport<UdpLocator> tr2 = new ThroughTransport<UdpLocator>(
        				p2.newBaseChannelTransport(
        				new UdpLocator(
        						new InetSocketAddress("localhost", 12368))));

        udp_received1 = false;
        udp_received2 = false;
                
        tr1.setListener(new TransportListener<UdpLocator>() {
            public void onReceive(Transport<UdpLocator> trans, ReceivedMessage msg) {
                udp_received1 = "654321".equals(msg.getMessage());
            }
        });
        
        tr2.setListener(new TransportListener<UdpLocator>() {
            public void onReceive(Transport<UdpLocator> trans, ReceivedMessage msg) {
                udp_received2 = "123456".equals(msg.getMessage());
            }
        });

        tr1.send((UdpLocator) tr2.getEndpoint(), "123456");
        tr2.send((UdpLocator) tr1.getEndpoint(), "654321");

        // Channel ch = tr1.newChannel(tr2.getMyEndpoint());
        // ch.send("abcdefg".getBytes());

        // ReceivedMessage b = ch.receive(1000);
        // ch.close();
        Thread.sleep(1000);

        p1.fin();
        p2.fin();
        assertTrue(udp_received1, "UDP1 receive failed");
        assertTrue(udp_received2, "UDP2 receive failed");
        // assertTrue(ByteUtil.equals(b.getMessage(), "abcdefg".getBytes()));
    }
    
}
