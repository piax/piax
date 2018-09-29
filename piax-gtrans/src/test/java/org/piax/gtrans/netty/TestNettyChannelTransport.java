package org.piax.gtrans.netty;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;

import org.junit.jupiter.api.Test;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.netty.NettyLocator.TYPE;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.netty.udp.UdpPrimaryKey;

class TestNettyChannelTransport {
    boolean received1, received2;
    @Test
    public void testMutualConnection() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // top level
        Transport<PrimaryKey> tr1 = p1.newBaseTransport(new PrimaryKey(
                new PeerId("tr1"), new NettyLocator(new InetSocketAddress("localhost", 12367))));
        Transport<PrimaryKey> tr2 = p2.newBaseTransport(new PrimaryKey(
                new PeerId("tr2"), new NettyLocator(new InetSocketAddress("localhost", 12368))));

        tr1.setListener((trans, msg) -> {
                received1 = "654321".equals(msg.getMessage());
        });

        tr2.setListener((trans, msg) -> {
                received2 = "123456".equals(msg.getMessage());
        });

        new Thread(() -> {
            try {
                tr1.send((PrimaryKey)tr2.getEndpoint(), "123456");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }).start();
        tr2.send((PrimaryKey)tr1.getEndpoint(), "654321");

        // Channel ch = tr1.newChannel(tr2.getMyEndpoint());
        // ch.send("abcdefg".getBytes());

        // ReceivedMessage b = ch.receive(1000);
        // ch.close();
        Thread.sleep(1000);

        p1.fin();
        p2.fin();
        assertTrue(received1 && received2);
    }
    
    @Test
    public void testUdpChannel() throws Exception {
     // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        received2 = false;
        Transport<UdpPrimaryKey> tr1 = p1.newBaseTransport((UdpPrimaryKey)Endpoint.newEndpoint("udp:tr1")); // default port: 12367
        Transport<UdpPrimaryKey> tr2 = p2.newBaseTransport((UdpPrimaryKey)Endpoint.newEndpoint("udp:tr2:localhost:12368"));

        tr2.setListener((trans, msg) -> {
                received2 = "654321".equals(msg.getMessage());
        });

        tr1.send((UdpPrimaryKey)Endpoint.newEndpoint("udp:tr2:localhost:12368"), "654321");
        Thread.sleep(1000);
        assertTrue(received2);
        
        p1.fin();
        p2.fin();
        
    }
    
    @Test
    public void testUdpChannel2() throws Exception {
     // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        Transport<Endpoint> tr1 = p1.newBaseTransport(Endpoint.newEndpoint("udp")); // default port: 12367
        Transport<Endpoint> tr2 = p2.newBaseTransport(Endpoint.newEndpoint("udp:*:12368"));

        tr1.setListener((trans, msg) -> {
            received1 = "123456".equals(msg.getMessage());
        });
        tr2.setListener((trans, msg) -> {
            received2 = "654321".equals(msg.getMessage());
        });

        received2 = false;
        //tr1.send(Endpoint.newEndpoint("udp:*:localhost:12368"), "654321");
        tr1.send(new UdpPrimaryKey(new NettyLocator(TYPE.UDP, "localhost", 12368)), "654321");
        
        /*Channel c = tr1.newChannel(new NettyLocator(TYPE.UDP, "localhost", 12368));
        c.send("654321");
        c.getRemote()*/
        
        Thread.sleep(100);
        assertTrue(received2);
        received2 = false;
        tr1.send(new UdpPrimaryKey(new PeerId("p2")), "654321");
        received1 = false;
        tr2.send(new UdpPrimaryKey(new PeerId("p1")), "123456");
        
        Thread.sleep(100);
        assertTrue(received1);
        assertTrue(received2);
        
        p1.fin();
        p2.fin();
        
    }

    @Test
    public void testIdChannelConnection() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // top level
        Transport<PrimaryKey> tr1 = p1.newBaseTransport(new PrimaryKey(
                new StringKey("tr1"), new NettyLocator(new InetSocketAddress("localhost", 12367))));
        Transport<PrimaryKey> tr2 = p2.newBaseTransport(new PrimaryKey(
                new StringKey("tr2"), new NettyLocator(new InetSocketAddress("localhost", 12368))));

        tr1.setListener((trans, msg) -> {
                received1 = "654321".equals(msg.getMessage());
        });

        tr2.setListener((trans, msg) -> {
                received2 = "123456".equals(msg.getMessage());
        });

        tr2.send((PrimaryKey)tr1.getEndpoint(), "654321");

        Thread.sleep(1000);
        assertTrue(received1);
        
        p1.fin();
        p1 = Peer.getInstance(new PeerId("p1"));
        tr1 = p1.newBaseTransport(new PrimaryKey(
                new StringKey("tr1"), new NettyLocator(new InetSocketAddress("localhost", 12369))));
        tr1.setListener((trans, msg) -> {
            received1 = "654321".equals(msg.getMessage());
        });
        received1 = false;
        
        tr1.send((PrimaryKey)tr2.getEndpoint(), "123456");
        Thread.sleep(1000);
        assertTrue(received2);
        
        // tr2 needs not to know tr1 is changed.
        tr2.send(new PrimaryKey(new StringKey("tr1")), "654321");
        Thread.sleep(1000);
        assertTrue(received1);

        p1.fin();
        p2.fin();
    }

}
