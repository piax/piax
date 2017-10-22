package test.trans;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;

import org.junit.Test;
import org.piax.common.PeerId;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.idtrans.PrimaryKey;

public class TestNettyChannelTransport {
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
    public void testIdChannelConnection() throws Exception {
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

        tr2.send((PrimaryKey)tr1.getEndpoint(), "654321");

        Thread.sleep(1000);
        assertTrue(received1);
        
        p1.fin();
        p1 = Peer.getInstance(new PeerId("p1"));
        tr1 = p1.newBaseTransport(new PrimaryKey(
                new PeerId("tr1"), new NettyLocator(new InetSocketAddress("localhost", 12369))));
        tr1.setListener((trans, msg) -> {
            received1 = "654321".equals(msg.getMessage());
        });
        received1 = false;
        
        // tr2 needs not to know tr1 is changed.
        tr2.send((PrimaryKey)tr1.getEndpoint(), "654321");
        Thread.sleep(1000);
        assertTrue(received1);

        tr1.send((PrimaryKey)tr2.getEndpoint(), "123456");
        Thread.sleep(1000);
        assertTrue(received2);
        
        p1.fin();
        p2.fin();

    }
    
}
