package org.piax.gtrans.netty.idtrans;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.jupiter.api.Test;
import org.piax.common.PeerId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Peer;
import org.piax.gtrans.PeerLocator;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.suzaku.Suzaku;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIdTransport {
    boolean sg_received1, sg_received2;
    private static final Logger logger = 
            LoggerFactory.getLogger(TestIdTransport.class);
    
    
    @Test
    public void PrimaryKeyTest() throws Exception {
        // get peers
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // base transport (TCP)
        PeerLocator loc;
        ChannelTransport<?> bt1 = p1.newBaseChannelTransport(
                loc = new NettyLocator(new InetSocketAddress("localhost", 12367)));
        ChannelTransport<?> bt2 = p2.newBaseChannelTransport(
                new NettyLocator(new InetSocketAddress("localhost", 12368)));

        // top level
        Overlay<PrimaryKey, PrimaryKey> tr1 = new Suzaku<>(bt1);
        Overlay<PrimaryKey, PrimaryKey> tr2 = new Suzaku<>(bt2);

        sg_received1 = false;
        sg_received2 = false;

        tr1.setListener(new TransportListener<PrimaryKey>() {
            public void onReceive(Transport<PrimaryKey> trans,
                    ReceivedMessage rmsg) {
                logger.debug("tcp recv1:" + rmsg.getMessage());
                sg_received1 = rmsg.getMessage().equals("recv");
            }
        });

        tr2.setListener(new TransportListener<PrimaryKey>() {
            public void onReceive(Transport<PrimaryKey> trans,
                    ReceivedMessage rmsg) {
                try {
                    logger.debug("tcp recv2:" + rmsg.getMessage());
                    sg_received2 = true;
                    trans.send(new PrimaryKey(Double.parseDouble(
                            (String) rmsg.getMessage())), "recv");
                } catch (IOException e) {
                    fail("IOException occured");
                }
            }
        });

        boolean succ1 = tr1.join(loc);
        boolean succ2 = tr2.join(loc);
        Thread.sleep(500);
        boolean succ3 = tr1.addKey(new PrimaryKey(1.0));
        boolean succ4 = tr2.addKey(new PrimaryKey(2.0));

        assertTrue(succ1, "SG1 join failed");
        assertTrue(succ2, "SG2 join failed");
        assertTrue(succ3, "SG1 addKey failed");
        assertTrue(succ4, "SG2 addKey failed");
        Thread.sleep(500);

        tr1.send(new PrimaryKey(2.0), "1.0");

        Thread.sleep(1000);
        assertTrue(sg_received2, "SG2 receive failed");
        assertTrue(sg_received1, "SG1 receive failed");

        p1.fin();
        p2.fin();
    }

}
