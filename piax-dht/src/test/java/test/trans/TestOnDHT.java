package test.trans;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Test;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.StatusRepo;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.async.suzaku.Suzaku;
import org.piax.gtrans.ov.ddll.NodeMonitor;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.kvs.dht.DHT;
import org.piax.kvs.dht.HashId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOnDHT {
    private static final Logger logger = LoggerFactory
            .getLogger(TestOnDHT.class);

    static <D extends Destination, K extends ComparableKey<?>> Overlay<D, K> genOv(
            boolean isSG, Peer peer, PeerLocator locator)
            throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(locator);
        Overlay<D, K> ov = null;
        if (isSG) {
            ov = new MSkipGraph<D, K>(tr);
        } else {
            ov = new Suzaku<D, K>(tr);
        }
        return ov;
    }

    public static void printf(String f, Object... args) {
        logger.debug(String.format(f, args));
    }

    public static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
        }
    }

    static void printDHT() {
        logger.debug("\n** print DHT repo\n");
        for (int i = 0; i < numPeer; i++) {
            printf(" * DHT repository status on %s, %s",
                    dhts[i].sg.getPeerId(), dhts[i]);
        }
    }

    static int numPeer = 16;
    static DHT[] dhts = new DHT[numPeer];

    enum L {
        UDP, TCP, EMU, NETTY
    };

    @Test
    public void DHTOnSuzakuOnEmuTest() throws Exception {
        DHTRun(false, L.EMU);
    }

    @Test
    public void DHTOnSuzakuOnUdpTest() throws Exception {
        DHTRun(false, L.UDP);
    }

    @Test
    public void DHTOnSuzakuOnTcpTest() throws Exception {
        DHTRun(false, L.TCP);
    }

    @Test
    public void DHTOnSuzakuOnNettyTest() throws Exception {
        DHTRun(false, L.NETTY);
    }

    @Test
    public void DHTOnSkipGraphOnNettyTest() throws Exception {
        DHTRun(true, L.NETTY);
    }

    @Test
    public void DHTOnSkipGraphOnEmuTest() throws Exception {
        DHTRun(true, L.EMU);
    }

    @Test
    public void DHTOnSkipGraphOnUdpTest() throws Exception {
        DHTRun(true, L.UDP);
    }

    @Test
    public void DHTOnSkipGraphOnTcpTest() throws Exception {
        DHTRun(true, L.TCP);
    }

    public void DHTRun(boolean useSG, L loc) throws Exception {
        StatusRepo.ON_MEMORY = true;
        NodeMonitor.PING_TIMEOUT = 100 * 1000;
        GTransConfigValues.rpcTimeout = 100 * 1000;
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<LowerUpper, HashId>[] ovs = new Overlay[numPeer];
        int seedPeerNo = 0;

        printf("** Simulation start **%n");
        printf(" - num of peers: %d%n", numPeer);
        printf(" - seed: %d%n", seedPeerNo);

        printf("%n** new peerId and overlay%n");
        for (int i = 0; i < numPeer; i++) {
            peers[i] = Peer.getInstance(new PeerId("p" + i));
            try {
                PeerLocator l = null;
                switch (loc) {
                case NETTY:
                    // if (i % 10 == 1) {
                    // l = new NettyNATLocator(new
                    // InetSocketAddress("localhost", 20000 + i));
                    // }
                    // else {
                    l = new NettyLocator(new InetSocketAddress("localhost",
                            20000 + i));
                    // }
                    break;
                case TCP:
                    l = new TcpLocator(new InetSocketAddress("localhost",
                            20000 + i));
                    break;
                case UDP:
                    l = new UdpLocator(new InetSocketAddress("localhost",
                            20000 + i));
                case EMU:
                    l = new EmuLocator(10000 + i);
                }
                ovs[i] = genOv(useSG, peers[i], l);
                // Id id = new Id(new byte[]{(byte)(i * 256 / numPeer)});
                // dhts[i] = new DHT(new ServiceId("dht"), ovs[i], id, true);
                dhts[i] = new DHT(ovs[i], true);
            } catch (IdConflictException e) {
                logger.debug(e.toString());
            }
            printf("%s ", peers[i].getPeerId());
            if ((i + 1) % 20 == 0)
                printf("%n");
        }
        printf("%n");

        printf("%n** join%n");
        for (int i = 0; i < numPeer; i++) {
            seedPeerNo = (i % 10) == 0 ? 0 : (i / 10) * 10;
            Endpoint seed = ovs[seedPeerNo].getBaseTransport().getEndpoint();
            ovs[i].join(seed);
            printf("%s ", ovs[i].getPeerId());
            if ((i + 1) % 20 == 0)
                printf("%n");
        }
        printf("%n");
        final DHT dht = dhts[0];
        int n = 20;
        printf("%n** put (%d)%n", n);
        for (int i = 0; i < n; i++) {
            printf("putting %s", "hage" + i);
            try {
                dht.put("hoge" + i, "hage" + i);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        printf("%n** get (%d)%n", n);
        for (int i = 0; i < n; i++) {
            String get = null;
            try {
                get = (String) dht.get("hoge" + i);
            } catch (Exception e) {
                e.printStackTrace();
            }
            assertTrue("GET failed", (get != null && get.equals("hage" + i)));
        }
        printf("%n** fin%n");
        for (int i = 1; i < numPeer; i++) {
            dhts[i].fin();
            try {
                ovs[i].leave();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        dhts[0].fin();
        try {
            ovs[0].leave();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (int i = 0; i < numPeer; i++) {
            peers[i].fin();
        }
        printf("** end **%n");
    }
}
