package test.trans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.StatusRepo;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.impl.BaseTransportMgr;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap.SerializerType;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.ddll.NodeMonitor;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.suzaku.Suzaku;
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
            O ovt, Peer peer, Endpoint locator)
            throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(locator);
        Overlay<D, K> ov = null;
        switch(ovt) {
        case SG:
            Peer.RECEIVE_ASYNC.set(true);
            ov = new MSkipGraph<D, K>(tr);
            break;
        case OCS:
            Peer.RECEIVE_ASYNC.set(true);
            ov = new org.piax.gtrans.ov.szk.Suzaku<D, K>(tr);
            break;
        case CS:
            Peer.RECEIVE_ASYNC.set(true);
            ov = new Suzaku<D, K>(tr, 1);
            break;
        case SZK:
            Peer.RECEIVE_ASYNC.set(false);
            ov = new Suzaku<D, K>(tr, 3);
            break;
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

    static int numPeer = 16;
    //

    enum L {
        UDP, TCP, EMU, NETTY, ID
    };
    
    enum O {
        SG, OCS, CS, SZK
    }

    @Test
    public void DHTOnSuzakuOnEmuTest() throws Exception {
        DHTRun(O.SZK, L.EMU);
    }

    @Test
    public void DHTOnSuzakuOnUdpTest() throws Exception {
        DHTRun(O.SZK, L.UDP);
    }

    @Test
    public void DHTOnSuzakuOnTcpTest() throws Exception {
        DHTRun(O.SZK, L.TCP);
    }

    @Test
    public void DHTOnSuzakuOnNettyTest() throws Exception {
        DHTRun(O.SZK, L.NETTY);
    }
    
    @Test
    public void DHTOnSuzakuOnIdTest() throws Exception {
        DHTRun(O.SZK, L.ID);
    }
    
    @Test
    public void DHTOnOCSOnNettyTest() throws Exception {
        try {
            NettyBootstrap.SERIALIZER.set(SerializerType.Java);
            DHTRun(O.OCS, L.NETTY);
        }
        finally {
            NettyBootstrap.SERIALIZER.set(SerializerType.Kryo);
        }
    }
    
    @Test
    public void DHTOnOCSOnIdTest() throws Exception {
        try {
            NettyBootstrap.SERIALIZER.set(SerializerType.Java);
            DHTRun(O.OCS, L.ID);
        }
        finally {
            NettyBootstrap.SERIALIZER.set(SerializerType.Kryo);
        }
    }
    
    @Test
    public void DHTOnCSOnNettyTest() throws Exception {
        DHTRun(O.CS, L.NETTY);
    }
    
    @Test
    public void DHTOnCSOnIdTest() throws Exception {
        DHTRun(O.CS, L.ID);
    }

    
    @Test
    public void DHTOnSkipGraphOnNettyTest() throws Exception {
        DHTRun(O.SG, L.NETTY);
    }
    
    @Test
    public void DHTOnSkipGraphOnIdTest() throws Exception {
        DHTRun(O.SG, L.ID);
    }

    @Test
    public void DHTOnSkipGraphOnEmuTest() throws Exception {
        DHTRun(O.SG, L.EMU);
    }

    @Test
    public void DHTOnSkipGraphOnUdpTest() throws Exception {
        DHTRun(O.SG, L.UDP);
    }

    @Test
    public void DHTOnSkipGraphOnTcpTest() throws Exception {
        DHTRun(O.SG, L.TCP);
    }

    public static long DHTRun(O ovt, L loc) throws Exception {
        BaseTransportMgr.BASE_TRANSPORT_MANAGER_CLASS.set("org.piax.gtrans.impl.DefaultBaseTransportGenerator");

        DHT[] dhts = new DHT[numPeer];
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
        
//        long start = System.currentTimeMillis();

        printf("%n** new peerId and overlay%n");
        for (int i = 0; i < numPeer; i++) {
            peers[i] = Peer.getInstance(new PeerId("p" + i));
            try {
                Endpoint l = null;
                switch (loc) {
                case ID:
                    l = new PrimaryKey(peers[i].getPeerId(),
                            new NettyLocator(new InetSocketAddress("localhost", 20000 + i)));
                    break;
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
                ovs[i] = genOv(ovt, peers[i], l);
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
        int n = 100;
        long ret;
        try {
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
            long start = System.currentTimeMillis();
            printf("%n** get (%d)%n", n);
            for (int i = 0; i < n; i++) {
                String get = null;
                try {
                    get = (String) dht.get("hoge" + i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //assertTrue("GET failed",
                if (get == null || !get.equals("hage" + i)) {
                    System.err.println("get failed:" + get + "!= hage" + i);
                }
            }
            ret = System.currentTimeMillis() - start;
        } finally {
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
        }
        printf("** end **%n");
        return ret;
    }
    
    public static void eval(O ovt, L loc) throws Exception {
        int TRIAL = 10;
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < TRIAL; i++) {
            long elapsed = DHTRun(ovt, loc);
            results.add((int)elapsed);
            System.out.println(i + "th trial end.");
        }
        // remove head and tail (min and max) then calc ave. 
        double ave = results.stream().sorted().limit(results.size() - 1).skip(1).mapToInt(Integer::intValue).average().getAsDouble();
        System.out.println(ovt + "/"+ loc + ":" + ave);
    }

    public static void main(String args[]) throws Exception {
        for (int n = 10; n <= 30; n+=10) {
            numPeer = n;
            System.out.println("n=" + numPeer);
            eval(O.SZK, L.ID);
            eval(O.SZK, L.TCP);
            //eval(O.OCS, L.ID);
            //eval(O.OCS, L.NETTY);
            eval(O.SZK, L.NETTY);

            
            
            eval(O.CS, L.NETTY);
            eval(O.CS, L.ID);
            //eval(O.SG, L.NETTY);
            //eval(O.SG, L.ID);
        }
    }
}
