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
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.szk.Suzaku;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.kvs.dht.DHT;
import org.piax.kvs.dht.HashId;

public class TestOnDHT {
	
	static <D extends Destination, K extends ComparableKey<?>>
	Overlay<D, K> genOv(boolean isSG, Peer peer, PeerLocator locator) 
			throws IdConflictException, IOException {
		ChannelTransport<?> tr = peer.newBaseChannelTransport(locator);
		Overlay<D, K> ov = null;
		if (isSG) {
			ov = new MSkipGraph<D, K>(tr);
		}
		else {
			ov = new Suzaku<D, K>(tr);
		}
		return ov;
	}
    
    public static void printf(String f, Object... args) {
        System.out.printf(f, args);
    }
    
    public static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
        }
    }

    static void printDHT() {
        System.out.printf("%n** print DHT repo%n");
        for (int i = 0; i < numPeer; i++) {
            printf(" * DHT repository status on %s, %s", dhts[i].sg.getPeerId(),
                    dhts[i]);
        }
    }
    
    static int numPeer = 100;
    static DHT[] dhts = new DHT[numPeer];
    
    enum L {
    		UDP,TCP,EMU
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
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<LowerUpper, HashId>[] ovs = new Overlay[numPeer];
        int seedPeerNo = 0;
        long stime, etime;
        
        printf("** Simulation start **%n");
        printf(" - num of peers: %d%n", numPeer);
        printf(" - seed: %d%n", seedPeerNo);

        printf("%n** new peerId and overlay%n");
        stime = System.currentTimeMillis();
        for (int i = 0; i < numPeer; i++) {
            peers[i] = Peer.getInstance(new PeerId("p" + i));
            try {
            		PeerLocator l = null;
            		switch(loc) {
            		case TCP:
            			l = new TcpLocator(new InetSocketAddress("localhost", 20000 + i));
            			break;
            		case UDP:
            			l = new UdpLocator(new InetSocketAddress("localhost", 20000 + i));
            		case EMU:
            			l = new EmuLocator(10000 + i);
            		}            		
                ovs[i] = genOv(useSG, peers[i], l);
//                Id id = new Id(new byte[]{(byte)(i * 256 / numPeer)});
//                dhts[i] = new DHT(new ServiceId("dht"), ovs[i], id, true);
                dhts[i] = new DHT(ovs[i], true);
            } catch (IdConflictException e) {
                System.out.println(e);
            }
            printf("%s ", peers[i].getPeerId());
            if ((i+1) % 20 == 0) printf("%n");
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));
        
        printf("%n** join%n");
        stime = System.currentTimeMillis();
        Endpoint seed = ovs[seedPeerNo].getBaseTransport().getEndpoint();
        for (int i = 0; i < numPeer; i++) {
            ovs[i].join(seed);
            printf("%s ", ovs[i].getPeerId());
            if ((i+1) % 20 == 0) printf("%n");
            sleep(10);
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));

        DHT dht = dhts[0];
//        dht.put("hoge" + 0, "abc");
        printf("sleeping 10 seconds...");
        sleep(10000);
//        printDHT();

        int n = 100;
        printf("%n** put (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            dht.put("hoge" + i, "hage" + i);
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));

        printf("%n** get (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            String get = (String) dht.get("hoge" + i);
            assertTrue("GET failed", (get != null && get.equals("hage" + i)));
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));

        printDHT();

        sleep(200);
        printf("%n** fin%n");
        for (int i = 0; i < numPeer; i++) {
            dhts[i].fin();
            ovs[i].leave();
        }
        sleep(200);
        for (int i = 0; i < numPeer; i++) {
            peers[i].fin();
        }
        printf("** end **%n");
        
    }
}
