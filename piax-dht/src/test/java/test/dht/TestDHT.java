package test.dht;

import java.io.IOException;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.kvs.dht.DHT;
import org.piax.kvs.dht.HashId;

import test.Util;

/**
 * DHTについての簡単なテスト
 */
public class TestDHT extends Util {

    static <D extends Destination, K extends ComparableKey<?>>
            Overlay<D, K> genOv(Peer peer, Net ntype, int port) 
                    throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", port));
        Overlay<D, K> ov = new MSkipGraph<D, K>(tr);
        return ov;
    }

    static void printDHT() {
        printf("%n** print DHT repo%n");
        for (int i = 0; i < numPeer; i++) {
            printf(" * DHT repository status on %s, %s", dhts[i].sg.getPeerId(),
                    dhts[i]);
        }
    }
    
    static int numPeer = 100;
    static DHT[] dhts = new DHT[numPeer];
    
    public static void main(String[] args) throws IOException {
        Net ntype = Net.EMU;
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<LowerUpper, HashId>[] ovs = new Overlay[numPeer];
        int seedPeerNo = 0;
        long stime, etime;
        
        printf("** Simulation start **%n");
        printf(" - locator type: %s%n", ntype);
        printf(" - num of peers: %d%n", numPeer);
        printf(" - seed: %d%n", seedPeerNo);

        printf("%n** new peerId and overlay%n");
        stime = System.currentTimeMillis();
        for (int i = 0; i < numPeer; i++) {
            peers[i] = Peer.getInstance(new PeerId("p" + i));
            try {
                ovs[i] = genOv(peers[i], ntype, 10000 + i);
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
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));

        DHT dht = dhts[0];
//        dht.put("hoge" + 0, "abc");
//        sleep(100);
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
            if (get == null) {
                System.out.print("!");
            } else if (get.equals("hage" + i)) {
                System.out.print("+");
            } else {
                System.out.print("?");
            }
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
