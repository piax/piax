package test.ov;

import java.io.IOException;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.wrapper.IntegerKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.ddll.NodeMonitor;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.sg.SkipGraph;

/**
 * MSkipGraphについての簡単なテスト
 */
public class TestSG extends Common {
    
    static <D extends Destination, K extends ComparableKey<?>> 
            Overlay<D, K> genOv(Peer peer, Net ntype, int port)
                    throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                "LINGER0", genLocator(ntype, "localhost", port));
        Overlay<D, K> ov = new MSkipGraph<D, K>(tr);
        App<D, K> app = new App<D, K>(peer.getPeerId());
        ov.setListener(appId, app);
        return ov;
    }

    public static void main(String[] args) throws IOException {
        GTransConfigValues.MAX_RECEIVER_THREAD_SIZE = 100000;
        GTransConfigValues.newChannelTimeout = 10000;
        NodeMonitor.PING_TIMEOUT = 10000;
        SkipGraph.DDLL_CHECK_PERIOD_L0 = 3 * 60 * 1000;
        SkipGraph.DDLL_CHECK_PERIOD_L1 = 10 * 60 * 1000;
        GTransConfigValues.ALLOW_REF_SEND_IN_BASE_TRANSPORT = true;
        
        Net ntype = Net.TCP;
        int numPeer = 2000;
        int seedPeerNo = 0;
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<Destination, IntegerKey>[] ovs = new Overlay[numPeer];
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
                ovs[i] = genOv(peers[i], ntype, 10000+ i);
                ovs[i].addKey(appId, new IntegerKey(i));
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
        
        sleep(3000);
        int n = 50;
        printf("%n** range query (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Overlay<Destination, ?> ov = ovs[next(numPeer)];
            rangeQuery2(ov, next(numPeer), next(numPeer));
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));
        
        sleep(200);
        n = 5;
        printf("%n** send (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Overlay<Destination, ?> ov = ovs[next(numPeer)];
            PeerId to = new PeerId("p" + next(numPeer));
            send(ov, to, "hogehoge");
            sleep(100);
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));

        printf("%n** fin%n");
        sleep(1000);
        for (int i = numPeer - 1; i >= 0; i--) {
            peers[i].fin();
        }
        printf("** end **%n");
    }
}
