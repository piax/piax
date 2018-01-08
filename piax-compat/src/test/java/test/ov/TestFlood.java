package test.ov;

import java.io.IOException;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.wrapper.IntegerKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.flood.SimpleFlooding;

/**
 * SimpleFloodingについての簡単なテスト
 */
public class TestFlood extends Common {

    static <D extends Destination, K extends Key> Overlay<D, K> genOv(
            Peer peer, Net ntype, int port) throws IdConflictException,
            IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", port));
        Overlay<D, K> ov = new SimpleFlooding<D, K>(tr);
        App<D, K> app = new App<D, K>(peer.getPeerId());
        ov.setListener(appId, app);
        return ov;
    }
    
    public static void main(String[] args) throws IOException {
        Net ntype = Net.TCP;
        int numPeer = 10;
        int seedPeerNo = 0;
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<Destination, Key>[] ovs = new Overlay[numPeer];
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
                Location ll = new Location(i/10.0, i/10.0);
                ovs[i].addKey(appId, ll);
            } catch (IdConflictException e) {
                System.out.println(e);
            }
            printf("%s ", peers[i].getPeerId());
            if ((i+1) % 20 == 0) printf("%n");
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("** took %d msec%n", (etime-stime));
        
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
        
        int n = 20;
        printf("%n** range query (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Overlay<Destination, Key> ov = ovs[next(numPeer)];
            rangeQuery2(ov, next(numPeer), next(numPeer));
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));

        printf("%n** geo query (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Overlay<Destination, Key> ov = ovs[next(numPeer)];
            query2(ov, next(numPeer)/10.0, next(numPeer)/10.0);
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));

//        printf("%n** show table%n");
//        for (int i = 0; i < numPeer; i++) {
//            printf("%s:%n%s%n", ovs[i].getPeerId(),
//                    ((SimpleFlooding) ovs[i]).showTable());
//        }
        
        printf("%n** fin%n");
        sleep(20);
        for (int i = numPeer - 1; i >= 0; i--) {
            peers[i].fin();
//            ovs[i].getLowerTransport().fin();
//            ovs[i].fin();
        }
        printf("** end **%n");
    }
}
