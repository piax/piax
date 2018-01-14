package test.ov;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.llnet.LocationId;
import org.piax.gtrans.ov.sg.MSkipGraph;

/**
 * LLNetについての簡単なテスト
 */
public class TestLLNet extends Common {

    static Overlay<GeoRegion, Location> genOv(Peer peer, Net ntype, int port) 
            throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", port));
        Overlay<KeyRanges<LocationId>, LocationId> sg = 
                new MSkipGraph<KeyRanges<LocationId>, LocationId>(tr);
        Overlay<GeoRegion, Location> llnet = new LLNet(sg);
        App<GeoRegion, Location> app = new App<GeoRegion, Location>(peer.getPeerId());
        llnet.setListener(appId, app);
        return llnet;
    }
    
    public static void main(String[] args) throws IOException {
        Net ntype = Net.UDP;
        int numPeer = 10;
        int seedPeerNo = 0;
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<GeoRegion, Location>[] ovs = new Overlay[numPeer];
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
        printf("=> took %d msec%n", (etime-stime));
        
        printf("%n** join%n");
        stime = System.currentTimeMillis();
        Endpoint seed = ovs[seedPeerNo].getBaseTransport().getEndpoint();
        for (int i = 0; i < numPeer; i++) {
            ((Overlay<?,?>) ovs[i].getLowerTransport()).join(seed);
            printf("%s ", ovs[i].getPeerId());
            if ((i+1) % 20 == 0) printf("%n");
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));
        
        int n = 10;
        printf("%n** query (%d)%n", n);
        stime = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Overlay<GeoRegion, Location> ov = ovs[next(numPeer)];
            query2(ov, next(numPeer)/10.0, next(numPeer)/10.0);
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));
        
        printf("%n** fin%n");
        sleep(20);
        for (int i = numPeer - 1; i >= 0; i--) {
            peers[i].fin();
//            Transport<?> sg = ovs[i].getLowerTransport();
//            ovs[i].fin();
//            sg.fin();
//            sg.getLowerTransport().fin();
        }
        printf("** end **%n");
    }
}
