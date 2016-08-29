package test.ov;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.compound.CompoundOverlay;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.llnet.LocationId;
import org.piax.gtrans.ov.sg.MSkipGraph;

/**
 * LLNetとLLNetの間でCompoundOverlayのテストを行う
 * 
 * @author     Mikio Yoshida
 * @version
 */
public class TestCompound2 extends Common {

    static class CompPeer<D extends Destination, K extends Key> {
        Peer peer;
        Transport<?> bt;
        CompoundOverlay<D, K> comp;
        Overlay<D, K> ov1;
        Overlay<D, K> ov2;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, IdConflictException {
        Net ntype = Net.UDP;
        int numPeer = 20;
        int gateway = 10;
        int seedPeerNo = 0;
        List<TransportIdPath> ovIdPaths = 
                Arrays.asList(new TransportIdPath("llnet"), new TransportIdPath("llnet2"));
        @SuppressWarnings("unchecked")
        CompPeer<GeoRegion, Location>[] peers = new CompPeer[numPeer];
        long stime, etime;
        
        printf("** Simulation start **%n");
        printf(" - locator type: %s%n", ntype);
        printf(" - num of peers: %d%n", numPeer);
        printf(" - seed: %d%n", seedPeerNo);

        printf("%n** new peerId and overlays%n");
        stime = System.currentTimeMillis();
        for (int i = 0; i < numPeer; i++) {
            Peer peer = Peer.getInstance(new PeerId("p" + i));
            ChannelTransport<?> bt = peer.newBaseChannelTransport(
                    genLocator(ntype, "localhost", 10000 + i));
            peers[i] = new CompPeer<GeoRegion, Location>();
            peers[i].peer = peer;
            peers[i].bt = bt;
            CompoundOverlay<GeoRegion, Location> comp = 
                    new CompoundOverlay<GeoRegion, Location>(peer, ovIdPaths);
            comp.setListener(appId, new App<GeoRegion, Location>(peer.getPeerId()));
            peers[i].comp = comp;
            if (0 <= i && i <= gateway) {
                Overlay<KeyRanges<LocationId>, LocationId> sg = 
                        new MSkipGraph<KeyRanges<LocationId>, LocationId>(bt);
                Overlay<GeoRegion, Location> llnet = new LLNet(new TransportId(
                        "llnet"), sg);
                llnet.setListener(comp.getTransportId(), comp);
                llnet.addKey(comp.getTransportId(), new Location(i / 10.0,
                        i / 10.0));
                peers[i].ov1 = llnet;
            }
            if (gateway <= i && i < numPeer) {
                Overlay<KeyRanges<LocationId>, LocationId> sg = 
                        new MSkipGraph<KeyRanges<LocationId>, LocationId>(
                                new TransportId("sg2"), bt);
                Overlay<GeoRegion, Location> llnet = 
                        new LLNet(new TransportId("llnet2"), sg);
                llnet.setListener(comp.getTransportId(), comp);
                llnet.addKey(comp.getTransportId(), new Location(i / 10.0,
                        i / 20.0));
                peers[i].ov2 = llnet;
            }
            printf("%s ", peer.getPeerId());
            if ((i + 1) % 20 == 0)
                printf("%n");
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));
        
        printf("%n** join%n");
        stime = System.currentTimeMillis();
        printf("llnet: ");
        Endpoint seed1 = peers[0].bt.getEndpoint();
        for (int i = 0; i <= gateway; i++) {
            peers[i].ov1.join(seed1);
            printf("%s ", peers[i].peer.getPeerId());
        }
        printf("%nllnet2: ");
        Endpoint seed2 = peers[gateway].bt.getEndpoint();
        for (int i = gateway; i < numPeer; i++) {
            peers[i].ov2.join(seed2);
            printf("%s ", peers[i].peer.getPeerId());
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));
        
        sleep(20);
        int n = 10;
        printf("%n** geo query (%d)%n", n);
        stime = System.currentTimeMillis();
        CompoundOverlay<GeoRegion, Location> gate = peers[gateway].comp;
        for (int i = 0; i < n; i++) {
            double x = next(numPeer)/10.0;
            double y = next(numPeer)/10.0;
            gate.setGateway(false);
            printf("--gateway off--%n");
            query(peers[0].comp, x, y);
            query(peers[numPeer - 1].comp, x, y);
            gate.setGateway(true);
            printf("--gateway on--%n");
            query(peers[0].comp, x, y);
            query(peers[numPeer - 1].comp, x, y);
            printf("%n");
        }
        etime = System.currentTimeMillis();
        printf("** took %d msec%n", (etime-stime));
        
        sleep(100);
        printf("%n** fin%n");
        stime = System.currentTimeMillis();
//        for (int i = gateway; i >= 0; i--) {
//            peers[i].ov1.fin();
//            peers[i].ov1.getLowerTransport().fin();
//        }
//        for (int i = numPeer - 1; i >= gateway; i--) {
//            peers[i].ov2.fin();
//            peers[i].ov2.getLowerTransport().fin();
//        }
        for (int i = 0; i < numPeer; i++) {
//            peers[i].comp.fin();
//            peers[i].bt.fin();
            peers[i].peer.fin();
            printf("%s ", peers[i].peer.getPeerId());
            if ((i+1) % 20 == 0) printf("%n");
        }
        etime = System.currentTimeMillis();
        printf("%n");
        printf("=> took %d msec%n", (etime-stime));
        printf("%n** end **%n");
    }
}
