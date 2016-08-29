package test.trans;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.TransportIdPath;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.compound.CompoundOverlay;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.llnet.LocationId;
import org.piax.gtrans.ov.sg.MSkipGraph;

import test.Util;

public class TestTransportTree extends Util {

    static Overlay<GeoRegion, Location> genOv(Peer peer, Net ntype, int port) 
            throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", port));
        Overlay<KeyRanges<LocationId>, LocationId> sg = 
                new MSkipGraph<KeyRanges<LocationId>, LocationId>(tr);
        Overlay<GeoRegion, Location> llnet = new LLNet(sg);
        return llnet;
    }
    
    public static void main(String[] args) throws IOException, IdConflictException {
        Net ntype = Net.UDP;
        printf("- start -%n");
        printf("%n");

        // Peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        List<TransportIdPath> ovIdPaths = 
                Arrays.asList(new TransportIdPath("llnet"), new TransportIdPath("llnet2"));

        // Transport, Overlayを生成する
        Overlay<GeoRegion, Location> ov = genOv(p1, ntype, 10000);
        CompoundOverlay<GeoRegion, Location> comp = 
                new CompoundOverlay<GeoRegion, Location>(p1, ovIdPaths);

        printf("** path of ov: %s%n", ov.getTransportIdPath());
        printf("** path of comp: %s%n", comp.getTransportIdPath());
        printf("** lowers of ov: %s%n", ov.getLowerTransports());
        printf("** lowers of comp: %s%n", comp.getLowerTransports());
        printf("** all transports: %s%n", p1.getAllTransports());
        printf("%n");
        printf("** %s%n%n", p1);
        printf("** transport tree:%n%s%n", p1.printTransportTree());
        p1.fin();
        printf("- end -%n");
    }
}
