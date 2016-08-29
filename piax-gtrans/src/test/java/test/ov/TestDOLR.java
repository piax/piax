package test.ov;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.PeerId;
import org.piax.common.dcl.parser.ParseException;
import org.piax.common.wrapper.ConvertedComparableKey;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.ov.sg.MSkipGraph;

/**
 * DOLRについての簡単なテスト
 */
public class TestDOLR extends Common {
    
    static <K extends Key> Overlay<K, K> genOv(Peer peer, Net ntype, int port) 
            throws IdConflictException, IOException {
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", port));
        Overlay<ConvertedComparableKey<K>, ConvertedComparableKey<K>> sg = 
                new MSkipGraph<ConvertedComparableKey<K>, ConvertedComparableKey<K>>(tr);
        Overlay<K, K> dolr = new DOLR<K>(sg);
        App<K, K> app = new App<K, K>(peer.getPeerId());
        dolr.setListener(appId, app);
        return dolr;
    }

    static <K extends Key> void query(Overlay<K, K> ov, K key) {
        FutureQueue<?> fq;
        try {
            fq = ov.request(appId, appId, key, null, 10000);
            System.out.printf("** query %s from %s: %s%n",
                    key, ov.getPeerId(), fq2List(fq));
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }
    
    static void query2(Overlay<?, ?> ov, String key) {
        String query = String.format("\"%s\"", key);
        FutureQueue<?> fq;
        try {
            fq = ov.request(appId, appId, query, null, 10000);
            System.out.printf("** query \"%s\" from %s: %s%n",
                    query, ov.getPeerId(), fq2List(fq));
        } catch (ParseException e) {
            System.err.println(e);
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    public static void main(String[] args) throws IOException {
        Net ntype = Net.UDP;
        int numPeer = 10;
        int seedPeerNo = 0;
        Peer[] peers = new Peer[numPeer];
        @SuppressWarnings("unchecked")
        Overlay<Key, Key>[] ovs = new Overlay[numPeer];
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
                ovs[i].addKey(appId, new StringKey("dolr-" + i));
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
            Overlay<Key, Key> ov = ovs[next(numPeer)];
            query(ov, new StringKey("dolr-" + next(numPeer)));
            query2(ov, "dolr-" + next(numPeer));
        }
        etime = System.currentTimeMillis();
        printf("=> took %d msec%n", (etime-stime));
        
        printf("%n** fin%n");
        sleep(200);
        for (int i = numPeer - 1; i >= 0; i--) {
            peers[i].fin();
        }
        printf("** end **%n");
    }
}
