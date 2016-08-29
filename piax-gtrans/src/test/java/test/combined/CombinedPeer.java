package test.combined;

import java.io.IOException;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.Key;
import org.piax.common.Location;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.common.attribs.RowData;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.combined.CombinedOverlay;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.sg.MSkipGraph;

import test.Util;

public class CombinedPeer extends Util {

    //--- test data
    // attributes
    static String[] attribs = new String[] {"city", "pop", "loc", "cityId"};
    static Id cid1 = Id.newId(16);
    static Id cid2 = Id.newId(16);
    
    // for p0
    static Object[][] rowData0 = new Object[][] {
        {"奈良市", 366528, new Location(135.836105, 34.679359), Id.newId(16)},
        {"京都市", 1474473, new Location(135.754807, 35.009129), cid1},
    };
    
    // for p1
    static Object[][] rowData1 = new Object[][] {
        {"舞鶴市", 88681, new Location(135.332901, 35.441528), Id.newId(16)},
        {"大阪市", 2666371, new Location(135.496505, 34.702509), cid2},
        {"和歌山市", 369400, new Location(135.166107, 34.221981), null},
        {"神戸市", 1544873, new Location(135.175479, 34.677484), Id.newId(16)},
        {"姫路市", 536338, new Location(134.703094, 34.829361), null},
        {"津市", 285728, new Location(136.516800, 34.719109), Id.newId(16)},
    };
    
    // dcond
    static String[] dconds = new String[] {
        "pop in (0..300000)",
        "pop in (0..300000) and loc in rect(135, 34.5, 1, 1)",
        "pop in [1000000..10000000)",
        "loc in rect(135, 34.5, 1, 1)",
        "cityId eq 0x" + cid1.toHexString() + "G",
        "pop in (300000..) and cityId eq 0x" + cid2.toHexString() + "G",
        "pop eq maxLower(1000000)",
        "pop in lower(1000000, 10)",
    };

    static ObjectId appId = new ObjectId("app");

    class App implements OverlayListener<Destination, Key> {
        public void onReceive(Overlay<Destination, Key> ov,
                OverlayReceivedMessage<Key> rmsg) {
        }

        public FutureQueue<?> onReceiveRequest(Overlay<Destination, Key> ov,
                OverlayReceivedMessage<Key> rmsg) {
            FutureQueue<Object> fq = new FutureQueue<Object>();
            for (Key k : rmsg.getMatchedKeys()) {
                Object ret = ((RowData) k).getAttribValue((String) rmsg.getMessage());
                fq.add(new RemoteValue<Object>(peer.getPeerId(), ret));
            }
            fq.setEOFuture();
            return fq;
        }
        
        // unused
        public void onReceive(Transport<Destination> trans, ReceivedMessage msg) {
        }
        public void onReceive(RequestTransport<Destination> trans,
                ReceivedMessage rmsg) {
        }
        public FutureQueue<?> onReceiveRequest(
                RequestTransport<Destination> trans, ReceivedMessage rmsg) {
            return null;
        }
    }

    final Peer peer;
    final ChannelTransport<?> tr;
    final Overlay<Destination, ComparableKey<?>> sg;
    final LLNet llnet;
    final CombinedOverlay comb;
    
    CombinedPeer(int peerNo, String tableName, Net ntype)
            throws IdConflictException, IOException {
        PeerId peerId = new PeerId("p" + peerNo);
        peer = Peer.getInstance(peerId);
        
        // init overlays
        tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", 10000 + peerNo));
        sg = new MSkipGraph<Destination, ComparableKey<?>>(tr);
        llnet = new LLNet(sg);
        
        // create table and CombinedOverlay
        comb = new CombinedOverlay(peer, new TransportId(tableName));
        App app = new App();
        comb.setListener(appId, app);
        
        // declare attribute and bind overlay
        comb.declareAttrib("city");
        comb.declareAttrib("pop", Integer.class);
        comb.declareAttrib("loc", Location.class);
        comb.declareAttrib("cityId", Id.class);
        try {
            comb.bindOverlay("pop", sg.getTransportIdPath());
            comb.bindOverlay("loc", llnet.getTransportIdPath());
            comb.bindOverlay("cityId", sg.getTransportIdPath());
        } catch (NoSuchOverlayException e) {
            System.out.println(e);
        } catch (IncompatibleTypeException e) {
//            e.printStackTrace();
            System.out.println(e);
        }
    }
    
    void leave() throws IOException {
        sg.leave();
        llnet.leave();
    }
    
    void fin() {
//        comb.fin();
//        llnet.fin();
//        sg.fin();
//        tr.fin();
        peer.fin();
    }

    void join(Endpoint seed) throws IOException {
        sg.join(seed);
        llnet.join(seed);
    }
    
    void setRows(String[] attribs, Object[][] rowData) {
        for (int i = 0; i < rowData.length; i++) {
            Id id = new Id("id" + i);
            try {
                RowData row = comb.newRow(id);
                for (int j = 0; j < rowData[i].length; j++) {
                    Object val = rowData[i][j];
                    if (val != null)
                        row.setAttrib(attribs[j], val, true);
                }
            } catch (IdConflictException e) {
                System.out.println(e);
            } catch (IncompatibleTypeException e) {
                System.out.println(e);
            }
        }
    }
    
    void removeRow(Id rowId) {
        comb.removeRow(rowId);
    }
    
    void printTable() {
        System.out.println(comb.table);
    }

    void dumpTable() {
        for (String att : comb.getDeclaredAttribNames()) {
            System.out.println(comb.table.getAttrib(att));
        }
        for (RowData row : comb.getRows()) {
            System.out.println(row);
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        printf("-- CombinedOverlay test --%n");

        printf("-- init peers --%n");
        CombinedPeer root = new CombinedPeer(0, "attribtab", Net.UDP);
        CombinedPeer peer = new CombinedPeer(1, "attribtab", Net.UDP);
        
//        printf("-- join peers --%n");
//        root.activate(root.tr.getEndpoint());
//        peer.activate(root.tr.getEndpoint());
//        peer.dumpTable();
        
        printf("-- set attributes at each peer --%n");
        root.setRows(attribs, rowData0);
        peer.setRows(attribs, rowData1);
        root.printTable();
        peer.printTable();
//        peer.dumpTable();
        
        printf("-- join peers --%n");
        root.join(root.tr.getEndpoint());
        peer.join(root.tr.getEndpoint());
        root.dumpTable();
        peer.dumpTable();
        
        printf("-- invoke request --%n");
        for (int i = 0; i < dconds.length; i++) {
            FutureQueue<?> fq = root.comb.request(appId, appId, dconds[i], "city", 1000);
            printf("\"%s\" => %s%n", dconds[i], fq2List(fq));
        }

        printf("-- remove 大阪市 --%n");
        peer.removeRow(new Id("id" + 1));
        
        printf("-- invoke same request --%n");
        for (int i = 0; i < dconds.length; i++) {
            FutureQueue<?> fq = root.comb.request(appId, appId, dconds[i], "city", 1000);
            printf("\"%s\" => %s%n", dconds[i], fq2List(fq));
        }

        printf("%n-- test fin --%n");

        peer.comb.table.clear();
        sleep(50);
        peer.leave();
        root.leave();
        peer.fin();
        root.fin();
    }
}
