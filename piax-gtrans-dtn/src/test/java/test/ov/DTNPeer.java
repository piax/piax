package test.ov;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.common.wrapper.IntegerKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.Transport;
import org.piax.gtrans.adhoc.AdHocTransport;
import org.piax.gtrans.dtn.DTNNode;
import org.piax.gtrans.dtn.SimpleDTN;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.tsd.TSDDiscoverable;

public class DTNPeer<D extends Destination, K extends Key> {

    static void printf(String f, Object... args) {
        System.out.printf(f, args);
    }

    static void sleep(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ignore) {
        }
    }

    enum Net {
        EMU, UDP, TCP
    }

    @SuppressWarnings("unchecked")
    static <E extends PeerLocator> E genLocator(Net net, String host, int port) {
        PeerLocator loc;
        switch (net) {
        case EMU:
            loc = new EmuLocator(port);
            break;
        case UDP:
            loc = new UdpLocator(new InetSocketAddress(host, port));
            break;
        case TCP:
            loc = new TcpLocator(new InetSocketAddress(host, port));
            break;
        default:
            loc = null;
        }
        return (E) loc;
    }

    static KeyRanges<IntegerKey> query = new KeyRanges<IntegerKey>(
            new KeyRange<IntegerKey>(new IntegerKey(4), new IntegerKey(10)));

    static ObjectId appId = new ObjectId("app");

    class App implements OverlayListener<D, K> {
        public void onReceive(Overlay<D, K> trans, OverlayReceivedMessage<K> rmsg) {
            printf("%s: receive msg %s %n", peer.getPeerId(), rmsg.getMessage());
        }

        public FutureQueue<?> onReceiveRequest(Overlay<D, K> trans,
                OverlayReceivedMessage<K> rmsg) {
            throw new UnsupportedOperationException();
        }

        // unused
        public void onReceive(Transport<D> trans, ReceivedMessage rmsg) {
        }
        public void onReceive(RequestTransport<D> trans, ReceivedMessage rmsg) {
        }
        public FutureQueue<?> onReceiveRequest(RequestTransport<D> trans,
                ReceivedMessage rmsg) {
            return null;
        }
    }

    final Peer peer;
    final ChannelTransport<PeerLocator> tr;
    final AdHocTransport<PeerLocator> adhoc;
    final SimpleDTN<D, K> dtn;

    DTNPeer(String pre, int peerNo, Net ntype) throws IdConflictException,
            IOException {
        PeerId peerId = new PeerId(pre + peerNo);
        peer = Peer.getInstance(peerId);

        // init DTN
        tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", 10000 + peerNo));

        // create AdHocTransport
        TransportId transId = new TransportId("adhoc");
        adhoc = new AdHocTransport<PeerLocator>(transId, tr,
                TSDDiscoverable.Type.MULTICAST);
        adhoc.scheduleDiscovery(500, 1000);
        dtn = new SimpleDTN<D, K>(adhoc);
        App app = new App();
        dtn.setListener(appId, app);
        dtn.join((Endpoint) null);
    }

    void fin() {
        adhoc.cancelDiscovery();
//        dtn.fin();
//        adhoc.fin();
//        tr.fin();
        peer.fin();
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        TSDDiscoverable.DEFAULT_EXPIRATION_TIME = 3000;
        DTNNode.MAINTENANCE_INTERVAL = 2000;
        DTNNode.MAX_EXPIRATION_TIME = 10000;
        DTNNode.MAX_HOPS = 7;
        DTNNode.MSG_TTL = 120000;

        int num = 5;
        @SuppressWarnings("unchecked")
        DTNPeer<Destination, Key>[] peers = new DTNPeer[num];

        printf("-- DTN test --%n");

        for (int i = 0; i < num + 2; i++) {
            // peer i - 2 を終了させる
            int j = i - 2;
            if (0 <= j && j < num) {
                printf("-- stop peer %s --%n", peers[j].peer.getPeerId());
                printf("** msg table %s:%n%s", peers[j].peer.getPeerId(), peers[j].dtn.dumpStoredMsgs());
                peers[j].fin();
            }
            // peer i を起動する
            if (0 <= i && i < num) {
                printf("-- start peer %s --%n", "p" + i);
                peers[i] = new DTNPeer<Destination, Key>("p", i, Net.TCP);
//                peers[i].dtn.addKey(appId, i);
            }
            // peer 0 を起動した時にはsendも行う
            if (i == 0) {
                printf("** send msg \"Hello\" from %s --%n", peers[i].peer.getPeerId());
                peers[0].dtn.send(appId, appId, query, "Hello");
            }
            sleep(5000);
        }

        printf("%n-- restart peer p0 and dump msg table --%n");
        peers[0] = new DTNPeer<Destination, Key>("p", 0, Net.UDP);
        printf("** msg table %s:%n%s", peers[0].peer.getPeerId(), peers[0].dtn.dumpStoredMsgs());
        sleep(1000);
        peers[0].fin();
        printf("%n-- test fin --%n");
    }
}
