package test.trans;

import java.io.IOException;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.DiscoveryListener;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.PeerInfo;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.adhoc.AdHocTransport;
import org.piax.gtrans.tsd.TSDDiscoverable;

import test.Util;

public class AdHocPeer<E extends PeerLocator> extends Util {

    /**
     * AdHocTransportのsenderまたはreceiverとなるアプリクラス。
     * inner classとして定義する。
     */
    class App implements TransportListener<E>, ChannelListener<E>,
            DiscoveryListener<E> {
        
        String me = peer.getPeerId() + ":" + appId.toString();
        
        public void onDiscovered(PeerInfo<E> peer, boolean isNew) {
            if (isNew) {
                printf("(%s) peer %s newly discovered.%n", me, peer);
                try {
                    adhoc.send(appId, appId, peer.getEndpoint(), "hello");
                } catch (ProtocolUnsupportedException e) {
                    System.out.println(e);
                } catch (IOException e) {
                    System.out.println(e);
                }
            } else {
                printf("(%s) peer %s discovered.%n", me, peer);
            }
        }

        public void onFadeout(PeerInfo<E> peer) {
            printf("(%s) peer %s fadeout.%n", me, peer);
        }

        public boolean onAccepting(Channel<E> ch) {
            printf("(%s) accept channel %d%n", me, ch.getChannelNo());
            return true;
        }

        public void onClosed(Channel<E> ch) {
            printf("(%s) closed channel %d%n", me, ch.getChannelNo());
        }

        public void onFailure(Channel<E> ch, Exception cause) {
            printf("(%s) fail channel %d%n", me, ch.getChannelNo());
        }

        public void onReceive(Channel<E> ch) {
            String msg = (String) ch.receive();
            printf("(%s) ch-%d received msg from %s: %s%n", me,
                    ch.getChannelNo(), ch.getRemote(), msg);
        }

        public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
            printf("(%s) received msg from %s: %s%n", me,
                    rmsg.getSource(), rmsg.getMessage());
        }
    }

    final Peer peer;
    ChannelTransport<E> tr;
    final AdHocTransport<E> adhoc;
    ObjectId appId = new ObjectId("app");
    App app;
    
    AdHocPeer(String pre, int peerNo, Net ntype) throws IdConflictException,
            IOException {
        peer = Peer.getInstance(new PeerId(pre + peerNo));
        
        // init overlays
        tr = peer.newBaseChannelTransport(
                Util.<E>genLocator(ntype, "localhost", 10000 + peerNo));
        // create AdHocTransport
        TransportId transId = new TransportId("adhoc");
        adhoc = new AdHocTransport<E>(transId, tr, TSDDiscoverable.Type.MULTICAST);
        app = new App();
        adhoc.addDiscoveryListener(app);
        adhoc.setListener(appId, app);
        adhoc.setChannelListener(appId, app);
    }
    
    AdHocPeer(String pre, int peerNo) throws IdConflictException, IOException {
        peer = Peer.getInstance(new PeerId(pre + peerNo));
        
        // create Bluetooth AdHocTransport
        TransportId transId = new TransportId("adhoc");
        adhoc = new AdHocTransport<E>(peer, transId);
        app = new App();
        adhoc.addDiscoveryListener(app);
        adhoc.setListener(appId, app);
        adhoc.setChannelListener(appId, app);
    }

    void fin() {
//        adhoc.removeDiscoveryListener(app);
//        adhoc.setListener(appId, null);
//        adhoc.setChannelListener(appId, null);
//        adhoc.fin();
//        if (tr != null)
//            tr.fin();
        peer.fin();
    }

    public static void main(String[] args) throws Exception {

        // Bluetoothを使う場合は、同じマシンで複数上げれないので1をセットする
        int num = 3;    
        AdHocPeer<?>[] peers = new AdHocPeer[num];
        
        printf("-- AdHocTransport test --%n");
        printf("-- init peers --%n");
        for (int i = 0; i < num; i++) {
            peers[i] = new AdHocPeer<PeerLocator>("p", i, Net.UDP);
//            peers[i] = new AdHocPeer<BluetoothLocator>("p", i); // for Bluetooth
        }
        
        printf("-- start discovery --%n");
        for (int i = 0; i < num; i++) {
            // Bluetoothを使う場合は、12秒以上の間隔でdiscoveryをしないと前のdiscoveryと重なる
            peers[i].adhoc.scheduleDiscovery(1000 + i * 100, 15000);
        }
        
        sleep(30000);
        printf("-- stop discovery --%n");
        for (int i = 0; i < num; i++) {
            peers[i].adhoc.cancelDiscovery();
        }
        printf("-- available peers --%n");
        for (int i = 0; i < num; i++) {
            printf("peer %s availables: %s%n", peers[i].peer.getPeerId(),
                    peers[i].adhoc.getAvailablePeerInfos());
        }

        sleep(1000);
        printf("%n-- test fin --%n");
        for (int i = 0; i < num; i++) {
            peers[i].fin();
        }
    }
}
