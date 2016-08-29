package test.trans;

import java.io.IOException;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.util.FragmentationTransport;
import org.piax.gtrans.util.Fragments;
import org.piax.gtrans.util.ThroughTransport;

import test.Util;

public class TestFragment extends Util {

    /**
     * BaseTransportのsenderまたはreceiverとなるアプリクラス
     */
    static class App<E extends PeerLocator> implements TransportListener<E>,
            ChannelListener<E> {
        final ObjectId appId;

        App(String id) {
            appId = new ObjectId(id);
        }

        public boolean onAccepting(Channel<E> ch) {
            printf("(%s) new ch-%d accepted from %s%n", appId,
                    ch.getChannelNo(), ch.getRemoteObjectId());
            return true;
        }

        public void onClosed(Channel<E> ch) {
            printf("(%s) ch-%d closed via %s%n", appId, ch.getChannelNo(),
                    ch.getRemoteObjectId());
        }

        public void onFailure(Channel<E> ch, Exception cause) {
        }

        public void onReceive(Channel<E> ch) {
            String msg = (String) ch.receive();
            printf("(%s) ch-%d received msg from %s: %s%n", appId,
                    ch.getChannelNo(), ch.getRemoteObjectId(), msg);
        }

        public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
            printf("(%s) received msg from %s: %s%n", appId,
                    rmsg.getSender(), rmsg.getMessage());
        }
    }

    static class Probe<E extends PeerLocator> extends ThroughTransport<E> {
        public Probe(TransportId transId, ChannelTransport<E> trans)
                throws IdConflictException {
            super(transId, trans);
        }

        @Override
        public int getMTU() {
            return 10;
        }
        
        @Override
        protected Object _preSend(ObjectId sender, ObjectId receiver,
                E dst, Object msg) throws IOException {
//            int p = next(100);
//            if (p == 0) throw new IOException("lost");
//            else sleep(p/10);
            
//            byte[] b = (byte[]) msg;
//            printf("==> %d %s%n", b.length, ByteUtil.dumpBytes(b));
            return msg;
        }

        @Override
        protected Object _postReceive(ObjectId sender, ObjectId receiver,
                E src, Object msg) {
            msgs++;
//            byte[] b = (byte[]) msg;
//            printf("<== %d %s%n", b.length, ByteUtil.dumpBytes(b));
            return msg;
        }
    }
    static int msgs = 0;

    public static <E extends PeerLocator> void main(String[] args) throws IOException {
        Net ntype = Net.UDP;
        printf("- start -%n");
        printf("- locator type: %s%n", ntype);

        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // sender, receiverとなるAppを生成する
        App<E> app1 = new App<E>("app1");
        App<E> app2 = new App<E>("app2");

        // BaseTransportを生成する
        ChannelTransport<E> tr1, tr2;
        try {
            tr1 = p1.newBaseChannelTransport(
                    Util.<E>genLocator(ntype, "localhost", 10001));
            tr1 = new Probe<E>(new TransportId("throw"), tr1);
            tr1 = new FragmentationTransport<E>(new TransportId("dfrag"), tr1);
            tr2 = p2.newBaseChannelTransport(
                    Util.<E>genLocator(ntype, "localhost", 10002));
            tr2 = new Probe<E>(new TransportId("throw"), tr2);
            tr2 = new FragmentationTransport<E>(new TransportId("dfrag"), tr2);
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }

        // BaseTransportに、Listenerをセットする
        tr1.setListener(app1.appId, app1);
        tr2.setListener(app2.appId, app2);
        tr1.setChannelListener(app1.appId, app1);
        tr2.setChannelListener(app2.appId, app2);
        Channel<E> ch = tr1.newChannel(app1.appId, app2.appId, tr2.getEndpoint());

        // 文字列を送信する
        for (int i = 0; i < 10; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 100; j++) {
                sb.append("" + i);
            }
            try {
                tr1.send(app1.appId, app2.appId, tr2.getEndpoint(), sb.toString());
                ch.send(sb.toString());
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        
        sleep(100);
        p1.fin();
        p2.fin();
        printf("- end -%n");
        printf("-- msgs %d%n", msgs);
        printf("-- losses %d%n", Fragments.losses);
        printf("-- duplicated %d%n", Fragments.duplicated);
        printf("-- skipped %d%n", Fragments.skipped);
    }
}
