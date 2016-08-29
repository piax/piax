package test.trans;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerIdWithLocator;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.handover.HandoverTransport;

import test.Util;

/**
 * HandoverTransportの機能をテストする
 * 
 * @author     Mikio Yoshida
 * @version    3.0.0
 */
public class TestHandover extends Util {

    /**
     * HandoverTransportのsenderまたはreceiverとなるアプリクラス
     */
    static class App<E extends Endpoint> implements TransportListener<E>,
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
            // acceptしたchannelの場合だけ反応させる
            if (ch.isCreatorSide()) return;

            // 受信したメッセージをそのまま返送する
            String msg = (String) ch.receive();
            printf("(%s) ch-%d received msg from %s: %s%n", appId,
                    ch.getChannelNo(), ch.getRemoteObjectId(), msg);
            printf("(%s) reply to %s via ch-%d: %s%n", appId,
                    ch.getRemoteObjectId(), ch.getChannelNo(), msg);
            try {
                ch.send(msg);
            } catch (IOException e) {
                System.err.println(e);
            }
        }

        public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
            printf("(%s) received msg from %s: %s%n", appId,
                    rmsg.getSender(), rmsg.getMessage());
        }
    }
    
    /**
     * test code
     * 
     * @param args
     */
    public static void main(String[] args) throws IOException {
        // Id
        TransportId hoId = new TransportId("hover");

        printf("-start-%n");
        
        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        
        // sender, receiverとなるAppを生成する
        App<PeerId> app1 = new App<PeerId>("app1");
        App<PeerId> app2 = new App<PeerId>("app2");

        // HandoverTransportを用意する
        HandoverTransport ho1, ho2;
        try {
            ho1 = new HandoverTransport(p1, hoId);
            ho1.setListener(app1.appId, app1);
            ho1.setChannelListener(app1.appId, app1);
        } catch (IdConflictException e) {
            System.err.println(e);
            return;
        }
        try {
            ho2 = new HandoverTransport(p2, hoId);
            ho2.setListener(app2.appId, app2);
            ho2.setChannelListener(app2.appId, app2);
        } catch (IdConflictException e) {
            System.err.println(e);
            return;
        }
        
        // locatorを用意する
        PeerLocator emu1 = genLocator(Net.EMU, "localhost", 20001);
        PeerLocator emu2 = genLocator(Net.EMU, "localhost", 20002);
        PeerLocator emu3 = genLocator(Net.EMU, "localhost", 20003);
        PeerLocator udp1 = genLocator(Net.UDP, "localhost", 10001);
        PeerLocator udp2 = genLocator(Net.UDP, "localhost", 10002);
        
        // ho1.emu1, ho2.emu2 として通信
        printf("%nsend ho1.emu1 <-> ho2.emu2%n");
        ho1.onEnabled(emu1, true);
        ho2.onEnabled(emu2, true);
        try {
            // 初めてp2に通信を行うときは、PeerIdの指定では例外が出る
            ho1.send(app1.appId, app2.appId, p2.getPeerId(), "123456");
        } catch (IOException e) {
            System.err.println(e);
        }
        // PeerIdWithLocatorを指定するとOK
        PeerId dstHo2 = new PeerIdWithLocator(p2.getPeerId(), emu2);
        ho1.send(app1.appId, app2.appId, dstHo2, "123456");
        // 折り返しでは、destinationにPeerId p1を指定できるようになる
        sleep(100);
        ho2.send(app2.appId, app1.appId, p1.getPeerId(), "abcdefghijk");
        
        // ho1.emu1をemu3にhandoverさせる
        sleep(10);
        printf("%nchange ho1.emu1 to emu3 and send ho1 <-> ho2.emu2%n");
        ho1.onChanging(emu1, emu3);
        // 直後の ho2からのメッセージは落ちる
        try {
            ho2.send(app2.appId, app1.appId, p1.getPeerId(), "abcdefghijk");
        } catch (IOException e) {
            System.err.println(e);
        }
        // こちらは成功する
        sleep(10);
        ho2.send(app2.appId, app1.appId, p1.getPeerId(), "abcdefghijk");
        ho1.send(app1.appId, app2.appId, p2.getPeerId(), "123456");
        
        // channelでもやってみる
        sleep(10);
        printf("%nsend ho1.emu3 <-> ho2.emu2 via channel%n");
        Channel<PeerId> ch1 = ho1.newChannel(app1.appId, app2.appId, p2.getPeerId());
        ch1.send("xyzzzz");
        String rep = (String) ch1.receive(1000);
        printf("(%s) ch-%d received reply from %s: %s%n", app1.appId,
                ch1.getChannelNo(), ch1.getRemoteObjectId(), rep);
        
        // ho2.emu2をemu1にhandoverさせる
        sleep(10);
        printf("%nchange ho2.emu2 to emu1 and send ho1 <-> ho2 via channel%n");
        ho2.onChanging(emu2, emu1);
        sleep(10);
        ch1.send("pqrstuv");
        rep = (String) ch1.receive(1000);
        printf("(%s) ch-%d received reply from %s: %s%n", app1.appId,
                ch1.getChannelNo(), ch1.getRemoteObjectId(), rep);
        
        // 次は2つ目のlocatorとして、udpを付与し、その後、emuを落としてみる
        sleep(10);
        printf("%nadd udp to each peer and down emu on p1%n");
        ho1.onEnabled(udp1, true);
        ho2.onEnabled(udp2, true);
        sleep(30);
        ho1.onFadeout(emu3, true);
        sleep(100);
        ch1.send("0987654321");
        rep = (String) ch1.receive(1000);
        printf("(%s) ch-%d received reply from %s: %s%n", app1.appId,
                ch1.getChannelNo(), ch1.getRemoteObjectId(), rep);
        
        sleep(1000);
        printf("%n-fin-%n");
//        ho1.fin();
//        ho2.fin();
        p1.fin();
        p2.fin();
        printf("-end-%n");
    }
}
