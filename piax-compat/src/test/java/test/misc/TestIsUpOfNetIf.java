package test.misc;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.tcp.TcpLocator;

import test.Util;

public class TestIsUpOfNetIf extends Util {
    private final static Timer maintainTimer = new Timer(true);
    final static int CHECK_INTERVAL = 200;

    private TimerTask maintainTask;
    Map<NetworkInterface, Boolean> netifs = new HashMap<NetworkInterface, Boolean>();

    public TestIsUpOfNetIf() {
        maintainTask = new TimerTask() {
            @Override
            public void run() {
                check();
            }
        };
        maintainTimer.schedule(maintainTask, CHECK_INTERVAL, CHECK_INTERVAL);
    }
    
    public void fin() {
        maintainTask.cancel();
    }
    
    public void addNetIf(NetworkInterface netif) {
        netifs.put(netif, false);
    }

    void check() {
        for (Map.Entry<NetworkInterface, Boolean> ent : netifs.entrySet()) {
            NetworkInterface nif = ent.getKey();
            boolean prevIsUp = ent.getValue();
            boolean isUp;
            try {
                isUp = nif.isUp();
//                printf("%s%n", isUp);
            } catch (SocketException e) {
                e.printStackTrace();
                continue;
            }
            if (prevIsUp == isUp) continue;
            netifs.put(nif, isUp);
            if (isUp) {
                printf("### %s is up%n", nif);
            } else {
                printf("### %s is down%n", nif);
            }
        }
    }

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
            // acceptしたchannelの場合だけ反応させる
            if (ch.isCreatorSide()) return;

            // 受信したメッセージをそのまま返送する
            String msg = (String) ch.receive();
            printf("(%s) ch-%d received msg from %s: %s%n", appId,
                    ch.getChannelNo(), ch.getRemoteObjectId(), msg);
        }

        public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
            printf("(%s) received msg from %s: %s%n", appId,
                    rmsg.getSender(), rmsg.getMessage());
        }
    }
    
    public static void main(String[] args) throws Exception {
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));
        App<InetLocator> app = new App<InetLocator>("app");
        InetSocketAddress addr1 = new InetSocketAddress("192.168.64.106", 10000);
        InetSocketAddress addr2 = new InetSocketAddress("192.168.71.1", 10000);
        InetLocator loc1 = new TcpLocator(addr1);
        InetLocator loc2 = new TcpLocator(addr2);
        ChannelTransport<InetLocator> tr1 = p1.newBaseChannelTransport(loc1);
        ChannelTransport<InetLocator> tr2 = p2.newBaseChannelTransport(loc2);
        tr2.setListener(app.appId, app);
        tr2.setChannelListener(app.appId, app);
        Channel<InetLocator> ch = tr1.newChannel(app.appId, app.appId, loc2);
        
        NetworkInterface netIf1 = NetworkInterface.getByInetAddress(loc1.getInetAddress());
        NetworkInterface netIf2 = NetworkInterface.getByInetAddress(loc2.getInetAddress());
        printf("net1=%s%n", netIf1);
        printf("net2=%s%n", netIf2);

        TestIsUpOfNetIf ob = new TestIsUpOfNetIf();
        ob.addNetIf(netIf1);
        ob.addNetIf(netIf2);
        for (int i = 0; i < 60; i++) {
            sleep(1000);
            try {
//                tr1.send(app.appId, app.appId, loc2, "hoge");
                ch.send("hoge");
            } catch (Exception e) {
                e.printStackTrace();
//                System.out.println(e);
            }
        }
//        waitForKeyin();
        ch.close();
        ob.fin();
        p1.fin();
        p2.fin();
    }
}
