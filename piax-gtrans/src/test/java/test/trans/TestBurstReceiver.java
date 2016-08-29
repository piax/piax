package test.trans;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelListener;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;

import test.Util;

public class TestBurstReceiver extends Util {
    static int COUNT_EXPIRE_TIME = 1000;
    
    static class App<E extends PeerLocator> implements TransportListener<E>,
            ChannelListener<E> {
        final ObjectId appId;
        AtomicInteger count = new AtomicInteger(0);
        long startTime = 0;
        long lastTime = System.currentTimeMillis();

        App(String id) {
            appId = new ObjectId(id);
        }

        public boolean onAccepting(Channel<E> ch) {
            printf("(%s) new ch-%d accepted from %s%n", appId,
                    ch.getChannelNo(), ch.getRemote());
            startTime = System.currentTimeMillis();
            count.set(0);
            return true;
        }

        public void onClosed(Channel<E> ch) {
            printf("%n=> lap: %,d msec, %,d msgs received%n",
                    lastTime - startTime, count.get());
            printf("(%s) ch-%d closed via %s%n", appId, ch.getChannelNo(),
                    ch.getRemote());
        }

        public void onFailure(Channel<E> ch, Exception cause) {
        }

        void proc(byte[] msg) {
            long curr = System.currentTimeMillis();
            if (curr - lastTime > COUNT_EXPIRE_TIME) {
                int n = count.get();
                if (n != 0) {
                    printf("%n=> lap: %,d msec, %,d msgs received%n",
                            lastTime - startTime, n);
                    startTime = curr;
                    count.set(0);
                }
            }
            int n = count.getAndIncrement();
            if (n == 0) {
                printf("start: %,d bytes data%n", msg.length);
            }
            if (n % 100 == 0) printf(".");
            if ((n + 1) % 5000 == 0) printf("\n");
            lastTime = curr;
        }
        
        public void onReceive(Channel<E> ch) {
            byte[] msg = (byte[]) ch.receive();
            proc(msg);
        }

        public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
            byte[] msg = (byte[])rmsg.getMessage();
            proc(msg);
        }
    }
    
    public static void main(String[] args) throws IOException {
        Net ntype = Net.UDP;
        String addr = "localhost";
        int port = 10002;
        
        printf("- start -%n");
        printf("- locator type: %s%n", ntype);

        // peerを用意する
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // sender, receiverとなるAppを生成する
        App<PeerLocator> app2 = new App<PeerLocator>("app");

        // BaseTransportを生成する
        ChannelTransport<PeerLocator> tr2;
        try {
            tr2 = p2.newBaseChannelTransport(genLocator(ntype, addr, port));
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }

        // BaseTransportに、Listenerをセットする
        tr2.setListener(app2.appId, app2);
        tr2.setChannelListener(app2.appId, app2);
        
        sleep(5 * 60 * 1000);
        p2.fin();
        printf("- end -%n");
    }
}
