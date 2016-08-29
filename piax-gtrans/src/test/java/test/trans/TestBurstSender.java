package test.trans;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;

import test.Util;

public class TestBurstSender extends Util {
    
    static ExecutorService threadPool;

    static void waitForExecFin() {
        threadPool.shutdown();
        try {
            // 最大2分待つ
            if (!threadPool.awaitTermination(120000, TimeUnit.MILLISECONDS)) {
                printf("some tasks not terminated%n");
            }
        } catch (InterruptedException ignore) {
        }
    }
    
    static void send(ChannelTransport<PeerLocator> tr1, ObjectId app,
            PeerLocator dst, byte[] msg) {
        try {
            tr1.send(app, app, dst, msg);
        } catch (ProtocolUnsupportedException e) {
            System.out.println(e);
        } catch (IOException e) {
            System.out.println(e);
        }
    }
    
    static void chSend(Channel<?> ch, byte[] msg) {
        try {
            ch.send(msg);
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    static void asyncSend(final ChannelTransport<PeerLocator> tr1,
            final ObjectId app, final PeerLocator dst, final byte[] msg) {
        threadPool.execute(new Runnable() {
            public void run() {
                send(tr1, app, dst, msg);
            }
        });
    }

    static void asyncChSend(final Channel<?> ch, final byte[] msg) {
        threadPool.execute(new Runnable() {
            public void run() {
                chSend(ch, msg);
            }
        });
    }
    
    public static void main(String[] args) throws IOException {
        Net ntype = Net.UDP;
        String srcAddr = "localhost";
        int srcPort = 10001;
        String dstAddr = "localhost";
//        String dstAddr = "192.168.2.10";
        int dstPort = 10002;
        boolean useCh = true;
        int n = 1000;
        int paraNum = 1;
        int size = 100000;
        printf("- start -%n");
        printf("- locator type: %s%s%n", ntype, (useCh ? ", use Channel" : ""));
        printf("- send data: %,d bytes%n", size);
        printf("- send count: %,d (%d parallel)%n", n * paraNum, paraNum);

        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));

        // Appを生成する
        ObjectId appId = new ObjectId("app");

        // BaseTransportを生成する
        ChannelTransport<PeerLocator> tr1;
        try {
            tr1 = p1.newBaseChannelTransport(
                    genLocator(ntype, srcAddr, srcPort));
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }
        PeerLocator dst = genLocator(ntype, dstAddr, dstPort);

        long stime = 0, etime;
        long lattime = 0;
        int dumNum = 5;
        int grain = n / 50;
        if (grain == 0) grain = 1;
        
        // msgを用意する
        byte[] msg = new byte[size];
        for (int i = 0; i < msg.length; i++) {
            msg[i] = (byte) i;
        }
        
        Channel<?> ch = null;
        threadPool = Executors.newFixedThreadPool(paraNum);
        if (useCh) {
            ch = tr1.newChannel(appId, appId, dst);
            for (int i = 0; i < n + dumNum; i++) {
                if (i == dumNum) {
                    stime = System.currentTimeMillis();
                }
                if (i >= dumNum && (i - dumNum + 1) % grain == 0) {
                    System.out.print('.');
                }
                if (i == n + dumNum - 1) {
                    long s = System.nanoTime();
                    ch.send(msg);
                    lattime = System.nanoTime() - s;
                } else {
                    ch.send(msg);
                }
                for (int j = 0; j < paraNum - 1; j++) {
                    asyncChSend(ch, msg);
                }
            }
        } else {
            for (int i = 0; i < n + dumNum; i++) {
                if (i == dumNum) {
                    stime = System.currentTimeMillis();
                }
                if (i >= dumNum && (i - dumNum + 1) % grain == 0) {
                    System.out.print('.');
                }
                if (i == n + dumNum - 1) {
                    long s = System.nanoTime();
                    tr1.send(appId, appId, dst, msg);
                    lattime = System.nanoTime() - s;
                } else {
                    tr1.send(appId, appId, dst, msg);
                }
                for (int j = 0; j < paraNum - 1; j++) {
                    asyncSend(tr1, appId, dst, msg);
                }
            }
        }
        waitForExecFin();
        etime = System.currentTimeMillis();

        sleep(1000);
        long unitUsec = (etime-stime) * 1000L / (n * paraNum);
        long latencyUsec = lattime / 1000L;
        printf("%n%n=> lap: %,d msec, throughput: %,d KB/s, latency: %,d usec%n",
                etime - stime, size * 1000L / unitUsec, latencyUsec);
        if (ch != null) ch.close();
        p1.fin();
        printf("- end -%n");
    }
}
