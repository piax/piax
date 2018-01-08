package test.rpc;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.RemoteCallable;

import test.Util;

public class RPCBench extends Util {
    
    public interface SAppIf extends RPCIf {
        @RemoteCallable
        int simple(int s) throws RPCException;
        @RemoteCallable
        int large(Data data) throws RPCException;
    }
    
    public interface AppIf extends SAppIf {
//        @RemoteCallable(Type.ONEWAY)
        @RemoteCallable
        void longSleep(int stime) throws RPCException;
        
        @RemoteCallable
        void dummy(int n, String... s) throws RPCException;
    }

    static class App<E extends PeerLocator> extends RPCInvoker<AppIf, E>
            implements AppIf {
        App(TransportId transId, ChannelTransport<E> trans) throws IOException,
                IdConflictException {
            super(transId, trans);
        }

        public void longSleep(int stime) {
            System.out.println("I will sleep now: " + stime);
            sleep(stime);
        }

        public int simple(int s) {
            return 0;
        }
        
        public int large(Data data) {
            return 0;
        }

        public void dummy(int n, String... s) {
            System.out.println("dummy called: " + n + ", " + Arrays.toString(s));
        }
    }

    public interface ObjIf2 extends RPCIf {
        @RemoteCallable
        String concat(Number... s) throws RPCException;
    }
    
    static class Hoge implements ObjIf2 {
        public String concat(Number... s) {
            return "haaaha:" + Arrays.toString(s);
        }
    }

    @SuppressWarnings("serial")
    public static class Data implements Serializable {
        byte[] data;
        Data(int size) {
            data = new byte[size];
        }
     }
     

    public static <E extends PeerLocator> void main(String[] args) {
        Net ntype = Net.NETTY;
        printf("- start -%n");
        printf("- locator type: %s%n", ntype);

        // peerを用意する
        Peer p1 = Peer.getInstance(new PeerId("p1"));
        Peer p2 = Peer.getInstance(new PeerId("p2"));

        // BaseTransportを生成する
        ChannelTransport<E> tr1, tr2;
        try {
            tr1 = p1.newBaseChannelTransport(
                    Util.<E>genLocator(ntype, "localhost", 10001));
            tr2 = p2.newBaseChannelTransport(
                    Util.<E>genLocator(ntype, "localhost", 10002));
        } catch (IOException e) {
            System.out.println(e);
            return;
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }

        // app
        TransportId appId = new TransportId("app");

        App<E> a1;
        App<E> a2;
        try {
            a1 = new App<E>(appId, tr1);
            a2 = new App<E>(appId, tr2);
        } catch (IOException e) {
            System.out.println(e);
            return;
        } catch (IdConflictException e) {
            System.out.println(e);
            return;
        }
        
        final AppIf stub = a1.getStub(tr2.getEndpoint());

        int LARGE = 100*1024;
        final Data data = new Data(LARGE);
        
        final Runnable simple = new Runnable() {
            @Override
            final public void run() {
                try {
                    stub.simple(1);
                } catch (RPCException e) {
                    System.out.println(e);
                }
            }
        };
        final Runnable large = new Runnable() {
            @Override
            final public void run() {
                try {
                    stub.large(data);
                } catch (RPCException e) {
                    System.out.println(e);
                }
            }
        };

        for (int i = 0; i < 4; i++) {
            /* RPC with small arguments */
            repeat(1000, "RPC(simple)", simple);
            /* RPC with large arguments */
            repeat(1000, "RPC(" + (LARGE / 1024) + "KB)", large);
        }

        printf("- end1 -%n");
        a1.fin();
        a2.fin();
        p1.fin();
        p2.fin();
        printf("- end -%n");
    }

    private static void repeat(final int count, String title, Runnable run) {
        long[] t = new long[count];
        for (int i = 0; i < count; i++) {
            System.gc();
            t[i] = i;
            long start = System.nanoTime();
            run.run();
            long end = System.nanoTime();
            t[i] = (end - start) / 1000;
        }
        
        System.out.println(title);
        System.out.println(" 1st invocation [0]: " + t[0] + " usec");
        do {
            if (count <= 1) break;
            System.out.println("  ave. [   1..LAST]: " + average(t, 1) + " usec");
            if (count <= 1000) break;
            System.out.println("  ave. [1000..LAST]: " + average(t, 1000) + " usec");
            if (count <= 5000) break;
            System.out.println("  ave. [5000..LAST]: " + average(t, 5000) + " usec");
        } while (false);
        
        if (false) {
            for (int i = 0; i < count; i += 16) {
                System.out.print(i + ": ");
                for (int j = 0; j < 16 && i + j < count; j++) {
                    System.out.print(t[i + j] + " ");
                }
                System.out.println();
            }
        }
    }

    private static long average(long[] data, int start) {
        long sum = 0;
        for (int i = start; i < data.length; i++) {
            sum += data[i];
        }
        return sum / (data.length - start); 
    }
}
