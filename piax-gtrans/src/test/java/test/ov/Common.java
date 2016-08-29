package test.ov;

import java.io.IOException;

import org.piax.common.Destination;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.dcl.parser.ParseException;
import org.piax.common.subspace.GeoRectangle;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.common.wrapper.IntegerKey;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;

import test.Util;

public class Common extends Util {
    
    static class App<D extends Destination, K extends Key> implements
            OverlayListener<D, K> {
        PeerId me;
        public App(PeerId me) {
            this.me = me;
        }

        public void onReceive(Overlay<D, K> trans,
                OverlayReceivedMessage<K> rmsg) {
            printf("(%s) received msg from %s: %s%n", me, rmsg.getSource(),
                    rmsg.getMessage());
        }

        public FutureQueue<?> onReceiveRequest(Overlay<D, K> trans,
                OverlayReceivedMessage<K> rmsg) {
            FutureQueue<Key> fq = new FutureQueue<Key>();
            for (Key k : rmsg.getMatchedKeys()) {
                fq.add(new RemoteValue<Key>(me, k));
            }
            fq.setEOFuture();
            return fq;
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

    static ObjectId appId = new ObjectId("app");

    static void send(Overlay<? super PeerId, ?> ov, PeerId dst, String msg) {
        try {
            ov.send(appId, appId, dst, msg);
            System.out.printf("** send to %s from %s: %n",
                    dst, ov.getPeerId(), msg);
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    static void rangeQuery(Overlay<? super KeyRanges<?>, ?> ov, int from, int to) {
        KeyRange<IntegerKey> range = new KeyRange<IntegerKey>(
                new IntegerKey(Math.min(from, to)), 
                new IntegerKey(Math.max(from, to)));
        KeyRanges<IntegerKey> rq = new KeyRanges<IntegerKey>(range);
        FutureQueue<?> fq;
        try {
            fq = ov.request(appId, appId, rq, null, 10000);
            System.out.printf("** range query %s from %s: %s%n",
                    range, ov.getPeerId(), fq2List(fq));
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }
    
    static void rangeQuery2(Overlay<?, ?> ov, int from, int to) {
        String range = String.format("[%d..%d]", Math.min(from, to),
                Math.max(from, to));
        FutureQueue<?> fq;
        try {
            fq = ov.request(appId, appId, range, null, 10000);
            System.out.printf("** range query \"%s\" from %s: %s%n",
                    range, ov.getPeerId(), fq2List(fq));
        } catch (ParseException e) {
            System.err.println(e);
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    static void query(Overlay<? super GeoRectangle, ?> ov, double x, double y) {
        GeoRectangle rect = new GeoRectangle(x, y, 0.5, 0.5);
        FutureQueue<?> fq;
        try {
            fq = ov.request(appId, appId, rect, null, 10000);
            System.out.printf("** query %s from %s: %s%n",
                    rect, ov.getPeerId(), fq2List(fq));
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    static void query2(Overlay<?, ?> ov, double x, double y) {
        String region = String.format("rect(%f,%f,0.5,0.5)", x, y);
        FutureQueue<?> fq;
        try {
            fq = ov.request(appId, appId, region, null, 10000);
            System.out.printf("** query \"%s\" from %s: %s%n",
                    region, ov.getPeerId(), fq2List(fq));
        } catch (ParseException e) {
            System.err.println(e);
        } catch (ProtocolUnsupportedException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
