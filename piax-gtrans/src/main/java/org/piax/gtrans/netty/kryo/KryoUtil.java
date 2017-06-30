package org.piax.gtrans.netty.kryo;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.FTEntry;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.netty.ControlMessage;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
import org.piax.gtrans.ov.async.rq.RQRequest;
import org.piax.gtrans.ov.async.suzaku.Suzaku.ExecQueryAdapter;
import org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTEntReplyEvent;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.FTEntrySet;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

public class KryoUtil {
    static boolean DEBUG = false;

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            if (DEBUG) {
                Log.TRACE();
            }
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.register(Node.class, new NodeSerializer());
            kryo.register(LocalNode.class, new NodeSerializer());
            // the size becomes small if registered because registered classes are represented as a number, not name.
            kryo.register(ControlMessage.class);
            kryo.register(NettyMessage.class);
            kryo.register(NettyEndpoint.class);
            kryo.register(NettyLocator.class);
            kryo.register(Event.class);
            kryo.register(DdllKey.class);
            kryo.register(PeerId.class);
            kryo.register(NettyLocator.class);
            kryo.register(ArrayList.class);
            kryo.register(Lookup.class);
            kryo.register(TransportId.class);
            kryo.register(FTEntry.class);
            kryo.register(GetFTEntReplyEvent.class);
            kryo.register(DdllKeyRange.class);
            kryo.register(FTEntrySet.class);
            kryo.register(NestedMessage.class);
            kryo.register(ExecQueryAdapter.class);
            kryo.register(RQRequest.class); 
            
            kryo.register(org.piax.gtrans.netty.idtrans.PrimaryKey.class);
            kryo.register(org.piax.common.wrapper.DoubleKey.class);
            kryo.register(org.piax.common.wrapper.StringKey.class);
            kryo.register(org.piax.common.wrapper.ByteKey.class);
            kryo.register(org.piax.common.wrapper.IntegerKey.class);
            kryo.register(org.piax.common.wrapper.BooleanKey.class);
            kryo.register(org.piax.common.wrapper.LongKey.class);
            kryo.register(org.piax.gtrans.netty.NettyLocator.TYPE.class);
            kryo.register(org.piax.gtrans.netty.ControlMessage.ControlType.class);
            kryo.register(org.piax.gtrans.async.Event.AckEvent.class);
            kryo.register(org.piax.gtrans.async.Event.LookupDone.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.SetR.class);
            kryo.register(org.piax.gtrans.ov.ddll.LinkNum.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.SetRAck.class);
            kryo.register(java.util.HashSet.class);
            kryo.register(org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTEntEvent.class);
            kryo.register(org.piax.gtrans.async.FTEntry.class);
            kryo.register(java.util.HashMap.class);
            kryo.register(java.lang.Class.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQAdapter.InsertionPointAdapter.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQAdapter.KeyAdapter.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidates.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidatesResponse.class);
            kryo.register(byte[].class);
            kryo.register(org.piax.gtrans.async.FTEntry[].class);
            kryo.register(org.piax.gtrans.async.FTEntry[].class);
            kryo.register(org.piax.common.ObjectId.class);
            kryo.register(org.piax.gtrans.TransOptions.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQRange.class);
            kryo.register(Integer[].class);
            kryo.register(org.piax.util.UniqId.SpecialId.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQReply.class);
            kryo.register(org.piax.gtrans.ov.ring.rq.DKRangeRValue.class);
            kryo.register(org.piax.gtrans.RemoteValue.class);
            kryo.register(org.piax.gtrans.impl.RequestTransportImpl.IsEasySend.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.SetL.class);

            return kryo;
        };
    };

    static public byte[] encode(Object obj, int bufsize, int bufsizeMax) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Output o = new Output(bufsize, bufsizeMax);
        o.setOutputStream(outStream);
        try {
            kryos.get().writeClassAndObject(o, obj);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        o.flush();
        byte[] outArray = outStream.toByteArray();
        return outArray;
    }

    static public Object decode(byte[] bytes) {
        Input input = new Input(bytes);
        Object obj = kryos.get().readClassAndObject(input);
        return obj;
    }
}
