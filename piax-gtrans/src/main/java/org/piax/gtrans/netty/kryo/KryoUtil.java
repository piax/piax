package org.piax.gtrans.netty.kryo;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.netty.bootstrap.NettyBootstrap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

public class KryoUtil {
    static boolean DEBUG = false;

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.setClassLoader(GTransConfigValues.classLoaderForDeserialize);
            if (DEBUG) {
                Log.TRACE();
            }
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            // the size becomes small if registered because registered classes are represented as a number, not name.
            kryo.register(java.util.ArrayList.class);
            kryo.register(java.util.HashSet.class);
            kryo.register(java.util.HashMap.class);
            kryo.register(java.lang.Class.class);
            kryo.register(byte[].class);
            kryo.register(Integer[].class);
            kryo.register(org.piax.common.ObjectId.class);
            kryo.register(org.piax.common.PeerId.class);
            kryo.register(org.piax.common.TransportId.class);
            kryo.register(org.piax.common.wrapper.DoubleKey.class);
            kryo.register(org.piax.common.wrapper.StringKey.class);
            kryo.register(org.piax.common.wrapper.ByteKey.class);
            kryo.register(org.piax.common.wrapper.IntegerKey.class);
            kryo.register(org.piax.common.wrapper.BooleanKey.class);
            kryo.register(org.piax.common.wrapper.LongKey.class);
            kryo.register(org.piax.util.UniqId.SpecialId.class);
            kryo.register(org.piax.gtrans.RemoteValue.class);
            kryo.register(org.piax.gtrans.impl.RequestTransportImpl.IsEasySend.class);
            kryo.register(org.piax.gtrans.impl.NestedMessage.class);
            kryo.register(org.piax.gtrans.TransOptions.class);
            kryo.register(org.piax.gtrans.async.Event.class);
            kryo.register(org.piax.gtrans.netty.ControlMessage.class);
            kryo.register(org.piax.gtrans.netty.NettyMessage.class);
            kryo.register(org.piax.gtrans.netty.NettyEndpoint.class);
            kryo.register(org.piax.gtrans.netty.NettyLocator.class);
            kryo.register(org.piax.gtrans.netty.NettyLocator.TYPE.class);
            kryo.register(org.piax.gtrans.netty.ControlMessage.ControlType.class);
            kryo.register(org.piax.gtrans.netty.idtrans.PrimaryKey.class);

            kryo.register(org.piax.gtrans.ov.ddll.DdllKey.class);
            kryo.register(org.piax.gtrans.ov.ring.rq.DdllKeyRange.class);
            kryo.register(org.piax.gtrans.ov.ring.rq.DKRangeRValue.class);
            kryo.register(org.piax.gtrans.ov.ddll.LinkNum.class);

            // ayame related classes
            kryo.register(org.piax.gtrans.async.Node.class, new NodeSerializer());
            kryo.register(org.piax.gtrans.async.LocalNode.class, new NodeSerializer());
            kryo.register(org.piax.gtrans.async.FTEntry.class);
            kryo.register(org.piax.gtrans.async.FTEntry[].class);
            kryo.register(org.piax.gtrans.async.Event.Lookup.class);
            kryo.register(org.piax.gtrans.async.Event.AckEvent.class);
            kryo.register(org.piax.gtrans.async.Event.LookupDone.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.SetRAck.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.SetR.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQAdapter.InsertionPointAdapter.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQAdapter.KeyAdapter.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQRange.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQReply.class);
            kryo.register(org.piax.gtrans.ov.async.rq.RQRequest.class); 
            kryo.register(org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTEntReplyEvent.class);
            kryo.register(org.piax.gtrans.ov.async.suzaku.SuzakuStrategy.FTEntrySet.class);
            kryo.register(org.piax.gtrans.ov.async.suzaku.Suzaku.ExecQueryAdapter.class);
            kryo.register(org.piax.gtrans.ov.async.suzaku.SuzakuEvent.GetFTEntEvent.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidates.class);
            kryo.register(org.piax.gtrans.ov.async.ddll.DdllEvent.GetCandidatesResponse.class);
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
