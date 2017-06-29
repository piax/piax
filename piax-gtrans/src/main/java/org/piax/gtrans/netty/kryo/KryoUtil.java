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
import org.piax.gtrans.netty.ControlMessage;
import org.piax.gtrans.netty.NettyEndpoint;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.NettyMessage;
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

    static public Kryo getKryoInstance() {
        Kryo kryo = new Kryo();
        if (DEBUG) {
            Log.DEBUG();
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
        return kryo;
    }
    static public byte[] encode(Kryo kryo, Object obj, int bufsize) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Output o = new Output(outStream, bufsize);
        try {
            kryo.writeClassAndObject(o, obj);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        o.flush();
        byte[] outArray = outStream.toByteArray();
        return outArray;
    }

    static public Object decode(Kryo kryo, byte[] bytes) { 
        Input input = new Input(bytes);
        return kryo.readClassAndObject(input);
    }
}
