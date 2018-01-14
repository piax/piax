package org.piax.gtrans.netty.kryo;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import org.objenesis.strategy.StdInstantiatorStrategy;
import org.piax.gtrans.GTransConfigValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

public class KryoUtil {
    private static final Logger logger = LoggerFactory.getLogger(KryoUtil.class.getName());
    static boolean DEBUG = false;
    static ArrayList<RegistrationPair> registrations = new ArrayList<>();
    static long initialLastRegistered = System.nanoTime();
    static long lastRegistered = initialLastRegistered;

    static class RegistrationPair {
        Class clazz;
        Serializer serializer;
        public RegistrationPair(Class clazz, Serializer serializer) {
            this.clazz = clazz;
            this.serializer = serializer;
        }
    }
    
    public synchronized static void register(Class clazz) {
        if (!registrations.contains(clazz)) {
            registrations.add(new RegistrationPair(clazz, null));
            lastRegistered = System.nanoTime();
            logger.debug("last registered={}", lastRegistered);
        }
    }
    
    public synchronized static void register(Class clazz, Serializer serializer) {
        if (!registrations.contains(clazz)) {
            registrations.add(new RegistrationPair(clazz, serializer));
            lastRegistered = System.nanoTime();
            logger.debug("last registered={}", lastRegistered);
        }
    }
    
    public synchronized static void reRegisterIfModified() {
        if (lasts.get() < lastRegistered) {
            logger.debug("re-register because the registration is modified on thread {}", Thread.currentThread());
            Kryo kryo = kryos.get();
            for (RegistrationPair p : registrations) {
                if (p.serializer == null) {
                    kryo.register(p.clazz);
                }
                else {
                    kryo.register(p.clazz, p.serializer);
                }
            }
            lasts.set(lastRegistered);
        }
        else {
            logger.debug("Not registered because the registration is not modified on thread {}", Thread.currentThread());
        }
    }
    
    // for debugging
    // throws an exception if not registered.
    public synchronized static int getRegistrationId(Class clazz) {
        int ret = -1;
        boolean required = kryos.get().isRegistrationRequired();
        kryos.get().setRegistrationRequired(true);
        ret = kryos.get().getRegistration(clazz).getId();
        kryos.get().setRegistrationRequired(required);
        return ret;
    }
    
    private static final ThreadLocal<Long> lasts = new ThreadLocal<Long>() {
        protected Long initialValue() {
            logger.debug("initial last registered={} on thread {}", lastRegistered, Thread.currentThread());
            return initialLastRegistered;
        }
    };

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
            kryo.register(org.piax.common.PeerId.SpecialId.class);
            kryo.register(org.piax.gtrans.RemoteValue.class);
            kryo.register(org.piax.common.DdllKey.class);

//            kryo.register(org.piax.gtrans.impl.RequestTransportImpl.IsEasySend.class);
//            kryo.register(org.piax.gtrans.impl.NestedMessage.class);
            kryo.register(org.piax.gtrans.TransOptions.class);
            kryo.register(org.piax.gtrans.netty.ControlMessage.class);
            kryo.register(org.piax.gtrans.netty.NettyMessage.class);
            kryo.register(org.piax.gtrans.netty.NettyEndpoint.class);
            kryo.register(org.piax.gtrans.netty.NettyLocator.class);
            kryo.register(org.piax.gtrans.netty.NettyLocator.TYPE.class);
            kryo.register(org.piax.gtrans.netty.ControlMessage.ControlType.class);
            kryo.register(org.piax.gtrans.netty.idtrans.PrimaryKey.class);
            logger.debug("registered basic classes on thread {}", Thread.currentThread());

            /*
            kryo.register(org.piax.ayame.Event.class);
            kryo.register(org.piax.ayame.ov.ddll.DdllKeyRange.class);
            kryo.register(org.piax.ayame.ov.rq.DKRangeRValue.class);
            kryo.register(org.piax.ayame.ov.ddll.LinkSeq.class);
            
            kryo.register(org.piax.ayame.Node.class, new NodeSerializer());
            kryo.register(org.piax.ayame.LocalNode.class, new NodeSerializer());
            kryo.register(org.piax.ayame.FTEntry.class);
            kryo.register(org.piax.ayame.FTEntry[].class);
            kryo.register(org.piax.ayame.Event.Lookup.class);
            kryo.register(org.piax.ayame.Event.AckEvent.class);
            kryo.register(org.piax.ayame.Event.LookupDone.class);
            kryo.register(org.piax.ayame.ov.ddll.DdllEvent.SetRAck.class);
            kryo.register(org.piax.ayame.ov.ddll.DdllEvent.SetR.class);
            kryo.register(org.piax.ayame.ov.rq.RQAdapter.InsertionPointAdapter.class);
            kryo.register(org.piax.ayame.ov.rq.RQAdapter.KeyAdapter.class);
            kryo.register(org.piax.ayame.ov.rq.RQRange.class);
            kryo.register(org.piax.ayame.ov.rq.RQReply.class);
            kryo.register(org.piax.ayame.ov.rq.RQRequest.class); 
            kryo.register(org.piax.ayame.ov.suzaku.SuzakuEvent.GetEntReply.class);
            kryo.register(org.piax.ayame.ov.suzaku.SuzakuStrategy.FTEntrySet.class);
            // XXX not registering class leads a performance issue 
            //kryo.register(org.piax.gtrans.ov.suzaku.Suzaku.ExecQueryAdapter.class);
            kryo.register(org.piax.ayame.ov.suzaku.SuzakuEvent.GetEntRequest.class);
            kryo.register(org.piax.ayame.ov.ddll.DdllEvent.GetCandidates.class);
            kryo.register(org.piax.ayame.ov.ddll.DdllEvent.GetCandidatesResponse.class);
            kryo.register(org.piax.ayame.ov.ddll.DdllEvent.SetL.class);
*/
            return kryo;
        };
    };

    static public byte[] encode(Object obj, int bufsize, int bufsizeMax) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Output o = new Output(bufsize, bufsizeMax);
        o.setOutputStream(outStream);
        try {
            KryoUtil.reRegisterIfModified();
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
        KryoUtil.reRegisterIfModified();
        Object obj = kryos.get().readClassAndObject(input);
        return obj;
    }
}
