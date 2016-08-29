package test.misc;

import java.nio.ByteBuffer;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.raw.emu.EmuLocator;
import org.piax.util.ByteUtil;
import org.piax.util.SerializingUtil;

public class TestNestedMessage {

    public static void main(String[] args) throws Exception {
        Object msgBody = "0123456789";
        NestedMessage nmsg0 = new NestedMessage(new ObjectId("s0"), 
                new ObjectId("r0"), new PeerId("peer0"), new EmuLocator(123),
                0, (byte) 1, msgBody);
        nmsg0.setPassthrough("path");
        NestedMessage nmsg1 = new NestedMessage(new ObjectId("s1"), 
                new ObjectId("r1"), null, new EmuLocator(123),
                -123, null, nmsg0);
        System.out.println(nmsg1);
        ByteBuffer bb = nmsg1.serialize();
        System.out.println(bb.remaining());
        System.out.println(ByteUtil.dumpBytes(bb));
        NestedMessage nmsg2 = NestedMessage.deserialize(bb);
        System.out.println(nmsg2);
        int len = SerializingUtil.serialize(nmsg2).length;
        System.out.println(len);
    }
}
