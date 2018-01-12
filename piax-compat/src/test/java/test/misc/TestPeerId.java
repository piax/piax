package test.misc;

import org.piax.common.Id;
import org.piax.common.PeerId;
import org.piax.util.SerializingUtil;

public class TestPeerId {

    public static void main(String[] args) throws Exception {
        PeerId id1 = new PeerId("01234567");
        PeerId id2 = new PeerId("81234566");
        PeerId id3 = new PeerId("fd0978ab");
        System.out.println(id1);
        
//        for (int i = 0; i < id1.bitLen(); i++) {
//            System.out.print("" + (id1.testBit(i) ? 1:0) );
//        }
        System.out.println(id1.toBinaryString());
        System.out.println(id2.toBinaryString());
        System.out.println(id1.compareTo(id2));
        System.out.println(id1.commonPrefixLen(id2));
//        System.out.println(id3.isNearThan(id1, id2));
        
        PeerId p1 = PeerId.PLUS_INFINITY;
        PeerId p2 = PeerId.MINUS_INFINITY;
        byte[] b1 = SerializingUtil.serialize(p1);
        Id p3 = (Id) SerializingUtil.deserialize(b1);
        System.out.println(p1 == p3);
        PeerId pp = (PeerId) p2;
        System.out.println(id1.compareTo(p1));
        System.out.println(p1.compareTo(p2));
    }

}
