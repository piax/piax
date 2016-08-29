package test.misc;

import org.piax.common.Id;
import org.piax.util.SerializingUtil;
import org.piax.util.UniqId;

public class TestUniqId {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        UniqId id1 = new UniqId("01234567");
        UniqId id2 = new UniqId("81234566");
        UniqId id3 = new UniqId("fd0978ab");
        System.out.println(id1);
        
//        for (int i = 0; i < id1.bitLen(); i++) {
//            System.out.print("" + (id1.testBit(i) ? 1:0) );
//        }
        System.out.println(id1.toBinaryString());
        System.out.println(id2.toBinaryString());
        System.out.println(id1.compareTo(id2));
        System.out.println(id1.commonPrefixLen(id2));
//        System.out.println(id3.isNearThan(id1, id2));
        
        UniqId p1 = UniqId.PLUS_INFINITY;
        UniqId p2 = UniqId.MINUS_INFINITY;
        byte[] b1 = SerializingUtil.serialize(p1);
        Id p3 = (Id) SerializingUtil.deserialize(b1);
        System.out.println(p1 == p3);
        UniqId pp = (UniqId) p2;
        System.out.println(id1.compareTo(p1));
        System.out.println(p1.compareTo(p2));
    }

}
