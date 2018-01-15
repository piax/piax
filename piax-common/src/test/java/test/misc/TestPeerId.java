package test.misc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ObjectStreamException;

import org.junit.jupiter.api.Test;
import org.piax.common.PeerId;
import org.piax.util.SerializingUtil;

public class TestPeerId {

    @Test
    public void test() {
        PeerId id1 = new PeerId("01234567");
        PeerId id2 = new PeerId("81234566");
        assertTrue(id1.toString().equals("01234567"));
        
//        for (int i = 0; i < id1.bitLen(); i++) {
//            System.out.print("" + (id1.testBit(i) ? 1:0) );
//        }
        assertTrue(id1.toBinaryString().equals("00110000_00110001_00110010_00110011_00110100_00110101_00110110_00110111"));
        assertTrue(id2.toBinaryString().equals("00111000_00110001_00110010_00110011_00110100_00110101_00110110_00110110"));
        
        assertTrue(id1.toHexString().equals("3031323334353637"));
        assertTrue(id1.compareTo(id2) < 0);
        assertTrue(id1.commonPrefixLen(id2) == 4);
        
        PeerId p1 = PeerId.PLUS_INFINITY;
        PeerId p2 = PeerId.MINUS_INFINITY;
        byte[] b1 = null;
        PeerId p3 = null;
        try {
            b1 = SerializingUtil.serialize(p1);
            p3 = (PeerId) SerializingUtil.deserialize(b1);
        } catch (ObjectStreamException | ClassNotFoundException e) {
        }
        assertTrue(p1.equals(p3));
        assertTrue(id1.compareTo(p1) < 0);
        assertTrue(p1.compareTo(p2) > 0);
    }

}
