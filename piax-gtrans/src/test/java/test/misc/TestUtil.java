package test.misc;

import java.util.Random;

import org.piax.util.ByteUtil;

public class TestUtil {

    static Random rand = new Random();
    static void printf(String f, Object...args) {
        System.out.printf(f, args);
    }
    
    static void byteUtil1() {
        for (int i = 0; i < 10; i++) {
            byte b1 = (byte) rand.nextInt();
            byte b2 = (byte) rand.nextInt();
            printf("b1=%02x, %s%n", b1, ByteUtil.byte2Binary(b1));
            printf("b2=%02x, %s%n", b2, ByteUtil.byte2Binary(b2));
//            for (int j = 0; j < 8; j++) {
//                printf("%dth bit of b1 is %s %n", j, ByteUtil.testBit(b1, j));
//            }
            printf("commonPostfixLen of b1, b2 is %d%n", ByteUtil.commonPostfixLen(b1, b2));
        }
    }
    
    static void byteUtil2() {
        for (int i = 0; i < 10; i++) {
            byte[] bb1 = new byte[2];
            byte[] bb2 = new byte[2];
            rand.nextBytes(bb1);
            rand.nextBytes(bb2);
//            bb2[0] = 0;
//            bb2[0] = bb1[0];
            bb2[1] = bb1[1];
            printf("bb1=%36s%n", ByteUtil.bytes2Binary(bb1));
            printf("bb2=%36s%n", ByteUtil.bytes2Binary(bb2));
            printf("commonPostfixLen of bb1, bb2 is %d%n", ByteUtil.commonPostfixLen(bb1, bb2));
            printf("compare of bb1, bb2 is %d%n", ByteUtil.compare(bb1, bb2));
        }
    }

    static void byteUtil3() throws Exception {
        byte[] bb = new byte[5];
        rand.nextBytes(bb);
        String s = new String(bb, "UTF-8");
        printf("%s -> %s%n", ByteUtil.bytes2Hex(bb), s);
        String s1 = "\ufc8e";
        printf("%s -> %s%n", s1, ByteUtil.bytes2Hex(s1.getBytes("ASCII")));
    }

    static void byteUtil4() throws Exception {
        String s = "34.a821\\0-8=1))\'";
        byte[] b = s.getBytes("UTF-8");
        printf("%s ASCII?:%s%n", ByteUtil.bytes2Hex(b), ByteUtil.isASCII(b));
        String s1 = new String(b, "UTF-8");
        printf("%s -> %s%n", s, s1);
        byte[] b2 = new byte[3];
        rand.nextBytes(b2);
        printf("%s ASCII?:%s%n", ByteUtil.bytes2Hex(b2), ByteUtil.isASCII(b2));
    }

    static void foo() {}
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        /*
         * ByteUtilのテスト
         */
        byteUtil4();
    }
}
