package org.piax.gtrans.ov.async.sg;

import java.math.BigInteger;
import java.util.Random;

import org.piax.gtrans.async.EventExecutor;

public class MembershipVector implements Comparable<MembershipVector> {
    public static Random rand = EventExecutor.random();
    public int length;
    protected BigInteger mvValue;
    protected long base;

    protected MembershipVector() {
        this(2);
    }

    public MembershipVector(long base) {
        this(rand.nextLong(), base);
    }

    public MembershipVector(long mv, long base) {
        if (mv < 0) {
            BigInteger d = BigInteger.ONE.shiftLeft(64);
            this.mvValue = d.subtract(BigInteger.valueOf(-mv));
            assert (mvValue.signum() >= 0);
        } else {
            this.mvValue = BigInteger.valueOf(mv);
        }
        this.base = base;
        this.length = 64;
    }

    public int matchLength(MembershipVector another) {
        //return commonPostfixLength(another);
        return commonPrefixLength(another);
    }

    protected int commonPrefixLength(MembershipVector another) {
        String x = this.toString();
        String y = another.toString();

        for (int i = 0; i < length; i++) {
            if (x.charAt(i) != y.charAt(i)) {
                return i;
            }
        }
        return length;
    }

    /**
     * aとbをBASE進数で下の桁から比較し，何桁一致するかを返す．
     * 全桁一致する場合(a==bの場合)，MVの最大桁数を返す．
     */
    protected int commonPostfixLength(MembershipVector another) {
        String x = this.toString();
        String y = another.toString();

        for (int i = (length - 1); i >= 0; i--) {
            if (x.charAt(i) != y.charAt(i)) {
                return (length - 1) - i;
            }
        }
        return length;
    }

    public String getPrefix(int length) {
        String s = toString();
        return s.substring(0, length);
    }

    @Override
    public String toString() {
        //すべての桁を0で埋めておく
        StringBuilder bin = new StringBuilder("");
        for (int i = 0; i < length; i++) {
            bin.append("0");
        }
        bin.append(mvValue.toString((int) base));
        String result = bin.substring(bin.length() - length);
        return result;
    }

    public int compareTo(MembershipVector mv) {
        return this.mvValue.compareTo(mv.mvValue);
    }
}
