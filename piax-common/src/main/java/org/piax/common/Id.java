/*
 * Id.java - Identifier using arbitrary length bytes or ASCII string
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Id.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Random;

import org.piax.util.ByteUtil;
import org.piax.util.RandomUtil;

/**
 * 任意長のbyte列、または ASCII文字列からなる Identifierを実現するクラス。
 * <p>
 * Idオブジェクトの生成時には、byte列またはASCII文字列を指定する。
 * ASCII文字列を指定した場合でも、内部的にはbyte列に変換して管理する。
 * toStringメソッドを使うことで、指定したASCII文字列の表現を取得できる。
 * 但し、ASCII文字列を指定しないで直接byte列を指定した場合でも、そのbyte列が7bitの
 * ASCII列である場合は、toStringは、ASCII文字列の表現を返す。
 * <p>
 * newIdメソッドを使うと長さを指定したランダムなbyte列を持つIdオブジェクトを生成できる。
 * immutableなオブジェクトとして振舞う。
 */
public class Id implements Serializable, Comparable<Id> {
    /*
     * The reason of not using BigInteger is below.
     * - shorten memory randSize.
     * - treat unsigned
     * 
     * In jdk 5.0 case, BigInteger has 24 bytes overhead.
     * As the proper data length of Id is 16 or 20 bytes, it is not small.
     */
    
    private static final long serialVersionUID = 1L;
    
    private static Random rand = null;
    
    /**
     * 指定された長さのランダムなbyte列を生成する。
     * サブクラスでの使用を前提とする。
     * 
     * @param len byte列の長さ
     * @return 長さlenのランダムなbyte列
     */
    protected static byte[] newRandomBytes(int len) {
        if (rand == null) {
            /*
             * この命令はidempotentな扱いが可能であるため、同期化しなくても、
             * 本メソッドはスレッドセーフになる
             */
            rand = RandomUtil.newCollisionlessRandom();
        }
        return RandomUtil.newBytes(len, rand);
    }
    
    /**
     * 指定された長さのランダムなbyte列を持つIdオブジェクトを生成する。
     * 
     * @param len byte列の長さ
     * @return 長さlenのランダムなbyte列を持つIdオブジェクト
     */
    public static Id newId(int len) {
        return new Id(newRandomBytes(len));
    }
    
    /**
     * Idオブジェクトを表現するbyte列。immutableな使い方を前提にしている。
     * 文字列を使ってIdオブジェクトを生成した場合も、このbyte列はUTF-8のエンコーディングを使って生成される。
     * <p>
     * 継承したクラス内でこのbyte列を変更するのは構わないが、その場合は、hashCodeをキャッシュしている
     * hash変数を0クリアする必要がある。
     */
    protected final byte[] bytes;
    
    /**
     * Idオブジェクトを文字列を使って生成した場合に保持される値。
     * 一時的に保持される。
     */
    protected transient String strVal;
    
    /** Cache the hash code */
    transient int hash; // default to 0

    /**
     * 指定されたbyte列を値として持つIdオブジェクトを生成する。
     * 
     * @param bytes byte列
     */
    public Id(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("arg should not be null");
        }
        strVal = null;
        this.bytes = bytes;
    }

    /**
     * 指定された文字列を値として持つIdオブジェクトを生成する。
     * 指定された文字列は7bit ASCII文字列でないといけない。
     * 
     * @param str 文字列
     * @throws IllegalArgumentException ASCII文字列でない場合
     */
    public Id(String str) throws IllegalArgumentException {
        if (str == null) {
            throw new IllegalArgumentException("arg should not be null");
        }
        strVal = str;
        byte[] b = null;
        try {
            b = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ignore) {}
        if (!ByteUtil.isASCII(b)) {
            throw new IllegalArgumentException("str should be ASCII");
        }
        bytes = b;
    }
    
    /**
     * Idオブジェクトの内部表現であるbyte列を返す。
     * 返り値であるbyte列が変更されてもよいように、ここでは内部表現のbyte列のコピーを返す。
     * 
     * @return Idオブジェクトの内部表現であるbyte列
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Idオブジェクトの内部表現であるbyte列を返す。
     * 内部で保持するbyte列をコピーせずにそのまま返すので、このメソッドを使う側ではbyte列の内容を
     * 変更しないよう配慮する必要がある。
     * 
     * @return Idオブジェクトの内部表現であるbyte列
     */
    public byte[] _getBytes() {
        return bytes;
    }
    
    public int getByteLen() {
        return bytes.length;
    }
    
    public void setBinaryString() {
        strVal = ByteUtil.bytes2Binary(bytes);
    }

    public void setHexString() {
        strVal = ByteUtil.bytes2Hex(bytes);
    }

    /*
     * TODO
     * testBitとcommonPostfixLenの2つのメソッドは、membership vectorで必要になるもので、
     * ここで定義すべきでないが、membership vectorは先の実装になるため忘れないよう、ここに残しておく
     */
    
    public boolean testBit(int ix) {
        return ByteUtil.testBit(bytes, ix);
    }
    
    public int commonPostfixLen(Id id) {
        return ByteUtil.commonPostfixLen(bytes, id.bytes);
    }

    public int commonPrefixLen(Id id) {
        return ByteUtil.commonPrefixLen(bytes, id.bytes);
    }

    // TODO　後ほどmembership vectorで必要となるかもしれないため、コメントアウトして残しておく
//    public boolean samePrefix(Id id, int nBits) {
//        for (int i = 0; i < nBits; i++) {
//            if (testBit(i) ^ id.testBit(i)) {
//                return false;
//            }
//        }
//        return true;
//    }
//    public boolean isNearThan(Id a, Id b) {
//        BigInteger b_x = new BigInteger(1, this.val);
//        BigInteger b_a = new BigInteger(1, a.val);
//        BigInteger b_b = new BigInteger(1, b.val);
//        BigInteger dista = b_x.subtract(b_a).abs();
//        BigInteger distb = b_x.subtract(b_b).abs();
//        return dista.compareTo(distb) == -1;
//    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = Arrays.hashCode(bytes);
        }
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || !(o instanceof Id)) return false;
        if (hashCode() != o.hashCode()) return false;
        Id id = (Id) o;
        return Arrays.equals(bytes, id.bytes);
    }

    @Override
    public int compareTo(Id id) {
        if (id == null) {
            throw new IllegalArgumentException();
        }
        return ByteUtil.compare(bytes, id.bytes);
    }

    /**
     * Idを2進表記した文字列を返す。
     * 文字列表現において、byteの切れ目には"_"が挿入される。
     * 
     * @return Idを2進表記した文字列
     */
    public String toBinaryString() {
        return ByteUtil.bytes2Binary(bytes);
    }
    
    /**
     * Idを16進表記した文字列を返す。
     * 
     * @return Idを16進表記した文字列
     */
    public String toHexString() {
        return ByteUtil.bytes2Hex(bytes);
    }

    /**
     * Idの文字列表現を返す。
     * <p>
     * 文字列を元にIdオブジェクトが生成されている場合は、元の文字列を返す。
     * Idの持つbyte列が7bitのASCII列である場合も文字列として返す。
     * それ以外の場合は、16進数の表現で返す。
     * 
     * @return Idの文字列表現
     */
    @Override
    public String toString() {
        if (strVal != null) return strVal;
        if (ByteUtil.isASCII(bytes)) {
            strVal = ByteUtil.dumpBytes(bytes);
            return strVal;
        }
        return toHexString();
    }
}
