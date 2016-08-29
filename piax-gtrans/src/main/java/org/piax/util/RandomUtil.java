/*
 * RandomUtil.java - A utility class for ramdoms
 * 
 * Copyright (c) 2009-2015 PIAX develoment team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: RandomUtil.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;


/**
 * 
 */
public class RandomUtil {

    /**
     * seedの初期生成を工夫し、その際に起こりうるコリジョンを抑制したRandomオブジェクトを生成する。
     * <p>
     * 通常のRandomオブジェクトの生成において、seedを省略した場合、ミリ秒精度の時刻が用いられる。
     * ミリ秒の精度（正しくはtick精度）の場合、同時に複数のRandomオブジェクトを生成したときには、
     * 生成箇所が異なるマシンであってもseedが一致してしまうことが起こりうる。
     * これに対処するため、このメソッドでは、System.nanoTime() で取れるlong値に、IPアドレス
     * （取得できない場合は、一時的に生成したObject）のhash値を足した値をseedに使用している。
     * 
     * @return seedの初期生成コリジョンのないRandomオブジェクト
     */
    public static Random newCollisionlessRandom() {
        long seed = System.nanoTime();
        try {
            seed += Arrays.hashCode(InetAddress.getLocalHost().getAddress());
        } catch (UnknownHostException e) {
            seed += new Object().hashCode();
        }
        return new MersenneTwister(seed);
    }
    
    public static byte[] newBytes(int len, Random rand) {
        byte[] val = new byte[len];
        rand.nextBytes(val);
        return val;
    }
}
