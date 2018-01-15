/*
 * UniqNumberGenerator.java - An unique number generator.
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
 * $Id: UniqNumberGenerator.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ユニーク性を保証したint値、ユニークNoを生成するためのクラス。
 * <p>
 * ユニークNoは指定された初期値からスタートし、newNoメソッドが呼ばれる度にインクリメントされる。
 * ユニークNoは指定された整数の範囲 [from, to] の中で生成される。
 * 順にインクリメントされ、toに達した次には、fromに戻って生成される。
 * 尚、from, to は0以上の整数（int値）でないといけない。
 * <p>
 * UniqNoGeneratorオブジェクトは、内部にmapを持ち、discardされていないactiveな番号を保持している。
 * これにより、範囲 [from, to] の中をサイクリックにインクリメントする際にactive番号をスキップさせて、
 * 必ずユニークな番号を生成できるようになっている。
 * <p>
 * UniqNoGeneratorオブジェクトは、ユニークNoを割り当てたいオブジェクトを管理するために用いることもできる。
 * newNo(obj)メソッドを使ってユニークNoを生成すると、指定されたオブジェクトは内部のmapに保持され、
 * getObject(no)メソッドを使って取り出すことが可能となる。
 * <p>
 * UniqNoGenerator実装はスレッドセーフである。
 */
public class UniqNumberGenerator<O> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(UniqNumberGenerator.class);

    private static final Object dummy = new Object();
    
    final ConcurrentMap<Integer, Object> duplicationCheckMap = 
            new ConcurrentHashMap<Integer, Object>();
    final int from;
    final int to;
    private int genNo;

    /**
     * ユニークNoの初期値と生成範囲を指定して、UniqNoGeneratorオブジェクトを生成する。
     * 初期値（init）がfromからtoの範囲に収まっていない場合、fromまたはtoが負の数の場合は、
     * IllegalArgumentExceptionがthrowされる。
     * 
     * @param init ユニークNoの初期値
     * @param from ユニークNoを生成する範囲を指定する始点
     * @param to ユニークNoを生成する範囲を指定する終点
     * @throws IllegalArgumentException
     *          initがfromからtoの範囲に収まっていない場合、fromまたはtoが負の数の場合
     */
    public UniqNumberGenerator(int init, int from, int to) throws IllegalArgumentException {
        if (0 > from || from > init || init > to) {
            throw new IllegalArgumentException("invalid argument value");
        }
        genNo = init - 1;
        this.from = from;
        this.to = to;
    }

    /**
     * ユニークNoを生成し、指定された オブジェクトをユニークNoを使って取得できるよう内部のmapに割り当てる。
     * <p>
     * ユニークNoはinit値からスタートし、fromからtoで指定された範囲のint値が用いられる。
     * to-from+1個のユニークNoが払い出された場合は、エラーとして-1が返される。
     * 
     * @param obj ユニークIDを割り付けるオブジェクト
     * @return ユニークNo, エラーの場合-1
     */
    public synchronized int newNo(O obj) {
        if (duplicationCheckMap.size() >= to - from + 1) {
            logger.error("Uniq number\'s pool overflow");
            return -1;
        }
        Object o = obj;
        if (obj == null) o = dummy;
        // decide genNo
        // 使用中のIDについてはスキップさせる
        do {
            if (genNo < to) genNo++;
            else genNo = from;
        } while (duplicationCheckMap.putIfAbsent(genNo, o) != null);
        logger.debug("new No {}", genNo);
        return genNo;
    }
    
    /**
     * ユニークNoを生成する。
     * <p>
     * ユニークNoはinit値からスタートし、fromからtoで指定された範囲のint値が用いられる。
     * to-from+1個のユニークNoが払い出された場合は、エラーとして-1が返される。
     * 
     * @return ユニークNo, エラーの場合-1
     */
    public int newNo() {
        return newNo(null);
    }
    
    /**
     * 指定されたユニークNoに割り当てられたオブジェクトを取得する。
     * newNo(obj) を使ってユニークNoを生成した場合にのみ、このメソッドは有効に機能する。
     * newNo(obj) を使わなかった場合、もしくは、discardNoによって、消去された場合はnullが返る。
     * 
     * @param no ユニークNo
     * @return 割り付けられたオブジェクト、存在しない場合はnull
     */
    @SuppressWarnings("unchecked")
    public O getObject(int no) {
        Object obj = duplicationCheckMap.get(no);
        if (obj == dummy) return null;
        return (O) obj;
    }
    
    /**
     * 指定されたユニークNoがすでに生成されたものかどうかをチェックする。
     * すでに生成されてactiveな状態である場合はtrueが返る。
     * 
     * @param no ユニークNo
     * @return ユニークNoがactiveな状態の場合、true
     */
    public boolean isActive(int no) {
        return duplicationCheckMap.get(no) != null;
    }
    
    /**
     * 指定されたユニークNoを再利用できるよう破棄する。
     * 
     * @param no 不要になったユニークNo
     */
    public void discardNo(int no) {
        logger.debug("discard No {}", no);
        duplicationCheckMap.remove(no);
    }

    @Override
    public String toString() {
        return String.format("UniqNumberGenertor: %s", 
                duplicationCheckMap);
    }
}
