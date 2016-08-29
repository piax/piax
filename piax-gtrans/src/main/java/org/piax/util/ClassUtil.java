/*
 * ClassUtil.java - A utlity for Class handling.
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
 * $Id: ClassUtil.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.WeakHashMap;

//-- I am waiting for someone to translate the following doc into English. :-)

/**
 * クラスおよびインタフェースの継承関係の処理に関するユーティリティクラス。
 * <p>
 * stubの生成に関するProxyクラスの生成に必要なsuperインタフェースの
 * 算出用メソッドが用意されている。
 * <p>
 * 上記のsuperインタフェースの2回目以降の算出コストを抑えるため、計算結果は
 * キャッシュしている。
 * 
 * @see MethodUtil
 */
public class ClassUtil {

    /** key object for cache map */
    private static class SubIfsKey {
        final Class<?> clazz;
        final Class<?> superIf;

        SubIfsKey(Class<?> clazz, Class<?> superIf) {
            this.clazz = clazz;
            this.superIf = superIf;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o == null || !(o instanceof SubIfsKey)) 
                return false;
            SubIfsKey mkey = (SubIfsKey) o;
            return clazz == mkey.clazz && superIf == mkey.superIf;
        }
        
        @Override
        public int hashCode() {
            return clazz.hashCode() 
                ^ ((superIf == null) ? 0 : superIf.hashCode());
        }
    }

    /** getIfList で用いるキャッシュ用のMap */
    private static WeakHashMap<SubIfsKey, Class<?>[]> cashe =
            new WeakHashMap<SubIfsKey, Class<?>[]>();

    public static boolean isSub(Class<?> clazz1, Class<?> clazz2) {
        return clazz2.isAssignableFrom(clazz1);
    }
    
    /**
     * poolに新しくinterfazeを追加する。
     * <p>
     * 追加の際に、そのinterfazeのサブとなるinterfaceがある場合は追加しない。
     * また、superとなるinterfaceがある場合はすべて削除して、interfazeを追加する。
     * これにより、poolにはinterfaceの集合をlatticeと見た場合に常に下界な要素からなる集合が保持される。
     * （つまい、pool内の各interfaceについて、そのsuperとなるinterfaceがpool内に存在することはない）
     * 
     * @param pool interfaceのリスト
     * @param interfaze 追加を試みるinterface
     */
    private static void addIf(List<Class<?>> pool, Class<?> interfaze) {
        // 要素削除があるため、for eachではなく iteratorを使う
        ListIterator<Class<?>> it = pool.listIterator();
        while (it.hasNext()) {
            Class<?> cur = it.next();
            if (isSub(cur, interfaze)) {
                // if cur < interfaze, do nothing
                return;
            }
            if (isSub(interfaze, cur)) {
                // if interfaze < cur, remove cur
                it.remove();
            }
        }
        it.add(interfaze);
    }
    
    /**
     * clazzの持つinterface（super classから継承されたものも含めて）のうち、
     * superIfと同じかまたはsubの関係にあるinterfaceのみをpoolに追加する。
     * clazz自身がinterfaceの場合は、clazz自身も含める。
     * 
     * @param clazz 基準となるクラス
     * @param superIf 指定されたinterface
     * @param pool interfaceのリスト
     */
    private static <I> void gatherSubIfs0(Class<I> clazz, Class<?> superIf, 
            List<Class<?>> pool) {
        if (clazz.isInterface()) {
            addIf(pool, clazz);
        }
        Class<?>[] ifs = clazz.getInterfaces();
        for (Class<?> interfaze : ifs) {
            if (isSub(interfaze, superIf)) {
                addIf(pool, interfaze);
            }
        }
        Class<? super I> superc = clazz.getSuperclass();
        if (superc != null) {
            gatherSubIfs0(superc, superIf, pool);
        }
    }
    
    /**
     * 指定されたclazzの持つinterface（super classから継承されたものも含めて）の中で、
     * 指定された superIfと同じかsub interface の関係にある interfaceの集合の中で、
     * 下界（lower bound）なものを求める。clazz自身がinterfaceの場合は、clazz自身も含める。
     * <p>
     * 効率化のため、計算した値はキャッシュに登録しておく。
     * 
     * @param clazz 基準となるクラス
     * @param superIf super interface
     * @return clazz から見て super であり、superIf から見て subである
     *          interface の中で下界なもの
     */
    public static <T> Class<?>[] gatherLowerBoundSuperInterfaces(
            Class<T> clazz, Class<?> superIf) {
        assert clazz != null;
        assert superIf != null;
//      if (superIf == null) superIf = Object.class;
        if (!superIf.isInterface()) {
            throw new IllegalArgumentException("superIf is not Interface");
        }
        SubIfsKey key = new SubIfsKey(clazz, superIf);
        Class<?>[] result;
        synchronized (cashe) {
            result = cashe.get(key);
            if (result == null) {
                List<Class<?>> pool = new ArrayList<Class<?>>();
                gatherSubIfs0(clazz, superIf, pool);
                result = new Class[pool.size()];
                pool.toArray(result);
                cashe.put(key, result);
            }
        }
        return result;
    }
}
