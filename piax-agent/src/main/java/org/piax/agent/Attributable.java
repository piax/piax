/*
 * Attributable.java - An interface of objects that have attributes
 * 
 * Copyright (c) 2012-2015 PIAX development team
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
 * $Id: Attributable.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent;

import java.util.List;

import org.piax.common.attribs.IncompatibleTypeException;

/**
 * 属性のハンドリング機能を持つことを示すinterface
 */
public interface Attributable {
    
    /**
     * 名前を指定して、属性値をセットする。
     * 可能な場合はindex化（オーバーレイへのaddKey含む）を行う。
     * 
     * @param name the attribute name
     * @param value the value to be associated with the specified name
     * @return index化された場合 true
     * @throws IllegalArgumentException 属性名または属性値にnullがセットされた場合
     * @throws IncompatibleTypeException 属性値が代入不可能な場合
     */
    boolean setAttrib(String name, Object value)
            throws IllegalArgumentException, IncompatibleTypeException;

    /**
     * 名前を指定して、属性値をセットする。
     * useIndex が false の場合は、index化（オーバーレイへのaddKey含む）は行わない。
     * 
     * @param name the attribute name
     * @param value the value to be associated with the specified name
     * @param useIndex index化（オーバーレイへのaddKey含む）の指示
     * @return index化された場合 true
     * @throws IllegalArgumentException 属性名または属性値にnullがセットされた場合
     * @throws IncompatibleTypeException 属性値が代入不可能な場合
     */
    boolean setAttrib(String name, Object value, boolean useIndex)
            throws IllegalArgumentException, IncompatibleTypeException;

    /**
     * 指定されたnameの属性値の登録を消去する。
     * 属性値がオーバレイにセットされている場合は、オーバレイの登録も
     * 消去する。
     * 
     * @param name the attribute name
     * @return 登録の消去に成功した場合は true、そうでない場合は false
     * @exception IllegalArgumentException if the name is null.
     */
    boolean removeAttrib(String name);

    /**
     * 指定されたnameの属性値を返す。
     * 
     * @param attribName the attribute name
     * @return the value to be associated with the specified name
     * @exception IllegalArgumentException 
     *          if the name is null or is not used.
     */
    Object getAttribValue(String attribName);

    /**
     * セットされている属性名の集合を返す。
     * 
     * @return セットされている属性名の集合
     */
    List<String> getAttribNames();

    /**
     * 属性値がindex化されている場合 trueを返す。
     * 
     * @param attribName 属性名
     * @return 属性値がindex化されている場合 true
     * @throws IllegalArgumentException 属性名が存在しない場合
     */
    boolean isIndexed(String attribName) throws IllegalArgumentException;
}
