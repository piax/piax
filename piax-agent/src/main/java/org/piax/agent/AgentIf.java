/*
 * AgentIf.java - An interface of agents.
 * 
 * Copyright (c) 2009-2015 PIAX development team
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
 */
/*
 * Revision History:
 * ---
 * 
 * $Id: AgentIf.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.agent;

import org.piax.gtrans.RPCIf;

/**
 * <code>Agent</code>クラスに実装される公開メソッドを定義したインタフェース。
 * ここに定義されたメソッドは公開メソッドとして振舞い、他のエージェントや
 * システムから呼び出すことができる。
 * <p>
 * <code>Agent</code>クラスを継承するクラス（エージェントクラス）を作成し、
 * 公開メソッドを定義する場合は、このインタフェースに継承されるインタフェース
 * を作成し、そこにメソッドを定義する必要がある。
 */
public interface AgentIf extends AgentCaller, Attributable, RPCIf {
    
    /**
     * Returns the agent ID of this agent.
     * 
     * @return the agent ID.
     */
    AgentId getId();
    
    /**
     * Returns the name of this agent.
     * 
     * @return the agent name.
     */
    String getName();

    /**
     * Returns the full name of this agent.
     * The agent full name is represented as 
     * "<i>agent name</i>@<i>peer name</i>". 
     * 
     * @return the agent full name.
     */
    String getFullName();

    /**
     * Returns the creation time of this agent, the number of milliseconds
     * since the midnight, January 1, 1970 UTC.
     * 
     * @return the creation time of this agent in milliseconds.
     */
    long getCreationTime();
    

    /**
     * Destroys this agent.
     */
    void destroy();
}
