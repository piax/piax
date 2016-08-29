/*
 * AgentHomeListener.java - A listener to receive status change of agent home.
 * 
 * Copyright (c) 2015 PIAX development team
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
 * $Id: AgentHome.java 912 2013-10-22 23:28:17Z yos $
 */

package org.piax.agent;

import org.piax.common.PeerId;

/**
 * AgentHomeに設定することにより
 * エージェントの状態が変更した場合の通知を受けるリスナー・インターフェース
 */
public interface AgentHomeListener {
    /**
     * エージェントが作成された時に呼ばれる。
     * @param agId 作成されたエージェントのId
     */
    void onCreation(AgentId agId);
    /**
     * エージェントが解体されようとした時に呼ばれる。
     * @param agId 解体されたエージェントのId
     */
    void onDestruction(AgentId agId);
    /**
     * エージェントがスリープしようとしている時に呼ばれる。
     * @param agId スリープしようとしているエージェントのId
     */
    void onSleeping(AgentId agId);
    /**
     * エージェントが複製されようとしている時に呼ばれる。
     * @param agId 複製されようとしているエージェントのId
     */
    void onDuplicating(AgentId agId);
    /**
     * エージェントが複製された時に呼ぼれる。
     * @param agId 複製されて新たに作成されたエージェントのId
     */
    void onDuplication(AgentId agId);
    /**
     * エージェントが保存されようとしている時に呼ばれる。
     * @param agId 保存されようとしているエージェントのId
     */
    void onSaving(AgentId agId);
    /**
     * エージェントの移動の際に、移動元から出ようとしている時に
     * 呼ばれる。
     * 移動は、結果的に失敗するかもしれないことに注意が必要である。
     * @param agId 移動しようとしているエージェントのId
     */
    void onDeparture(AgentId agId);
    /**
     * エージェントの移動の際に、到着した時に呼ばれる。
     * @param agId 到着したエージェントのId
     */
    void onArrival(AgentId agId);
    /**
     * エージェントがウェークアップしたときに呼ばれる。
     * @param agId ウェークアップしたエージェントのId
     */
    void onWakeup(AgentId agId);
    /**
     * エージェントが保存状態から回復した時に呼ばれる。
     * @param agId 回復したエージェントのId
     */
    void onRestore(AgentId agId);
    /**
     * MobileAgentが到着しようとしている時に呼ばれる。
     * falseを返すと、拒否する。
     * エージェントは、このピアにはまだ存在しないので
     * それに対する操作、情報取得は出来ないので注意が必要である。
     * 現状では、引数に渡されるエージェントの情報の意図的な偽装を
     * 防ぐ手段は何もないのでセキュリティ的には信用できず、
     * 注意が必要である。
     * @param agentClassName 到着しつつあるエージェントのクラス名
     * @param agId 到着しつつあるエージェントのId
     * @param agentName 到着しつつあるエージェントの名前
     * @param from 移動元
     * @return 到着を許可するか否か
     */
    boolean onArriving(String agentClassName, String agId,
            String agentName, PeerId from);
}
