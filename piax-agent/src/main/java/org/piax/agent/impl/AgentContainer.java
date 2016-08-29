/*
 * AgentContainer.java - A container of an agent.
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
 * 
 * $Id: AgentContainer.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.lang.reflect.UndeclaredThrowableException;

import org.piax.agent.Agent;
import org.piax.agent.AgentException;
import org.piax.agent.AgentHome;
import org.piax.agent.AgentId;
import org.piax.agent.AgentIf;
import org.piax.agent.AgentAccessDeniedException;
import org.piax.agent.IllegalAgentModeException;
import org.piax.agent.InvalidAgentJarException;
import org.piax.agent.MobileAgent;
import org.piax.agent.NoSuchAgentException;
import org.piax.agent.PersistentAgent;
import org.piax.common.PeerId;
import org.piax.util.ByteUtil;
import org.piax.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AgentContainer は1つのAgentをその状態に左右されない形で管理するための
 * クラスである。
 * Agentは、sleep や、他のピアへ移動した状態を持つため、実体がインスタンス
 * として存在しない場合がある。AgentContainer はいわば Agentのまゆのような
 * 働きをし、AgentHome や AgentShelf から見て変化しないオブジェクトとして
 * 見える。
 */
public class AgentContainer {
    /*
     * TODO Think!
     * パッケージ内に隠蔽させるクラスとして設計したが、結果的に、
     * パッケージ外に公開（public）せざるえないメソッドがたくさん
     * 出現している。
     */
    
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(AgentContainer.class);
    
    private AgentHomeImpl home;

    /* about agent */
    private LoadImage image = null;
    private final Class<? extends AgentIf> agentClass;
    private final AgentId agentId;
    private String agentName;
    private Agent agent;

    // public as called by musasabi
    public AgentContainer(AgentHomeImpl home, LoadImage image) {
        this.home = home;
        this.image = image;
        agent = image.agent;
        agentClass = agent.getClass();
        agentId = agent.getId();
        agentName = agent.getName();
    }
    
    // public as called by musasabi
    public void reload(LoadImage image) {
        this.image = image;
        agent = image.agent;
    }

    // public as called by musasabi
    public void attachAgent(AgentHomeImpl home, InitialAgentMode mode) {
        assert agent != null;
        if (agent == null) return;
        agent._$bindAndActivate(this, mode);
    }
    
    // public as called by musasabi
    public void detachAgent() {
        assert agent != null;
        if (agent == null) return;
        agent._$getAttribs().dettach();
//        agent._$freeCommPort();
        agent = null;
    }
    
    void fin() {
        home.removeAgContainer(agentId);
        detachAgent();
        image = null;
    }

    // public as called by Agent
    public boolean isSleeping() {
        assert !isDestroyed();
        return (agent == null);
    }
    
    boolean isDestroyed() {
        return (image == null);
    }

    boolean hasPersistency() {
        return ClassUtil.isSub(agentClass, PersistentAgent.class);
    }
    
    boolean hasMobility() {
        return ClassUtil.isSub(agentClass, MobileAgent.class);
    }

    // public as called by Agent
    public AgentHomeImpl getHome() {
        return home;
    }
    
    // public as called by musasabi
    public Agent getAgent() throws IllegalAgentModeException {
        if (isSleeping()) {
            throw new IllegalAgentModeException();
        }
        return agent;
    }
    
    Class<? extends AgentIf> getAgentClass() {
        return agentClass;
    }
    
    // public as called by musasabi
    public AgentId getAgentId() {
        return agentId;
    }
    
    String getAgentName() {
        return agentName;
    }
    
    public void giveAgentName(String name) {
        // TODO renew AgentShelf data
        agentName = name;
    }

    ClassLoader getAgentClassLoader() {
        return image.loader;
    }
    
    public void destroy() throws IllegalAgentModeException {
        if (isSleeping()) {
            throw new IllegalAgentModeException();
        }
        home.onCall(agent, AgentHome.DESTRUCTION);
        fin();
    }

    public void sleep() 
            throws IllegalAgentModeException, ObjectStreamException, IOException {
        if (isSleeping()) {
            throw new IllegalAgentModeException();
        }
        home.onCall(agent, AgentHome.SLEEPING);
        save0(null, true);
        detachAgent();
    }
    
    // Agent will not call this
    public void wakeup()
            throws AgentException, ObjectStreamException {
        if (!isSleeping()) {
            throw new IllegalAgentModeException();
        }

        byte[] jar;
        try {
            String jarPath = home.makeAgentJarPath(agentId, true);
            jar = home.getStatusRepo().restoreBytes(jarPath);
            home.getStatusRepo().delete(jarPath);
        } catch (FileNotFoundException e) {
            throw new NoSuchAgentException();
        } catch (IOException e) {
            throw new NoSuchAgentException();
        }
        if (jar == null) {
            throw new NoSuchAgentException("StatusRepo is null");
        }
        try {
            home.load(jar, InitialAgentMode.WAKEUP);
        } catch (ClassNotFoundException e) {
            // this exception will not occur
            logger.error("", e);
        } catch (InvalidAgentJarException e) {
            // this exception will not occur
            logger.error("", e);
        }
    }

    /**
     * AgentのJarイメージを取得する。
     * 
     * @param mode onメソッドを起動する場合のmode設定
     * @return Jarイメージ
     * @throws IllegalAgentModeException sleep中の場合
     * @throws ObjectStreamException Jar生成時のIO例外
     */
    byte[] getJar(int mode) 
            throws IllegalAgentModeException, ObjectStreamException {
        if (isSleeping()) {
            throw new IllegalAgentModeException();
        }
        home.onCall(agent, mode);
        byte[] jar = image.makeJar();
        return jar;
    }

    public AgentId duplicate()
            throws IllegalAgentModeException, ObjectStreamException {
        if (isSleeping()) {
            throw new IllegalAgentModeException();
        }
        byte[] jar = getJar(AgentHome.DUPLICATING);
        AgentId agId = null;
        try {
            agId = home.load(jar, InitialAgentMode.DUPLICATION);
        } catch (ClassNotFoundException e) {
            // this exception will not occur
            logger.error("", e);
        } catch (InvalidAgentJarException e) {
            // this exception will not occur
            logger.error("", e);
        }
        return agId;
    }
    
    private void save0(File file, boolean isSleep) 
            throws FileNotFoundException, 
            ObjectStreamException, IOException {
        byte[] jar = image.makeJar();
        
        if (file == null) {
            String jarPath = home.makeAgentJarPath(agentId, isSleep);
            home.getStatusRepo().saveBytes(jarPath, jar);
        } else {
            ByteUtil.bytes2File(jar, file);
        }
    }
    
    public void save(File file) throws FileNotFoundException,
            IllegalAgentModeException, ObjectStreamException, IOException {
        if (isSleeping()) {
            throw new IllegalAgentModeException();
        }
        home.onCall(agent, AgentHome.SAVING);
        save0(file, false);
    }

    public void travel(PeerId peerId) 
            throws IllegalAgentModeException, ClassNotFoundException, 
            ObjectStreamException, IOException, AgentAccessDeniedException {
        byte[] jar = getJar(AgentHome.DEPARTURE);
        try {
            // remote peer call
            home.load(peerId, jar, InitialAgentMode.ARRIVAL);
            fin(); //移動に成功したらunloadする。
        } catch (InvalidAgentJarException e) {
            logger.error("", e);
            throw new UndeclaredThrowableException(e);
        }
        return;
    }
}
