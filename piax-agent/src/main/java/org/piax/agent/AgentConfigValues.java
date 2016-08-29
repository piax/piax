/*
 * AgentConfigValues.java - configuration values for Agents.
 * 
 * Copyright (c) 2012-2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: AgentConfigValues.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.agent;


public class AgentConfigValues {
    public static ClassLoader defaultParentAgentLoader = ClassLoader.getSystemClassLoader();
/*	
    public static final String USER_HOME = System.getProperty("user.home");
    
    // default parent ClassLoader of AgentLoader


    // config for mini probe
    public static boolean useProbe = false;

    // config for overlay loading
    public static final String LOCATION_ATTRIB_NAME = "$location";

    // ID length
    public static int agentIdByteLength = 16;
    
    static {
        set();
    }
    
    public static void set() {
        AgentId.DEFAULT_BYTE_LENGTH = agentIdByteLength;
    }
*/
}
