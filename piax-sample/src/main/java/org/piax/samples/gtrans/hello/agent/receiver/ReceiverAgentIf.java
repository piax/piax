/*
 * ReceiverAgentIf.java - Interface for ReceiverAgent
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.samples.gtrans.hello.agent.receiver;

import org.piax.agent.AgentIf;
import org.piax.gtrans.RemoteCallable;

public interface ReceiverAgentIf extends AgentIf {
	@RemoteCallable
	public String hello();
	
	@RemoteCallable
	public String whatsYourName();
	
	public void setup();
}
