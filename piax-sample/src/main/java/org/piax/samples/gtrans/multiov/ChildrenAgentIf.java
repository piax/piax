/* ChildrenAgentIf - Interface of an agent of 'children'.
 *
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Channel.java 1176 2015-05-23 05:56:40Z teranisi $
 */
package org.piax.samples.gtrans.multiov;

import org.piax.agent.AgentIf;
import org.piax.gtrans.RemoteCallable;

public interface ChildrenAgentIf extends AgentIf {
	
	@RemoteCallable
	public Double getValue2();
}
