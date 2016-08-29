/* ChildrenAgent - Implementation of an agent of 'children'.
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

import java.util.List;

import org.piax.agent.Agent;

public class ChildrenAgent extends Agent implements ChildrenAgentIf {
	double val;
	@Override
	public Double getValue2() {
		return val;
	}
	@Override
	public void onCreation() {
		try {
			val = Math.random();
			setAttrib("value2", val);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
