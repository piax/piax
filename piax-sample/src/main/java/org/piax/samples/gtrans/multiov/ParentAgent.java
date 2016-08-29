/* ParentAgent - Implementation of an agent of 'parent'.
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

public class ParentAgent extends Agent implements ParentAgentIf, ChildrenAgentIf {
	double val;
	@Override
	public Double getValue() {
		return val;
	}
	@Override
	public Double getValue2() {
		return 0.0;
	}
	public void collectChildren() {
		List<Double> result = getList(getDCStub("value2 in [0.0 .. 1.0]", ChildrenAgentIf.class).getValue2());
		result.stream().forEach(d -> {
			System.out.println(getName() + " collected " + d);
		});
	}
	@Override
	public void onCreation() {
		try {
			val = Math.random();
			setAttrib("value", val);
			setAttrib("value2", 0.0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
