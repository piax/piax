/* RootAgent - Implementation of an agent of 'root'.
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

public class RootAgent extends Agent implements RootAgentIf, ParentAgentIf {
	@Override
	public Double getValue() {
		return 0.0;
	}

	public void collectParents() {
		List<Double> result = getList(getDCStub("value in [0.0 .. 1.0]", ParentAgentIf.class).getValue());
		result.stream().forEach(d -> {
			System.out.println(getName() + " collected " + d);
		});
	}
	
	@Override
	public void collectChildrenFromParents() {
		getDCStub("value in [0.0 .. 1.0]", ParentAgentIf.class).collectChildren();
	}
	
	@Override
	public void onCreation() {
		try {
			setAttrib("value", 0.0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void collectChildren() {
		// no children.
	}
}
