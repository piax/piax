package org.piax.samples.gtrans.hello.agent.receiver;

import org.piax.agent.Agent;
import org.piax.common.Location;

public class ReceiverAgent extends Agent implements ReceiverAgentIf {

	@Override
	public String hello() {
		return "world @" + getName();
	}
	
	@Override
	public String whatsYourName() {
		return getName();
	}
	
	@Override
	public void setup() {
		try {
			setAttrib("name", getName());
			setAttrib("location", new Location(Math.random(), Math.random()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
