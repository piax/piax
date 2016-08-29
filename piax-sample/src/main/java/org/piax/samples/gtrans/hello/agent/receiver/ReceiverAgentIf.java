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
