package org.piax.samples.gtrans.hello.agent.sender;

import org.piax.agent.Agent;

public class SenderAgent extends Agent implements SenderAgentIf {

	@Override
	public void send() {
		// Same behavior with type-checking by following;
		// getList(getDCStub("location in rect(0.0, 0.0, 1.0, 1.0)",
		//		ReceiverAgentIf.class, RPCMode.SYNC).hello()).toArray();
		String first = null;
		for (Object obj : discoveryCall("location in rect(0.0, 0.0, 1.0, 1.0)", "whatsYourName")) {
			if (first == null) {
				first = (String) obj;
			}
			System.out.println("agent @ " + obj);
		}
		if (first != null) {
			for (Object obj : discoveryCall(String.format("name eq \"%s\"", first), "hello")) {
				System.out.println(obj);
			}
		}

	}

}
