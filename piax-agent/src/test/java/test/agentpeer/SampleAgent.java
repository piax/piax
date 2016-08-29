package test.agentpeer;

import org.piax.agent.Agent;

public class SampleAgent extends Agent implements SampleAgentIf {
    public String hello() {
        return "I'm "+getName();
    }
}
