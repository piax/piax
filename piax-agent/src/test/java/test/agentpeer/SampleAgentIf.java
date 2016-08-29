package test.agentpeer;

import org.piax.agent.AgentIf;
import org.piax.gtrans.RemoteCallable;

public interface SampleAgentIf extends AgentIf {
    @RemoteCallable
    String hello();
}
