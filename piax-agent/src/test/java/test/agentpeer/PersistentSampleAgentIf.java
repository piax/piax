package test.agentpeer;

import org.piax.agent.AgentIf;
import org.piax.gtrans.RemoteCallable;

public interface PersistentSampleAgentIf extends AgentIf {
    @RemoteCallable
    String hello();
    
    void setN(int n);
    int getN();
}
