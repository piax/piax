package test.agentpeer;

import org.piax.agent.MobileAgent;

public class MobileSampleAgent extends MobileAgent implements PersistentSampleAgentIf {
    private static final long serialVersionUID = 1L;
    
    private int n = 0;
    
    public void setN(int n) {
        this.n = n;
    }
    
    public int getN() {
        return n;
    }

    public String hello() {
        return "I'm "+getName();
    }

}
