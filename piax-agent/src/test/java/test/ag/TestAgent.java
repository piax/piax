package test.ag;

import org.piax.agent.Agent;

public class TestAgent extends Agent {
    @Override
    public void onCreation() {
        System.out.printf("Hello! peer:%s agId:%s name:%s%n", getHome()
                .getPeerId(), getId(), getName());
    }
}
