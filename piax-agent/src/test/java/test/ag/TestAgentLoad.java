package test.ag;

import java.io.File;
import java.io.IOException;

import org.piax.agent.AgentHome;
import org.piax.agent.AgentId;
import org.piax.agent.AgentIf;
import org.piax.agent.AgentInstantiationException;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.PeerId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;

import test.Util;

public class TestAgentLoad extends Util {

    public static void main(String[] args) throws IOException, IdConflictException {
        Net ntype = Net.UDP;
        printf("- start -%n");

        // Peer, AgentHomeを用意する
        Peer peer = Peer.getInstance(new PeerId("p1"));
        ChannelTransport<?> tr = peer.newBaseChannelTransport(
                genLocator(ntype, "localhost", 10001));
        File path = new File("../gtrans_BK/classes");
//        printf("%s%n", path.isDirectory());
//        AgentHome home = new AgentHomeImpl(tr, ClassLoader.getSystemClassLoader(), path);
        AgentHome home = new AgentHomeImpl(tr, path);

        // createAgent
        try {
            AgentId agId = home.createAgent("test.ag.TestAgent2", "hoge");
            AgentIf stub = home.getStub(AgentIf.class,agId);
            printf("agent name: %s%n", stub.getFullName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (AgentInstantiationException e) {
            e.printStackTrace();
        }
        
        peer.fin();
    }
}
