package test.ag;

import java.util.Arrays;

import org.piax.agent.AgentId;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.gtrans.Peer;
import org.piax.samples.cityagent.CityAgent;
import org.piax.samples.cityagent.CityAgentIf;
import org.piax.samples.cityagent.CityAgentPeer;

import test.Util;

/**
 * org.piax.samples.cityagent.CityAgentPeerを使って、Agentの各モード遷移をテストする。
 */
public class TestAgentMode extends Util {

    //--- test data
    // attributes
    static String[] attribs = new String[] {"city", "pop", "loc"};
    
    // for p0
    static Object[][] rowData0 = new Object[][] {
        {"奈良市", 366528},
//        {"京都市", 1474473},
    };
    
    // for p1
    static Object[][] rowData1 = new Object[][] {
        {"大阪市", 2666371},
//        {"神戸市", 1544873},
//        {"姫路市", 536338},
    };
    
    // dcond
    static String[] dconds = new String[] {
        "pop in [1000000..10000000)",
        "pop in [0..300000) and loc in rect(135, 34.5, 1, 1)",
        "pop eq maxLower(1000000)",
        "pop in lower(1000000, 3)",
    };

    
    static AgentId getAg0(CityAgentPeer p) {
        for (AgentId id : p.home.getAgentIds()) {
            return id;
        }
        return null;
    }
    
    /**
     * main code
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {

        printf("-- Agent Mode Test --%n");
        Peer p0 = Peer.getInstance(new PeerId("p0"));
        Peer p1 = Peer.getInstance(new PeerId("p1"));

        printf("-- init peers --%n");
        CityAgentPeer root = new CityAgentPeer(p0, genLocator(Net.EMU, "localhost", 10000));
        CityAgentPeer peer = new CityAgentPeer(p1, genLocator(Net.EMU, "localhost", 10001));
        
        printf("-- join peers --%n");
        root.join(root.sg.getBaseTransport().getEndpoint());
        peer.join(root.bt.getBaseTransport().getEndpoint());
//        peer.dumpTable();
        
        printf("-- new agent and set attributes on each peer --%n");
        root.newAgentsWithAttribs(attribs, rowData0);
        peer.newAgentsWithAttribs(attribs, rowData1);
        printf("-- print internal table on each peer --%n");
        root.printTable();
        peer.printTable();
        
        AgentId target = getAg0(root);
        printf("target ag=%s%n", root.home.getAgentName(target));
        root.printStat();
        peer.printStat();
        
        printf("** sleep ag0 **%n");
        root.home.sleepAgent(target);
        root.printStat();
        peer.printStat();

        printf("** wakeup ag0 **%n");
        root.home.wakeupAgent(target);
        root.printStat();
        peer.printStat();

        printf("** travel ag0 **%n");
        root.home.travelAgent(target, peer.peer.getPeerId());
        root.printStat();
        peer.printStat();

        printf("** dup ag0 **%n");
        peer.home.duplicateAgent(target);
        root.printStat();
        peer.printStat();

        printf("** destroy ag0 **%n");
        peer.home.destroyAgent(target);
        root.printStat();
        peer.printStat();

//        waitForKeyin();
        
        printf("-- invoke discoveryCall --%n");
        for (int i = 0; i < dconds.length; i++) {
            Object[] ret = root.home.discoveryCall(dconds[i], "getCityName");
            printf("\"%s\" => %s%n", dconds[i], Arrays.toString(ret));
        }

        Endpoint dst = peer.tr.getEndpoint();
        printf("-- invoke normal RPC to %s --%n", dst);
        for (AgentId dstId : peer.home.getAgentIds()) {
            CityAgentIf stub = root.home.getStub(CityAgent.class, dstId, dst);
//            printf("result of getCityName() call to %s: %s%n", dstId, stub.getCityName());
        }
        
        printf("%n-- test fin --%n");
        sleep(100);
        peer.leave();
        root.leave();
        sleep(100);
        peer.fin();
        root.fin();
    }
}
