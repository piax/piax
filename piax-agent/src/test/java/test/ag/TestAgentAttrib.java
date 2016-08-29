package test.ag;

import java.util.Arrays;

import org.piax.agent.AgentId;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.gtrans.Peer;
import org.piax.samples.cityagent.CityAgentIf;
import org.piax.samples.cityagent.CityAgentPeer;
import org.piax.gtrans.ov.sg.SGMessagingFramework;

import test.Util;

public class TestAgentAttrib extends Util {

    //--- test data
    // attributes
    static String[] attribs = new String[] {"city", "pop", "loc"};
    
    // for p0
    static Object[][] rowData0 = new Object[][] {
        {"奈良市", 366528},
        {"京都市", 1474473},
    };
    
    // for p1
    static Object[][] rowData1 = new Object[][] {
        {"大阪市", 2666371},
    };

    // dcond
    static String[] dconds = new String[] {
        "pop in [1000000..10000000)",
        "loc in rect(135, 34.5, 1, 1)",
        "loc in rect(135, 34.5, 1, 1) and pop in [1000000..10000000)",
        "pop eq maxLower(1000000)",
        "pop in lower(1000000, 3)",
    };

    static CityAgentIf getAg0Stub(CityAgentPeer p) {
        for (AgentId id : p.home.getAgentIds()) {
            return p.home.getStub(CityAgentIf.class,id);
        }
        return null;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
    		SGMessagingFramework.MSGSTORE_EXPIRATION_TASK_PERIOD = 1000;
        printf("-- Agent and Peer Attrib Test --%n");
        Peer p0 = Peer.getInstance(new PeerId("p0"));
        Peer p1 = Peer.getInstance(new PeerId("p1"));

        printf("-- init peers --%n");
        CityAgentPeer root = new CityAgentPeer(p0, genLocator(Net.EMU,
                "localhost", 10000));
        CityAgentPeer peer = new CityAgentPeer(p1, genLocator(Net.EMU,
                "localhost", 10001));

        printf("-- join peers --%n");
        root.join(root.sg.getBaseTransport().getEndpoint());
        peer.join(root.bt.getBaseTransport().getEndpoint());

        printf("-- new agent and set attributes on each peer --%n");
        root.newAgentsWithAttribs(attribs, rowData0);
        peer.newAgentsWithAttribs(attribs, rowData1);
        printf("-- print internal table on each peer --%n");
        root.printTable();
        peer.printTable();

        printf("-- set peer attrib --%n");
        CityAgentIf ag0 = getAg0Stub(root);
        printf("%s->loc:%s%n", ag0.getAttribValue("city"), ag0.getAttribValue("loc"));
        printf("** set loc on peer **%n");
        root.home.setAttrib("loc", new Location(135, 35));
        printf("%s->loc:%s%n", ag0.getAttribValue("city"), ag0.getAttribValue("loc"));
        root.printTable();
//        printf("** remove loc on peer **%n");
//        root.home.removeAttrib("loc");
//        printf("%s->loc:%s%n", ag0.getAttribValue("city"), ag0.getAttribValue("loc"));
//        root.printTable();
        
        // ag0を眠らせて、dcの検索対象からはずしてみる
//        root.home.sleepAgent(ag0.getId());

        printf("-- invoke discoveryCall --%n");
        for (int i = 0; i < dconds.length; i++) {
            Object[] ret = root.home.discoveryCall(dconds[i], "getCityName");
            printf("\"%s\" => %s%n", dconds[i], Arrays.toString(ret));
        }
        printf("%n-- sleep 5 sec --%n");
        sleep(5000);
        printf("%n-- test fin --%n");
        peer.leave();
        root.leave();
        sleep(100);
        peer.fin();
        root.fin();
    }
}
