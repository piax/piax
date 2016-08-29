package test.misc;

import org.piax.common.Id;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RemoteValue;

/**
 * @author     Mikio Yoshida
 * @version    
 */
public class TestMisc {

    public static void main(String[] args) {
        // null Id
        System.out.println("".getBytes().length);
        Id n = new Id("");
        Id n2 = new Id(new byte[]{});
        System.out.println("@@@" + n + "***");
        System.out.println("@@@" + n2 + "***");
        System.out.println(n.equals(n2));
        
        RemoteValue<String> rv = new RemoteValue<String>(null, "hoge");
        FutureQueue<String> fq = new FutureQueue<String>();
        fq.add(rv);
        fq.setEOFuture();
        
        for (RemoteValue v : fq) {
            System.out.println(v.getValue());
        }
        
        fq = FutureQueue.emptyQueue();
        for (RemoteValue v : fq) {
            System.out.println(v.getValue());
        }
    }
}
