package org.piax.gtrans.ov.suzaku;

import java.util.concurrent.atomic.AtomicLong;

import org.piax.common.subspace.Lower;
import org.piax.common.subspace.LowerUpper;
import org.piax.common.wrapper.DoubleKey;
import org.piax.gtrans.ov.Overlay;

public class TestRunner {
    static public void main(String args[]) {
        
        int loops = 130;
        int ignore = 30;
            AtomicLong received1 = new AtomicLong(0);
            AtomicLong received2 = new AtomicLong(0);
            AtomicLong received3 = new AtomicLong(0);
            AtomicLong received4 = new AtomicLong(0);
            AtomicLong received5 = new AtomicLong(0);
            AtomicLong received6 = new AtomicLong(0);
            
            AtomicLong start = new AtomicLong(0L);

            try (
                    Overlay<LowerUpper, DoubleKey> ov1 = new Suzaku<>("id:0.0:tcp:localhost:12367");
                    Overlay<LowerUpper, DoubleKey> ov2 = new Suzaku<>("id:0.2:tcp:localhost:12368");
                    Overlay<LowerUpper, DoubleKey> ov3 = new Suzaku<>("id:0.3:tcp:localhost:12369");
                    Overlay<LowerUpper, DoubleKey> ov4 = new Suzaku<>("id:0.4:tcp:localhost:12370");
                    Overlay<LowerUpper, DoubleKey> ov5 = new Suzaku<>("id:0.5:tcp:localhost:12371");
                    Overlay<LowerUpper, DoubleKey> ov6 = new Suzaku<>("id:0.6:tcp:localhost:12372");
                 ) {
                ov1.setListener((trans, rmsg) -> {
                    received1.set(received1.get() + (System.currentTimeMillis() - start.get()));
                });
                ov2.setListener((trans, rmsg) -> {
                    received2.set(received2.get() + (System.currentTimeMillis() - start.get()));
                });
                ov3.setListener((trans, rmsg) -> {
                    received3.set(received3.get() + (System.currentTimeMillis() - start.get()));
                    System.out.println("elapsed3:" + (System.currentTimeMillis() - start.get()));
                });
                
                ov4.setListener((trans, rmsg) -> {
                    received4.set(received4.get() + (System.currentTimeMillis() - start.get()));
                    System.out.println("elapsed4:" + (System.currentTimeMillis() - start.get()));
                });
                
                ov5.setListener((trans, rmsg) -> {
                    received5.set(received5.get() + (System.currentTimeMillis() - start.get()));
                    System.out.println("elapsed5:" + (System.currentTimeMillis() - start.get()));
                });
                
                ov6.setListener((trans, rmsg) -> {
                    received5.set(received5.get() + (System.currentTimeMillis() - start.get()));
                    System.out.println("elapsed6:" + (System.currentTimeMillis() - start.get()));
                });

                ov1.join("tcp:localhost:12367");
                ov2.join("tcp:localhost:12367");
                ov3.join("tcp:localhost:12367");
                ov4.join("tcp:localhost:12367");
                ov5.join("tcp:localhost:12367");
                ov6.join("tcp:localhost:12367");
                        
                Thread.sleep(500);
                System.out.println("type any key");
                System.in.read();
                
                for (int i = 0; i < loops; i++) {
                    System.out.println((i + 1) + "th try");
                    start.set(System.currentTimeMillis());
                    ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.1), 1), "XXXXXXXXXXXXXXXXXXXXXX");
                    start.set(System.currentTimeMillis());
                    ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.3), 1), "XXXXXXXXXXXXXXXXXXXXXX");
                    start.set(System.currentTimeMillis());
                    ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.4), 1), "XXXXXXXXXXXXXXXXXXXXXX");
                    start.set(System.currentTimeMillis());
                    ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.41), 1), "XXXXXXXXXXXXXXXXXXXXXX");
                    start.set(System.currentTimeMillis());
                    ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.59), 1), "XXXXXXXXXXXXXXXXXXXXXX");
                    start.set(System.currentTimeMillis());
                    ov1.send(new Lower<DoubleKey>(false, new DoubleKey(0.69), 1), "XXXXXXXXXXXXXXXXXXXXXX");
                    if (i < ignore) { // reset;
                        received1.set(0);
                        received2.set(0);
                        received3.set(0);
                        received4.set(0);
                        received5.set(0);
                        received6.set(0);
                    }
                    
                    Thread.sleep(1000);
                }
                System.out.println("ave1:" + ((double)received1.get() / (loops - ignore)));
                System.out.println("ave2:" + ((double)received2.get() / (loops - ignore)));
                System.out.println("ave3:" + ((double)received3.get() / (loops - ignore)));
                System.out.println("ave4:" + ((double)received4.get() / (loops - ignore)));
                System.out.println("ave5:" + ((double)received5.get() / (loops - ignore)));
                System.out.println("ave6:" + ((double)received5.get() / (loops - ignore)));
            }   
            catch (Exception e) {
                e.printStackTrace();
            };
        }

}
