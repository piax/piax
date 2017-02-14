package test.async;

import java.util.function.Consumer;

import org.piax.gtrans.async.Event.TimerEvent;
import org.piax.gtrans.async.EventExecutor;

public class TestTimer {
    public static class Foo implements Consumer<TimerEvent> {
        int count = 0;

        @Override
        public void accept(TimerEvent t) {
            System.out.println("Foo: " + EventExecutor.getVTime() + ", " + count);
            count++;
            if (count == 5) {
                t.cancel();
            }
        }
    }

    public static void main(String[] args) {
        EventExecutor.sched(200, () -> {
            System.out.println("one shot: " + EventExecutor.getVTime());
        });
        EventExecutor.sched(500, 1000, new Foo());
        EventExecutor.sched(1000, 1000, tev -> {
            System.out.println("Bar! " + EventExecutor.getVTime());
        });
        EventExecutor.sched(300, 1000, tev -> {
            System.out.println("Baz! " + EventExecutor.getVTime());
            throw new NullPointerException();
        });
        EventExecutor.startSimulation(10000);
    }
}
