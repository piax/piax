package test.async;

import java.util.function.Consumer;

import org.piax.gtrans.async.Event.TimerEvent;
import org.piax.gtrans.async.EventDispatcher;

public class TestTimer {
    public static class Foo implements Consumer<TimerEvent> {
        int count = 0;

        @Override
        public void accept(TimerEvent t) {
            System.out.println("Foo: " + EventDispatcher.getVTime() + ", " + count);
            count++;
            if (count == 5) {
                t.cancel();
            }
        }
    }

    public static void main(String[] args) {
        EventDispatcher.sched(200, () -> {
            System.out.println("one shot: " + EventDispatcher.getVTime());
        });
        EventDispatcher.sched(500, 1000, new Foo());
        EventDispatcher.sched(1000, 1000, tev -> {
            System.out.println("Bar! " + EventDispatcher.getVTime());
        });
        EventDispatcher.sched(300, 1000, tev -> {
            System.out.println("Baz! " + EventDispatcher.getVTime());
            throw new NullPointerException();
        });
        EventDispatcher.startSimulation(10000);
    }
}
