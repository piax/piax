package test.misc.narrowing;

class PX implements P {
    public void on(A a) {
        System.out.println("on in P");
    }

    public void foo(B b) {
    }
}

class QX implements Q {
    // このメソッドは書かせたくないけど、書かないとおこられる
    public void on(A a) {
        System.out.println("ignore in Q");
    }
    public void on(B b) {
        System.out.println("on in Q");
    }

    public void foo(B b) {
    }

    public void foo(A a) {
    }
}

/*
 * narrowingのテスト。
 * getは、covariant return typeのサポートでoverrideされているが、
 * setの方は、overloadingのまま
 */
public class Main {
    public static void main(String[] args) {
        A a = new A();
        B b = new B();
        P p = new PX();
        Q q = new QX();
        
        b.set(q);
        b.set(p);
        P p1 = b.get();
        System.out.println(p1.getClass());
        Q q1 = b.get();
        System.out.println(q1.getClass());
        q1.on(b);
        q1.on(a);
    }
}
