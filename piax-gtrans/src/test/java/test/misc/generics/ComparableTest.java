package test.misc.generics;

public class ComparableTest {
    
    static void f0(Comparable<?> e) {
    }

    static <E extends Comparable<?>> void f1(E e) {
//        f2(e); // NG
//        f3(e); // NG
    }

    static <E extends Comparable<E>> void f2(E e) {
        f3(e);
        f1(e);
    }

    static <E extends Comparable<? super E>> void f3(E e) {
//        f2(e); // NG
        f1(e);
    }
    
    static class A implements Comparable<A> {
        @Override
        public int compareTo(A o) {
            return 0;
        }
    }

    static class B extends A {
    }
    
    static class Hen implements Comparable<Integer> {
        @Override
        public int compareTo(Integer o) {
            return 0;
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Integer e = new Integer(1);
        f0(e);
        f1(e);
        f2(e);
        f3(e);

        Comparable<?> e1 = new String("foo");
        f0(e1);
        f1(e1);
//        f2(e1); // NG
//        f3(e1); // NG

        A a = new A();
        f0(a);
        f1(a);
        f2(a);
        f3(a);

        B b = new B();
        f0(b);
        f1(b);
//        f2(b); // NG
        f3(b);

        Hen e2 = new Hen();
        f0(e2);
        f1(e2);
//        f2(e2); // NG
//        f3(e2); // NG
    }
}
