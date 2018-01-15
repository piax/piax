package test.misc.narrowing;

public class GenericInheritance {
    
    static interface I<T> {
    }
    
    static interface J<T> extends I<T> {
    }

    static class A implements I<A> {
    }

    // これは許されない
//    static class B extends A implements I<B> {
//    }

    // これはいける
    static class B extends A implements J<A> {
    }

    // こうしても許されない
//    static class Q extends A implements I {
//    }
    
    public static void main(String[] args) {
    }
}
