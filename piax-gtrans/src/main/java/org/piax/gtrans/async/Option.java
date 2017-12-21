package org.piax.gtrans.async;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class Option<E> {
    private static List<Option<?>> options = new ArrayList<>();
    protected String argName;
    boolean isGiven = false;
    E value;
    E defaultValue;
    Consumer<E> run;

    public Option(E defaultValue, String argName) {
        this(defaultValue, argName, null);
    }
    public Option(E defaultValue, String argName, Consumer<E> run) {
        options.add(this);
        this.value = this.defaultValue = defaultValue;
        this.argName = argName;
        this.run = run;
    }
    public String getArgName() {
        return argName;
    }
    public abstract void parse(List<String> args);
    public void set(E val) {
        this.value = val;
        invokeCallback();
    }
    public void invokeCallback() {
        if (this.run != null) {
            this.run.accept(value);
        }
    }
    public E value() {
        return this.value;
    }
    @Override
    public String toString() {
        return value().toString();
    }
    public abstract String possibleArgs();
    public static List<Option<?>> allOptions() {
        return new ArrayList<Option<?>>(options);
    }
    public static void parseParams(List<String> args) {
        outer: while (args.size() > 0) {
            String name = args.remove(0);
            for (Option<?> opt: options) {
                if (opt.getArgName().equals(name)) {
                    opt.isGiven = true;
                    opt.parse(args);
                    opt.invokeCallback();
                    continue outer;
                }
            }
            args.add(0, name);
            break;
        }
        // call callbacks that is not specified in the command line.
        for (Option<?> opt: options) {
            if (!opt.isGiven) {
                opt.invokeCallback();
            }
        }
    }
    public static void help() {
        for (Option<?> opt: options) {
            System.out.println("\t" + opt.getArgName()
                + " " + opt.possibleArgs());
        }
    }

    public static class BooleanOption extends Option<Boolean> {
        public BooleanOption(boolean defaultValue, String argName) {
            this(defaultValue, argName, null);
        }
        public BooleanOption(boolean defaultValue, String argName, 
                Consumer<Boolean> run) {
            super(defaultValue, argName, run);
        }
        @Override
        public void parse(List<String> args) {
            if (args.size() == 0 || !(args.get(0).equals("true")) && !(args.get(0).equals("false"))) {
                value = !defaultValue;
            } else {
                String arg = args.remove(0);
                value = Boolean.parseBoolean(arg);
            }
        }
        @Override
        public String possibleArgs() {
            return "true | false";
        }
    }
    public static class IntegerOption extends Option<Integer> {
        public IntegerOption(int defaultValue, String argName) {
            this(defaultValue, argName, null);
        }
        public IntegerOption(int defaultValue, String argName, 
                Consumer<Integer> run) {
            super(defaultValue, argName, run);
        }
        @Override
        public String toString() {
            return Integer.toString(value);
        }
        @Override
        public void parse(List<String> args) {
            if (args.size() == 0) {
                throw new IllegalArgumentException(argName + ": specify integer");
            }
            String arg = args.remove(0);
            try {
                value = Integer.parseInt(arg);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(argName + ": specify integer");
            }
        }
        @Override
        public String possibleArgs() {
            return "num(int)";
        }
    }
    public static class DoubleOption extends Option<Double> {
        public DoubleOption(double defaultValue, String argName) {
            this(defaultValue, argName, null);
        }
        public DoubleOption(double defaultValue, String argName, 
                Consumer<Double> run) {
            super(defaultValue, argName, run);
        }
        @Override
        public String toString() {
            return Double.toString(value);
        }
        @Override
        public void parse(List<String> args) {
            if (args.size() == 0) {
                throw new IllegalArgumentException(argName + ": specify double");
            }
            String arg = args.remove(0);
            try {
                value = Double.parseDouble(arg);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(argName + ": specify double");
            }
        }
        @Override
        public String possibleArgs() {
            return "num(double)";
        }
    }
    public static class EnumOption<E extends Enum<E>> extends Option<E> {
        final Class<E> clazz;
        public EnumOption(Class<E> clazz, E defaultValue, String argName) {
            this(clazz, defaultValue, argName, null);
        }
        public EnumOption(Class<E> clazz, E defaultValue, String argName, 
                Consumer<E> run) {
            super(defaultValue, argName, run);
            this.clazz = clazz;
        }
        @SuppressWarnings("unchecked")
        @Override
        public void parse(List<String> args) {
            E[] enums = clazz.getEnumConstants();
            if (args.size() == 0) {
                throw new IllegalArgumentException(argName
                        + ": specify one of " + Arrays.toString(enums));
            }
            String arg = args.remove(0);
            try {
                Method m = clazz.getMethod("valueOf", String.class);
                value = (E)m.invoke(null, arg);
            } catch (IllegalAccessException | NoSuchMethodException e) {
                throw new Error(e);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IllegalArgumentException) {
                    throw new IllegalArgumentException(argName
                            + ": specify one of " + Arrays.toString(enums));
                }
                throw new Error(e);
            }
        }
        @Override
        public String possibleArgs() {
            E[] enums = clazz.getEnumConstants();
            return Arrays.toString(enums);
        }
    }
    public static class StringOption extends Option<String> {
        public StringOption(String defaultValue, String argName) {
            this(defaultValue, argName, null);
        }
        public StringOption(String defaultValue, String argName, 
                Consumer<String> run) {
            super(defaultValue, argName, run);
        }
        @Override
        public String toString() {
            return value;
        }
        @Override
        public void parse(List<String> args) {
            if (args.size() == 0) {
                throw new IllegalArgumentException(argName + ": specify string");
            }
            String arg = args.remove(0);
            try {
                value = arg;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(argName + ": specify string");
            }
        }
        @Override
        public String possibleArgs() {
            return "string";
        }
    }
}
