package com.wepay.zktools.util;


import java.util.concurrent.ExecutionException;

public class State<T> {

    private volatile T value;
    private StateChangeFuture<T> future;

    public State(T initVal) {
        value = initVal;
        future = null;
    }

    public void set(T newVal) {
        synchronized (this) {
            value = newVal;
            if (future != null) {
                future.complete(newVal);
                future = null;
            }
        }
    }

    public boolean compareAndSet(T oldVal, T newVal) {
        synchronized (this) {
            if (stateEquals(value, oldVal)) {
                set(newVal);
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean is(T ref) {
        return stateEquals(value, ref);
    }

    public T get() {
        return value;
    }

    public StateChangeFuture<T> watch() {
        synchronized (this) {
            if (future == null)
                future = new StateChangeFuture<>(value);

            return future;
        }
    }

    public void await(T awaitValue) throws InterruptedException, ForcedWakeupException {
        try {
            while (true) {
                StateChangeFuture<T> future = watch();

                if (stateEquals(future.currentState, awaitValue))
                    return;

                if (stateEquals(future.get(), awaitValue))
                    return;
            }
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof ForcedWakeupException) {
                throw (ForcedWakeupException) cause;
            } else {
                // This should not happen
                throw new IllegalStateException(ex);
            }
        }
    }

    public void wakeup() {
        synchronized (this) {
            if (future != null)
                future.completeExceptionally(new ForcedWakeupException());
        }
    }

    private static <T> boolean stateEquals(T val1, T val2) {
        if (val1 == null) {
            return val2 == null;
        } else {
            return val1.equals(val2);
        }
    }

    public static class ForcedWakeupException extends Exception {
    }
}
