package com.wepay.zktools.util;

import java.util.concurrent.TimeoutException;

public class Uninterruptibly {

    @FunctionalInterface
    public interface Callable<T> {
        T call() throws Exception;
    }

    @FunctionalInterface
    public interface CallableWithTimeout<T> {
        T call(long timeRemaining) throws Exception;
    }

    @FunctionalInterface
    public interface Runnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface RunnableWithTimeout {
        void run(long timeRemaining) throws Exception;
    }

    public static <T> T call(Callable<T> callable) {
        while (true) {
            try {
                return callable.call();

            } catch (InterruptedException ex) {
                Thread.interrupted();
            } catch (Exception ex) {
                throw new InvocationException(ex);
            }
        }
    }

    public static <T> T call(CallableWithTimeout<T> callable, long timeout) {
        long due = System.currentTimeMillis() + timeout;
        while (true) {
            try {
                if (timeout > 0) {
                    return callable.call(timeout);
                } else {
                    return null;
                }
            } catch (InterruptedException ex) {
                Thread.interrupted();
                timeout = due - System.currentTimeMillis();
            } catch (Exception ex) {
                throw new InvocationException(ex);
            }

        }
    }

    public static void run(Runnable runnable) {
        while (true) {
            try {
                runnable.run();
                return;

            } catch (InterruptedException ex) {
                Thread.interrupted();
            } catch (Exception ex) {
                throw new InvocationException(ex);
            }
        }

    }

    public static void run(RunnableWithTimeout runnable, long timeout) {
        long due = System.currentTimeMillis() + timeout;
        while (true) {
            try {
                if (timeout > 0)
                    runnable.run(timeout);

                return;

            } catch (InterruptedException ex) {
                Thread.interrupted();
                timeout = due - System.currentTimeMillis();
            } catch (Exception ex) {
                throw new InvocationException(ex);
            }
        }
    }

    private static final Uninterruptibly.RunnableWithTimeout SLEEP = Thread::sleep;

    public static void sleep(long duration) {
        run(SLEEP, duration);
    }

    public static void join(long timeout, Thread... threads) throws TimeoutException {
        if (timeout <= 0)
            throw new IllegalArgumentException();

        long due = System.currentTimeMillis() + timeout;
        for (Thread thread : threads) {
            timeout = due - System.currentTimeMillis();

            if (timeout <= 0)
                throw new TimeoutException();

            try {
                Uninterruptibly.run(thread::join, timeout);
            } catch (InvocationException ex) {
                if (ex.getCause() instanceof TimeoutException)
                    throw (TimeoutException) ex.getCause();
            }
        }
    }

    public static class InvocationException extends RuntimeException {
        InvocationException(Throwable cause) {
            super(cause);
        }
    }

}
