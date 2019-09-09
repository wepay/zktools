package com.wepay.zktools.test;

import com.wepay.zktools.zookeeper.ZNode;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class DataWatch<D> {

    protected final LinkedBlockingQueue<D> q = new LinkedBlockingQueue<>();
    protected int updateCount = 0;

    /**
     * Waits util the watch receives the data that has the same version and cversion as the target.
     * @param target
     * @param timeout
     * @return T type data (null if timeout)
     */
    public D await(Stat target, long timeout) {
        long due = System.currentTimeMillis() + timeout;

        while (true) {
            long remaining = due - System.currentTimeMillis();

            if (remaining > 0) {
                try {
                    D item = q.poll(remaining, TimeUnit.MILLISECONDS);
                    if (item != null) {
                        if (checkStats(target, item))
                            return item;
                    }
                } catch (InterruptedException ex) {
                    // Ignore
                }
            } else {
                return null;
            }
        }
    }

    /**
     * Waits util the watch receives the node data that has the same version and cversion as the parent target, and
     * the child data that has the same version and cversion as the child target.
     * @param parentTarget
     * @param child
     * @param childTarget
     * @param timeout
     * @return T type data (null if timeout)
     */
    public D await(Stat parentTarget, ZNode child, Stat childTarget, long timeout) {
        long due = System.currentTimeMillis() + timeout;

        while (true) {
            long remaining = due - System.currentTimeMillis();

            if (remaining > 0) {
                try {
                    D item = q.poll(remaining, TimeUnit.MILLISECONDS);
                    if (item != null) {
                        try {
                            if (checkStats(parentTarget, child, childTarget, item)) {
                                return item;
                            }
                        } catch (NullPointerException ex) {
                            ex.printStackTrace();
                        }
                    }
                } catch (InterruptedException ex) {
                    // Ignore
                }
            } else {
                return null;
            }
        }
    }

    public int getUpdateCount() {
        synchronized (this) {
            return updateCount;
        }
    }

    public void resetUpdateCount() {
        synchronized (this) {
            updateCount = 0;
        }
    }

    protected void update(D data) {
        synchronized (this) {
            q.offer(data);
            updateCount++;
        }
    }

    protected abstract boolean checkStats(Stat target, D item);

    protected abstract boolean checkStats(Stat parentTarget, ZNode child, Stat childTarget, D item);

    protected boolean checkStats(Stat target, Stat stat) {
        if (target == null) {
            return (stat == null);
        } else {
            return stat.getVersion() == target.getVersion() && stat.getCversion() == target.getCversion();
        }
    }

}
