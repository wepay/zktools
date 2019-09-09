package com.wepay.zktools.test.util;

import com.wepay.zktools.util.Uninterruptibly;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class ZooKeeperServerRunner {

    private static final int tickTime = 30000;
    private static final int numConnections = 5000;

    private final AtomicReference<Thread> threadRef = new AtomicReference<>();
    private final File zkDir;

    private ZooKeeperServer server;
    private ServerCnxnFactory standaloneServerFactory;
    private volatile int zkPort;

    public ZooKeeperServerRunner(int port) throws IOException {
        this.zkPort = port;
        this.zkDir = Files.createTempDirectory("zookeeper-").toFile();
    }

    public String start() throws Exception {
        return startAsync(0L).get();
    }

    public Future<String> startAsync(long delay) throws Exception {
        final CompletableFuture<String> future = new CompletableFuture<>();

        if (threadRef.get() != null) {
            future.completeExceptionally(new IllegalStateException("server running"));
            return future;
        }

        Thread thread = new Thread() {
            public void run() {
                try {
                    if (delay > 0)
                        Uninterruptibly.sleep(delay);

                    standaloneServerFactory = ServerCnxnFactory.createFactory(zkPort, numConnections);
                    zkPort = standaloneServerFactory.getLocalPort();
                    server = new ZooKeeperServer(zkDir, zkDir, tickTime);
                    standaloneServerFactory.startup(server);
                    future.complete(connectString());

                } catch (Throwable ex) {
                    future.completeExceptionally(ex);
                }
            }
        };
        thread.setDaemon(true);

        if (threadRef.compareAndSet(null, thread)) {
            thread.start();
        } else {
            future.completeExceptionally(new IllegalStateException("server running"));
        }

        return future;
    }

    public void stop() throws Exception {
        Thread thread = threadRef.get();

        if (thread != null) {
            standaloneServerFactory.shutdown();
            // Wait for shutdown
            Uninterruptibly.join(10000, thread);
            threadRef.compareAndSet(thread, null);
        }
    }

    public void clear() {
        if (threadRef.get() == null) {
            Utils.removeDirectory(zkDir);
        } else {
            throw new IllegalStateException("server running");
        }
    }

    public String connectString() {
        return "127.0.0.1:" + zkPort;
    }

}
