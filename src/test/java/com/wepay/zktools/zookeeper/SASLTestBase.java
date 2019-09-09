package com.wepay.zktools.zookeeper;

import com.wepay.zktools.test.ZKTestUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class SASLTestBase extends ZKTestUtils {

    protected File configJaas(File confDir) throws IOException {
        File jaasConfFile = new File(confDir, "jaas.conf");
        try (Writer w = new OutputStreamWriter(new FileOutputStream(jaasConfFile, false), StandardCharsets.UTF_8)) {
            writeln(w, "Server {");
            writeln(w, "  org.apache.zookeeper.server.auth.DigestLoginModule required");
            writeln(w, "  user_user1=\"passwd1\";");
            writeln(w, "};");
        }

        System.setProperty("java.security.auth.login.config", jaasConfFile.getAbsolutePath());
        System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty("zookeeper.allowSaslFailedClients", "true");

        return jaasConfFile;
    }

    protected void appendClientJaasConfig(File jaasConfFile) throws IOException {
        try (Writer w = new OutputStreamWriter(new FileOutputStream(jaasConfFile, true), StandardCharsets.UTF_8)) {
            writeln(w, "Client {");
            writeln(w, "  org.apache.zookeeper.server.auth.DigestLoginModule required");
            writeln(w, "  username=\"user1\"");
            writeln(w, "  password=\"passwd1\";");
            writeln(w, "};");
        }
    }

    protected void clearSystemProperties() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("zookeeper.authProvider.1");
        System.clearProperty("zookeeper.allowSaslFailedClients");
    }

    private static void writeln(Writer w, String line) throws IOException {
        w.write(line);
        w.write('\n');
    }

}
