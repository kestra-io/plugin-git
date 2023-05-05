package io.kestra.plugin.git.services;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import lombok.AllArgsConstructor;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.transport.ssh.jsch.JschConfigSessionFactory;
import org.eclipse.jgit.transport.ssh.jsch.OpenSshConfig;
import org.eclipse.jgit.util.FS;

import java.io.File;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor
public class SshTransportConfigCallback implements TransportConfigCallback {
    private File privateKey;
    private String passphrase;

    @Override
    public void configure(Transport transport) {
        SshTransport sshTransport = (SshTransport) transport;
        sshTransport.setSshSessionFactory(new JschConfigSessionFactory() {
            @Override
            protected void configure(OpenSshConfig.Host hc, Session session) {
                session.setConfig("StrictHostKeyChecking", "no");
            }

            @Override
            protected JSch getJSch(final OpenSshConfig.Host hc, FS fs) throws JSchException {
                JSch jsch = super.getJSch(hc, fs);
                jsch.removeAllIdentity();

                if (passphrase != null) {
                    jsch.addIdentity(
                        privateKey.getAbsolutePath(),
                        passphrase.getBytes(StandardCharsets.UTF_8)
                    );
                } else {
                    jsch.addIdentity(privateKey.getAbsolutePath());
                }

                return jsch;
            }
        });
    }
}
