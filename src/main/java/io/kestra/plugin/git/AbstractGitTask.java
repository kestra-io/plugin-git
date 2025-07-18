package io.kestra.plugin.git;

import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.services.SshTransportConfigCallback;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractGitTask extends Task {
    private static final Pattern PEBBLE_TEMPLATE_PATTERN = Pattern.compile("^\\s*\\{\\{");
    private static volatile boolean sslConfigured = false;
    private static final Object SSL_CONFIG_LOCK = new Object();

    @Schema(
        title = "The URI to clone from."
    )
    protected Property<String> url;

    @Schema(
        title = "The username or organization."
    )
    protected Property<String> username;

    @Schema(
        title = "The password or Personal Access Token (PAT). When you authenticate the task with a PAT, any flows or files pushed to Git from Kestra will be pushed from the user associated with that PAT. This way, you don't need to configure the commit author (the `authorName` and `authorEmail` properties)."
    )
    protected Property<String> password;

    @Schema(
        title = "PEM-format private key content that is paired with a public key registered on Git.",
        description = "To generate an ECDSA PEM format key from OpenSSH, use the following command: `ssh-keygen -t ecdsa -b 256 -m PEM`. " +
            "You can then set this property with your private key content and put your public key on Git."
    )
    protected Property<String> privateKey;

    @Schema(
        title = "The passphrase for the `privateKey`."
    )
    protected Property<String> passphrase;

    @Schema(
        title = "The initial Git branch."
    )
    public abstract Property<String> getBranch();

    @Schema(
        title = "SSL Options",
        description = "Allows to configure Ssl options."
    )
    protected SslOptions sslOptions;

    protected void configureEnvironmentWithSsl(RunContext runContext) throws Exception {
     if (sslOptions != null && Boolean.TRUE.equals(runContext.render(sslOptions.getInsecureTrustAllCertificates()).as(Boolean.class).orElse(false))) {
            configureGlobalSsl();
        }
    }

    private void configureGlobalSsl() throws Exception {
        synchronized (SSL_CONFIG_LOCK) {
            if (sslConfigured) {
                return;
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                public void checkClientTrusted(java.security.cert.X509Certificate[] xcs, String string) { }
                public void checkServerTrusted(java.security.cert.X509Certificate[] xcs, String string) { }
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return new java.security.cert.X509Certificate[0];
                }
            }}, new java.security.SecureRandom());

            SSLContext.setDefault(sslContext);
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> {
                javax.net.ssl.HostnameVerifier defaultVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
                return defaultVerifier.verify(hostname, session);
            });

            sslConfigured = true;
        }
    }

    public <T extends TransportCommand<T, ?>> T authentified(T command, RunContext runContext) throws Exception {
        if (this.username != null && this.password != null) {
            command.setCredentialsProvider(new UsernamePasswordCredentialsProvider(
                runContext.render(this.username).as(String.class).orElseThrow(),
                runContext.render(this.password).as(String.class).orElseThrow()
            ));
        }

        if (this.privateKey != null) {
            command.setTransportConfigCallback(new SshTransportConfigCallback(
                runContext.render(this.privateKey).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8),
                runContext.render(this.passphrase).as(String.class).orElse(null)
            ));
        }

        return command;
    }
}