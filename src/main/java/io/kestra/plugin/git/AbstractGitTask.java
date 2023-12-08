package io.kestra.plugin.git;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.services.SshTransportConfigCallback;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractGitTask extends Task {
    @Schema(
        title = "The URI to clone from"
    )
    @PluginProperty(dynamic = true)
    protected String url;

    @Schema(
        title = "The username or organization."
    )
    @PluginProperty(dynamic = true)
    protected String username;

    @Schema(
        title = "The password or personal access token."
    )
    @PluginProperty(dynamic = true)
    protected String password;

    @Schema(
        title = "The private keyfile content used to connect."
    )
    @PluginProperty(dynamic = true)
    protected String privateKey;

    @Schema(
        title = "The passphrase for the `privateKey`."
    )
    @PluginProperty(dynamic = true)
    protected String passphrase;


    @Schema(
        title = "The initial Git branch."
    )
    @PluginProperty(dynamic = true)
    public abstract String getBranch();

    protected <T extends TransportCommand> T authentified(T command, RunContext runContext) throws Exception {
        if (this.username != null && this.password != null) {
            command.setCredentialsProvider(new UsernamePasswordCredentialsProvider(
                runContext.render(this.username),
                runContext.render(this.password)
            ));
        }

        if (this.privateKey != null) {
            Path privateKey = runContext.tempFile(
                runContext.render(this.privateKey).getBytes(StandardCharsets.UTF_8),
                null
            );

            Files.setPosixFilePermissions(privateKey, Set.of(PosixFilePermission.OWNER_READ));

            command.setTransportConfigCallback(new SshTransportConfigCallback(
                privateKey.toFile(),
                runContext.render(this.passphrase)
            ));
        }

        return command;
    }
}
