package io.kestra.plugin.git;

import io.kestra.core.models.annotations.PluginProperty;
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

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractGitTask extends Task {
    private static final Pattern PEBBLE_TEMPLATE_PATTERN = Pattern.compile("^\\s*\\{\\{");

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
