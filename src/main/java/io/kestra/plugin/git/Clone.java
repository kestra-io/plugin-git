package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.services.SshTransportConfigCallback;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Clone a repository"
)
@Plugin(
    examples = {
        @Example(
            title = "Clone a public GitHub repository",
            code = {
                "url: https://github.com/dbt-labs/jaffle_shop",
                "branch: main",
            }
        ),
        @Example(
            title = "Clone a private repository from an HTTP server such as a private GitHub repository using a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)",
            code = {
                "url: https://github.com/anna-geller/kestra-flows",
                "branch: main",
                "username: anna-geller",
                "password: your_personal_access_token"
            }
        ),
        @Example(
            title = "Clone a repository from an SSH server. If you want to clone the repository into a specific directory, you can configure the `directory` property as shown below.",
            code = {
                "url: git@github.com:kestra-io/kestra.git",
                "directory: kestra",
                "privateKey: <keyfile>",
                "passphrase: <passphrase>"
            }
        ),
        @Example(
            full = true,
            title = "Clone a GitHub repository and run a Python ETL script. Note that the `Worker` task is required so that the Python script shares the same local file system with files cloned from GitHub in the previous task.",
            code = {
                "id: gitPython",
                "namespace: prod",
                "",
                "tasks:",
                "  - id: fileSystem",
                "    type: io.kestra.core.tasks.flows.WorkingDirectory",
                "    tasks:",
                "      - id: cloneRepository",
                "        type: io.kestra.plugin.git.Clone",
                "        url: https://github.com/anna-geller/kestra-flows",
                "        branch: main",

                "      - id: pythonETL",
                "        type: io.kestra.plugin.scripts.python.Commands",
                "        beforeCommands:",
                "          - pip install requests pandas > /dev/null",
                "        commands:",
                "          - ./bin/python flows/etl_script.py",
            }
        )
    }
)
public class Clone extends Task implements RunnableTask<Clone.Output> {
    @Schema(
        title = "The URI to clone from"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String url;

    @Schema(
        title = "The optional directory associated with the clone operation.",
        description = "If the directory isn't set, the current directory will be used."
    )
    @PluginProperty(dynamic = true)
    private String directory;

    @Schema(
        title = "The initial Git branch."
    )
    @PluginProperty(dynamic = true)
    private String branch;

    @Schema(
        title = "Creates a shallow clone with a history truncated to the specified number of commits."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    @Min(1)
    private Integer depth = 1;

    @Schema(
        title = "Whether to clone submodules."
    )
    @PluginProperty(dynamic = false)
    private Boolean cloneSubmodules;

    @Schema(
        title = "The username or organization."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "The password or personal access token."
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "The private keyfile used to connect."
    )
    @PluginProperty(dynamic = true)
    protected String privateKey;

    @Schema(
        title = "The passphrase for the `privateKey`."
    )
    @PluginProperty(dynamic = true)
    protected String passphrase;

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String url = runContext.render(this.url);

        Path path = runContext.tempDir();
        if (this.directory != null) {
            String directory = runContext.render(this.directory);

            if (directory.startsWith("./") || directory.startsWith("..") || directory.startsWith("/")) {
                throw new IllegalArgumentException("Invalid directory (only relative path is supported) for path '" + directory + "'");
            }

            path = path.resolve(directory);
        }

        CloneCommand cloneCommand = Git.cloneRepository()
            .setURI(url)
            .setDirectory(path.toFile());

        if (this.branch != null) {
            cloneCommand.setBranch(runContext.render(this.branch));
        }

        if (this.depth != null) {
            cloneCommand.setDepth(this.depth);
        }

        if (this.cloneSubmodules != null) {
            cloneCommand.setCloneSubmodules(this.cloneSubmodules);
        }

        if (this.username != null && this.password != null) {
            cloneCommand.setCredentialsProvider(new UsernamePasswordCredentialsProvider(
                runContext.render(this.username),
                runContext.render(this.password)
            ));
        }

        if (this.privateKey != null) {
            Path privateKey = runContext.tempFile(
                runContext.render(this.privateKey).getBytes(StandardCharsets.UTF_8),
                ""
            );

            Files.setPosixFilePermissions(privateKey, Set.of(PosixFilePermission.OWNER_READ));

            cloneCommand.setTransportConfigCallback(new SshTransportConfigCallback(
                privateKey.toFile(),
                runContext.render(this.passphrase)
            ));
        }

        logger.info("Start cloning from '{}'", url);

        try (Git call = cloneCommand.call()) {
            return Output.builder()
                .directory(call.getRepository().getDirectory().getAbsolutePath())
                .build();
        }
}

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Short description for this output",
            description = "Full description of this output"
        )
        private final String directory;
    }
}
