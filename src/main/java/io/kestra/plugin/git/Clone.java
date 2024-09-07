package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.slf4j.Logger;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Clone a repository."
)
@Plugin(
    examples = {
        @Example(
            title = "Clone a public GitHub repository.",
            full = true,
            code = """
                id: git_clone
                namespace: company.team

                tasks:
                  - id: clone
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/dbt-labs/jaffle_shop
                    branch: main
                """
        ),
        @Example(
            title = "Clone a private repository from an HTTP server such as a private GitHub repository using a [personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token).",
            full = true,
            code = """
                id: git_clone
                namespace: company.team

                tasks:
                  - id: clone
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/kestra-io/examples
                    branch: main
                    username: git_username
                    password: your_personal_access_token
                """
        ),
        @Example(
            title = "Clone a repository from an SSH server. If you want to clone the repository into a specific directory, you can configure the `directory` property as shown below.",
            full = true,
            code = """
                id: git_clone
                namespace: company.team

                tasks:
                  - id: clone
                    type: io.kestra.plugin.git.Clone
                    url: git@github.com:kestra-io/kestra.git
                    directory: kestra
                    privateKey: <keyfile_content>
                    passphrase: <passphrase>
                """
        ),
        @Example(
            title = "Clone a GitHub repository and run a Python ETL script. Note that the `Worker` task is required so that the Python script shares the same local file system with files cloned from GitHub in the previous task.",
            full = true,
            code = """
                id: git_python
                namespace: company.team
                
                tasks:
                  - id: file_system
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone_repository
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/examples
                        branch: main
                      - id: python_etl
                        type: io.kestra.plugin.scripts.python.Commands
                        beforeCommands:
                          - pip install requests pandas > /dev/null
                        commands:
                          - python examples/scripts/etl_script.py
                """
        )
    }
)
public class Clone extends AbstractCloningTask implements RunnableTask<Clone.Output> {
    @Schema(
        title = "The optional directory associated with the clone operation.",
        description = "If the directory isn't set, the current directory will be used."
    )
    @PluginProperty(dynamic = true)
    private String directory;

    private String branch;

    @Schema(
        title = "Creates a shallow clone with a history truncated to the specified number of commits."
    )
    @PluginProperty
    @Builder.Default
    @Min(1)
    private Integer depth = 1;

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String url = runContext.render(this.url);

        Path path = runContext.workingDir().path();
        if (this.directory != null) {
            String directory = runContext.render(this.directory);
            path = runContext.workingDir().resolve(Path.of(directory));
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

        cloneCommand = authentified(cloneCommand, runContext);

        logger.info("Start cloning from '{}'", url);

        try (Git call = cloneCommand.call()) {
            return Output.builder()
                .directory(call.getRepository().getDirectory().getParent())
                .build();
        }
    }

    @Override
    @NotNull
    public String getUrl() {
        return super.getUrl();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The path where the repository is cloned."
        )
        private final String directory;
    }
}
