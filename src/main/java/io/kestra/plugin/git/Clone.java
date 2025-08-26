package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.transport.RefSpec;
import org.slf4j.Logger;

import java.nio.file.Path;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Clone a Git repository."
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
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
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
        ),
        @Example(
            title = "Clone then checkout a specific commit (detached HEAD).",
            full = true,
            code = """
                id: git_clone_commit
                namespace: company.team

                tasks:
                  - id: clone_at_sha
                    type: io.kestra.plugin.git.Clone
                    url: https://github.com/kestra-io/kestra
                    commit: 98189392a2a4ea0b1a951cd9dbbfe72f0193d77b
                """
        ),
    }
)
public class Clone extends AbstractCloningTask implements RunnableTask<Clone.Output> {
    @Schema(
        title = "The optional directory associated with the clone operation",
        description = "If the directory isn't set, the current directory will be used."
    )
    private Property<String> directory;

    @Schema(
        title = "The branch to checkout -- ignored if \"commit\" is provided."
    )
    private Property<String> branch;

    @Schema(
        title = "Creates a shallow clone with a history truncated to the specified number of commits.\nIgnored when `commit` is provided to guarantee the commit is available.")
    @Builder.Default
    private Property<Integer> depth = Property.ofValue(1);

    @Schema(
        title = "Commit SHA1 to checkout (detached HEAD) -- works also with a shortened SHA1.",
        description = "If set, the repository is cloned and the specified commit is checked out. This takes precedence over `branch` and disables shallow cloning to ensure the commit is present."
    )
    private Property<String> commit;

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String url = runContext.render(this.url).as(String.class).orElse(null);

        Path path = runContext.workingDir().path();
        if (this.directory != null) {
            String directory = runContext.render(this.directory).as(String.class).orElseThrow();
            path = runContext.workingDir().resolve(Path.of(directory));
        }

        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        CloneCommand cloneCommand = Git.cloneRepository()
            .setURI(url)
            .setDirectory(path.toFile());

        // If a specific commit is requested, we must not do a shallow clone;
        // otherwise the target commit might not be present.
        boolean hasCommit = this.commit != null;

        if (!hasCommit) {
            if (this.branch != null) {
                cloneCommand.setBranch(runContext.render(this.branch).as(String.class).orElse(null));
            }

            var rDepth = runContext.render(this.depth).as(Integer.class).orElse(1);
            if (this.depth != null) {
                cloneCommand.setDepth(rDepth);
            }
        }

        if (this.cloneSubmodules != null) {
            cloneCommand.setCloneSubmodules(runContext.render(this.cloneSubmodules).as(Boolean.class).orElseThrow());
        }

        cloneCommand = authentified(cloneCommand, runContext);

        logger.info("Start cloning from '{}'", url);

        try (Git git = cloneCommand.call()) {
            applyGitConfig(git.getRepository(), runContext);

            if (hasCommit) {
                String sha = runContext.render(this.commit).as(String.class).orElseThrow();

                // Ensure we have a full history in case the repo was shallow by default on the remote
                // or if the requested SHA is deep in history.
                try {
                    // Fetch all branches and tags to guarantee the target commit is available locally.
                    git.fetch()
                        .setRefSpecs(
                            new RefSpec("+refs/heads/*:refs/remotes/origin/*"),
                            new RefSpec("+refs/tags/*:refs/tags/*")
                        )
                        .call();
                } catch (Exception fetchEx) {
                    logger.warn("Fetch before checkout failed: {}", fetchEx.getMessage());
                }

                ObjectId target;
                try {
                    target = git.getRepository().resolve(sha);
                    if (target == null) {
                        throw new IllegalArgumentException("Cannot resolve commit SHA: " + sha);
                    }
                    git.checkout().setName(target.name()).call();
                    logger.info("Checked out commit {} (detached HEAD)", target.name());
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to checkout commit " + sha + ": " + e.getMessage(), e);
                }
            } else if (this.branch != null) {
                // If a branch was provided, ensure we're on it (useful when depth is set)
                String targetBranch = runContext.render(this.branch).as(String.class).orElse(null);
                if (targetBranch != null) {
                    git.checkout().setName(targetBranch).call();
                }
            }

            return Output.builder()
                .directory(git.getRepository().getDirectory().getParent())
                .build();
        }
    }

    @Override
    @NotNull
    public Property<String> getUrl() {
        return super.getUrl();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The path where the repository is cloned"
        )
        private final String directory;
    }
}
