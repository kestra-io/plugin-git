package io.kestra.plugin.git;

import java.nio.file.Path;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.shared.AbstractCloningTask;
import io.kestra.plugin.git.shared.services.CloneService;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Clone a Git repository",
    description = "Clones a repository over HTTP(S) or SSH, optionally checking out a branch, tag, or commit. Defaults to a shallow clone (depth 1) unless a tag or commit is requested; set `cloneSubmodules` to fetch submodules."
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
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/blueprints
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
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        url: https://github.com/kestra-io/blueprints
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
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        url: git@github.com:kestra-io/kestra.git
                        directory: kestra
                        privateKey: "{{ secret('SSH_PRIVATE_KEY') }}"
                        passphrase: "{{ secret('SSH_PASSPHRASE') }}"
                """
        ),
        @Example(
            title = "Clone a GitHub repository and run a Python ETL script. Note that the `WorkingDirectory` task is required so that the Python script shares the same local file system with files cloned from GitHub in the previous task.",
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
                        dependencies:
                          - requests
                          - pandas
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
        title = "Target directory",
        description = "Subdirectory under the working directory where the repo is cloned; defaults to the working directory root."
    )
    @PluginProperty(group = "destination")
    private Property<String> directory;

    @Schema(
        title = "Branch to checkout",
        description = "Used only when no commit or tag is specified."
    )
    @PluginProperty(group = "advanced")
    private Property<String> branch;

    @Schema(
        title = "Shallow clone depth",
        description = "Defaults to 1. Ignored when `commit` or `tag` is set to ensure history is available."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> depth = Property.ofValue(1);

    @Schema(
        title = "Commit SHA to checkout",
        description = "Detached HEAD checkout; short SHA allowed. Overrides `branch` and disables shallow clone."
    )
    @PluginProperty(group = "advanced")
    private Property<String> commit;

    @Schema(
        title = "Tag to checkout",
        description = "Ignored when `commit` is set; performs a full fetch to reach the tag."
    )
    @PluginProperty(group = "advanced")
    private Property<String> tag;

    @Schema(
        title = "Clone all branches",
        description = "When true, clones all remote branches. When false, follows single-branch clone behavior."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> cloneAllBranches = Property.ofValue(true);

    @Schema(
        title = "Do not fetch tags",
        description = "When true, skip fetching tags during clone and fetch fallback."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> noTags = Property.ofValue(false);

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {

        String url = runContext.render(this.url).as(String.class).orElse(null);
        var cloneOptions = resolveCloneOptions(runContext);

        Path path = runContext.workingDir().path();
        if (this.directory != null) {
            String directory = runContext.render(this.directory).as(String.class).orElseThrow();
            path = runContext.workingDir().resolve(Path.of(directory));
        }

        configureHttpTransport(runContext);

        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        var rDepth = (this.commit == null && this.tag == null) ? runContext.render(this.depth).as(Integer.class).orElse(1) : null;

        // CloneService.clone() already logs the start and any transport failure, so both editions share a single log line.
        var result = CloneService.clone(
            runContext, this, CloneService.CloneRequest.builder()
                .url(url)
                .path(path)
                .branch(cloneOptions.branch())
                .depth(rDepth)
                .commit(this.commit != null ? runContext.render(this.commit).as(String.class).orElseThrow() : null)
                .tag(this.tag != null ? runContext.render(this.tag).as(String.class).orElseThrow() : null)
                .cloneAllBranches(cloneOptions.cloneAllBranches())
                .noTags(cloneOptions.noTags())
                .cloneSubmodules(this.cloneSubmodules)
                .build()
        );

        return Output.builder().directory(result.directory()).build();
    }

    private CloneOptions resolveCloneOptions(RunContext runContext) throws Exception {
        var rBranch = runContext.render(this.branch).as(String.class).orElse(null);
        var rCloneAllBranches = runContext.render(this.cloneAllBranches).as(Boolean.class).orElse(true);
        var rNoTags = runContext.render(this.noTags).as(Boolean.class).orElse(false);

        if (!rCloneAllBranches) {
            if (rBranch == null || rBranch.isBlank()) {
                throw new IllegalArgumentException(
                    "Invalid clone configuration: when `cloneAllBranches` is false, `branch` must be set."
                );
            }
        }

        if (this.tag != null && rNoTags) {
            throw new IllegalArgumentException("Invalid clone configuration: `tag` cannot be used with `noTags: true`.");
        }

        return new CloneOptions(rBranch, rCloneAllBranches, rNoTags);
    }

    private record CloneOptions(
        String branch,
        boolean cloneAllBranches,
        boolean noTags) {
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
            title = "Path where the repository is cloned"
        )
        private final String directory;
    }
}
