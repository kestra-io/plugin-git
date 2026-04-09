package io.kestra.plugin.git;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.FetchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.TagOpt;
import org.eclipse.jgit.transport.URIish;
import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

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
    @PluginProperty(group = "advanced")
    private Property<Boolean> cloneAllBranches;

    @Schema(
        title = "Specific branches to clone",
        description = "Optional list of branch names to fetch (e.g. `main`), equivalent to JGit `setBranchesToClone`."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> branchesToClone;

    @Schema(
        title = "Do not fetch tags",
        description = "When true, skip fetching tags during clone and fetch fallback."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> noTags;

    @Override
    public Clone.Output run(RunContext runContext) throws Exception {

        Logger logger = runContext.logger();
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

        // If a specific commit or tag is requested, we must not do a shallow clone;
        // otherwise the target may not be present.
        boolean hasCommit = this.commit != null;
        boolean hasTag = this.tag != null;

        logger.info("Start cloning from '{}'", url);

        // When the target directory already contains files (e.g. from WorkingDirectory inputFiles),
        // JGit's CloneCommand fails with "Destination path already exists and is not an empty directory".
        // In that case, use git init + fetch + checkout instead.
        if (isNonEmptyDirectory(path)) {
            logger.info("Target directory '{}' is not empty, using init+fetch strategy", path);
            return initFetchCheckout(runContext, logger, url, path, hasCommit, hasTag, cloneOptions);
        }

        // Ensure the directory exists for clone
        Files.createDirectories(path);

        CloneCommand cloneCommand = Git.cloneRepository()
            .setURI(url)
            .setDirectory(path.toFile());

        if (!hasCommit && !hasTag) {
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

        if (this.cloneAllBranches != null) {
            cloneCommand.setCloneAllBranches(cloneOptions.cloneAllBranches);
        }

        if (!cloneOptions.branchesToClone.isEmpty()) {
            cloneCommand.setBranchesToClone(cloneOptions.branchesToClone.stream().map(Clone::normalizeBranchRef).toList());
        }

        if (cloneOptions.noTags) {
            cloneCommand.setNoTags();
        }

        cloneCommand = authentified(cloneCommand, runContext);

        try (var git = cloneCommand.call()) {
            applyGitConfig(git.getRepository(), runContext);
            postCloneCheckout(git, runContext, logger, hasCommit, hasTag, cloneOptions.noTags);

            return Output.builder()
                .directory(git.getRepository().getDirectory().getParent())
                .build();
        }
    }

    /**
     * Handles post-clone checkout for commit, tag, or branch.
     */
    private void postCloneCheckout(Git git, RunContext runContext, Logger logger, boolean hasCommit, boolean hasTag, boolean noTags) throws Exception {
        if (hasCommit) {
            var rSha = runContext.render(this.commit).as(String.class).orElseThrow();
            checkoutCommit(git, rSha, logger, noTags);
        } else if (hasTag) {
            var rTagName = runContext.render(this.tag).as(String.class).orElseThrow();
            checkoutTag(git, rTagName, logger, noTags);
        } else if (this.branch != null) {
            var rTargetBranch = runContext.render(this.branch).as(String.class).orElse(null);
            checkoutBranch(git, rTargetBranch, logger);
        }
    }

    /**
     * Clones into a non-empty directory using git init + fetch + checkout.
     * This is the standard Git workaround when the target directory already contains files
     * (e.g. from WorkingDirectory inputFiles).
     */
    private Clone.Output initFetchCheckout(RunContext runContext, Logger logger, String url, Path path, boolean hasCommit, boolean hasTag, CloneOptions cloneOptions) throws Exception {
        try (var git = Git.init().setDirectory(path.toFile()).call()) {
            git.remoteAdd()
                .setName("origin")
                .setUri(new URIish(url))
                .call();

            applyGitConfig(git.getRepository(), runContext);

            List<RefSpec> refSpecs = new ArrayList<>();

            if (!cloneOptions.branchesToClone.isEmpty()) {
                for (String branchToClone : cloneOptions.branchesToClone) {
                    String branchName = shortBranchName(branchToClone);
                    refSpecs.add(new RefSpec("+refs/heads/" + branchName + ":refs/remotes/origin/" + branchName));
                }
            } else if (Boolean.FALSE.equals(cloneOptions.cloneAllBranches) && cloneOptions.branch != null) {
                String branchName = shortBranchName(cloneOptions.branch);
                refSpecs.add(new RefSpec("+refs/heads/" + branchName + ":refs/remotes/origin/" + branchName));
            } else {
                // Preserve previous behavior when the new properties are not configured.
                refSpecs.add(new RefSpec("+refs/heads/*:refs/remotes/origin/*"));
            }

            if (!cloneOptions.noTags) {
                refSpecs.add(new RefSpec("+refs/tags/*:refs/tags/*"));
            }

            FetchCommand fetchCommand = git.fetch()
                .setRemote("origin")
                .setRefSpecs(refSpecs);

            if (cloneOptions.noTags) {
                fetchCommand.setTagOpt(TagOpt.NO_TAGS);
            }

            if (!hasCommit && !hasTag) {
                var rDepth = runContext.render(this.depth).as(Integer.class).orElse(1);
                if (this.depth != null) {
                    fetchCommand.setDepth(rDepth);
                }
            }

            authentified(fetchCommand, runContext).call();

            // Checkout the fetched content
            if (hasCommit) {
                var rSha = runContext.render(this.commit).as(String.class).orElseThrow();
                checkoutCommit(git, rSha, logger, cloneOptions.noTags);
            } else if (hasTag) {
                var rTagName = runContext.render(this.tag).as(String.class).orElseThrow();
                checkoutTag(git, rTagName, logger, cloneOptions.noTags);
            } else {
                // Resolve the target branch; fall back to detecting the remote HEAD default branch
                var targetBranch = cloneOptions.branch;
                if (targetBranch == null) {
                    var headRef = git.getRepository().exactRef("refs/remotes/origin/HEAD");
                    if (headRef != null && headRef.getTarget() != null) {
                        targetBranch = headRef.getTarget().getName().replace("refs/remotes/origin/", "");
                    }
                }

                // If still null, try common defaults
                if (targetBranch == null) {
                    if (git.getRepository().exactRef("refs/remotes/origin/main") != null) {
                        targetBranch = "main";
                    } else if (git.getRepository().exactRef("refs/remotes/origin/master") != null) {
                        targetBranch = "master";
                    } else {
                        throw new IllegalStateException(
                            "Cannot determine the default branch. Please specify the 'branch' property explicitly."
                        );
                    }
                }

                var remoteBranch = "origin/" + targetBranch;

                git.checkout()
                    .setName(targetBranch)
                    .setCreateBranch(true)
                    .setStartPoint(remoteBranch)
                    .call();

                logger.info("Checked out branch {} from {}", targetBranch, remoteBranch);
            }

            if (this.cloneSubmodules != null && runContext.render(this.cloneSubmodules).as(Boolean.class).orElse(false)) {
                git.submoduleInit().call();
                authentified(git.submoduleUpdate(), runContext).call();
            }

            return Output.builder()
                .directory(git.getRepository().getDirectory().getParent())
                .build();
        }
    }

    /**
     * Returns true if the path exists, is a directory, and contains at least one entry.
     */
    private static boolean isNonEmptyDirectory(Path path) {
        var dir = path.toFile();
        if (!dir.exists() || !dir.isDirectory()) {
            return false;
        }
        var entries = dir.list();
        return entries != null && entries.length > 0;
    }

    private static String normalizeBranchRef(String branch) {
        if (branch == null || branch.isBlank()) {
            return branch;
        }
        return branch.startsWith("refs/heads/") ? branch : "refs/heads/" + branch;
    }

    private static String shortBranchName(String branch) {
        if (branch == null) {
            return null;
        }
        return branch.startsWith("refs/heads/") ? branch.substring("refs/heads/".length()) : branch;
    }

    private CloneOptions resolveCloneOptions(RunContext runContext) throws Exception {
        String rBranch = this.branch != null
            ? runContext.render(this.branch).as(String.class).orElse(null)
            : null;
        Boolean rCloneAllBranches = this.cloneAllBranches != null
            ? runContext.render(this.cloneAllBranches).as(Boolean.class).orElse(false)
            : null;
        boolean rNoTags = this.noTags != null && runContext.render(this.noTags).as(Boolean.class).orElse(false);
        List<String> rBranchesToClone = this.branchesToClone != null
            ? runContext.render(this.branchesToClone).asList(String.class).stream()
                .filter(value -> value != null && !value.isBlank())
                .map(Clone::shortBranchName)
                .toList()
            : List.of();

        if (Boolean.FALSE.equals(rCloneAllBranches)) {
            if (rBranch == null || rBranch.isBlank()) {
                throw new IllegalArgumentException(
                    "Invalid clone configuration: when `cloneAllBranches` is false, `branch` must be set."
                );
            }

            String checkoutBranch = shortBranchName(rBranch);
            if (rBranchesToClone.isEmpty()) {
                rBranchesToClone = List.of(checkoutBranch);
            } else if (rBranchesToClone.stream().noneMatch(checkoutBranch::equals)) {
                throw new IllegalArgumentException(
                    "Invalid clone configuration: when `cloneAllBranches` is false, `branch` ('" +
                        checkoutBranch + "') must be included in `branchesToClone`."
                );
            }
        }

        if (this.tag != null && rNoTags) {
            throw new IllegalArgumentException("Invalid clone configuration: `tag` cannot be used with `noTags: true`.");
        }

        return new CloneOptions(rBranch, rCloneAllBranches, rBranchesToClone, rNoTags);
    }

    private record CloneOptions(
        String branch,
        Boolean cloneAllBranches,
        List<String> branchesToClone,
        boolean noTags
    ) {}

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
