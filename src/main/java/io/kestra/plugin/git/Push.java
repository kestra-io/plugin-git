package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.*;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.NamespaceFilesService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.lib.ObjectId;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.eclipse.jgit.lib.Constants.R_HEADS;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Pushes a commit to a repository.",
    description = "This task can be used in two ways: You can use it as a standalone task to both clone then commit directly with some added input or namespace files to a repository, " +
        "or you can use it in a `WorkingDirectory` task after a `Clone` task to do some in-between processing before pushing to a repository (execute some script for example)."
)
@Plugin(
    examples = {
        @Example(
            title = "Clone and push a commit to a repository in a single task.",
            code = {
                "namespaceFiles:",
                "  enabled: true",
                "branch: feature_branch # mandatory, we'll create the branch if not exists",
                "url: https://github.com/kestra-io/scripts",
                "username: git_username",
                "password: mypat",
                "commitMessage: \"{{ inputs.commit_message }}\" # required"
            }
        ),
        @Example(
            title = "Clone main branch, execute a script then push a commit which creates a branch on the repository with a file containing a script output. " +
                "Note that although URL is not required for push (since we're in a working directory with a .git directory), " +
                "credentials are still mandatory for push.",
            full = true,
            code = {
                "id: flow",
                "namespace: dev",
                "",
                "inputs:",
                "  - name: commit_message",
                "    type: STRING",
                "    defaults: my commit message",
                "",
                "tasks:",
                "  - id: wdir",
                "    type: io.kestra.core.tasks.flows.WorkingDirectory",
                "    tasks:",
                "      - id: clone",
                "        type: io.kestra.plugin.git.Clone",
                "        branch: main",
                "        url: https://github.com/kestra-io/scripts",
                "      - id: gen_data",
                "        type: io.kestra.plugin.scripts.python.Commands",
                "        docker:",
                "          image: ghcr.io/kestra-io/pydata:latest",
                "        commands:",
                "          - python generate_data/generate_orders.py",
                "      - id: push",
                "        type: io.kestra.plugin.git.Push",
                "        username: git_username",
                "        password: myPAT",
                "        branch: feature_branch",
                "        inputFiles:",
                "          to_commit/avg_order.txt: \"{{ outputs.gen_data.vars.average_order }}\"",
                "        addFilesPattern:",
                "          - to_commit",
                "        description: Adds all namespace files, commits and pushes them",
                "        commitMessage: \"{{ inputs.commit_message }}\""
            }
        )
    }
)
public class Push extends AbstractGitTask implements RunnableTask<Push.Output>, NamespaceFilesInterface, InputFilesInterface {
    @Schema(
        title = "The optional directory associated with the clone operation.",
        description = "If the directory isn't set, the current directory will be used."
    )
    @PluginProperty(dynamic = true)
    private String directory;

    @NotNull
    private String branch;

    @Schema(
        title = "Commit message to push."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String commitMessage;

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    @Schema(
        title = "Patterns of files to add to the commit. Default is `.` which means all files.",
        description = "A directory name (e.g. `dir` to add `dir/file1` and `dir/file2`) can also be given to add all files in the directory, recursively. File globs (e.g. *.c) are not yet supported."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private List<String> addFilesPattern = List.of(".");

    private boolean branchExists(RunContext runContext, String branch) throws Exception {
        if (this.url == null) {
            try (Git git = Git.open(runContext.resolve(Path.of(runContext.render(this.directory))).toFile())) {
                return git.branchList().setListMode(ListBranchCommand.ListMode.REMOTE).call().stream()
                    .anyMatch(ref -> ref.getName().equals(R_HEADS + branch));
            }
        }

        return authentified(Git.lsRemoteRepository().setRemote(runContext.render(url)), runContext)
            .callAsMap()
            .containsKey(R_HEADS + branch);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        Path basePath = runContext.tempDir();
        if (this.directory != null) {
            basePath = runContext.resolve(Path.of(runContext.render(this.directory)));
        }

        String branch = runContext.render(this.branch);
        if (this.url != null) {
            boolean branchExists = branchExists(runContext, branch);

            Clone cloneHead = Clone.builder()
                .depth(1)
                .url(this.url)
                .directory(this.directory)
                .username(this.username)
                .password(this.password)
                .privateKey(this.privateKey)
                .passphrase(this.passphrase)
                .build();

            if (branchExists) {
                cloneHead.toBuilder()
                    .branch(branch)
                    .build()
                    .run(runContext);
            } else {
                logger.info("Branch {} does not exist, creating it", branch);

                cloneHead.run(runContext);
            }
        }

        if (this.inputFiles != null) {
            FilesService.inputFiles(runContext, this.inputFiles);
        }

        if (this.namespaceFiles != null) {
            String tenantId = ((Map<String, String>) runContext.getVariables().get("flow")).get("tenantId");
            String namespace = ((Map<String, String>) runContext.getVariables().get("flow")).get("namespace");

            NamespaceFilesService namespaceFilesService = runContext.getApplicationContext().getBean(NamespaceFilesService.class);
            namespaceFilesService.inject(
                runContext,
                tenantId,
                namespace,
                runContext.tempDir(),
                this.namespaceFiles
            );
        }


        Git git = Git.open(basePath.toFile());

        if (Optional.ofNullable(git.getRepository().getBranch()).map(b -> !b.equals(branch)).orElse(true)) {
            git.checkout()
                .setName(branch)
                .setCreateBranch(true)
                .call();
        }

        logger.info(
            "Pushing to {}/tree/{}",
            git.getRepository().getConfig().getString("remote", "origin", "url"),
            git.getRepository().getBranch()
        );

        AddCommand add = git.add();
        runContext.render(this.addFilesPattern).forEach(add::addFilepattern);
        add.call();

        ObjectId commitId = git.commit().setMessage(runContext.render(this.commitMessage)).call().getId();

        authentified(git.push(), runContext).call();

        git.close();

        return Output.builder()
            .commitId(commitId.getName())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "ID of the commit pushed."
        )
        private final String commitId;
    }
}
