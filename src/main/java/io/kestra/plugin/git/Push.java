package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.tasks.InputFilesInterface;
import io.kestra.core.models.tasks.NamespaceFiles;
import io.kestra.core.models.tasks.NamespaceFilesInterface;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.NamespaceFilesService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.EmptyCommitException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.slf4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.eclipse.jgit.lib.Constants.R_HEADS;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Commit and push files to a Git repository.",
    description = "You can use this task to push your flows and namespace files to Git. To do that, you can set the `enabled` child property of `flows` and/or `namespaceFiles` to `true`. You can also add additional `inputFiles` to be committed and pushed. Furthermore, you can use this task in combination with the `Clone` task so that you can first clone the repository, then add or modify files and push to Git afterwards. " +
        "Check the examples below as well as the [Version Control with Git](https://kestra.io/docs/developer-guide/git) documentation for more information."
)
@Plugin(
    examples = {
        @Example(
            title = "Push flows and namespace files to a Git repository every 15 minutes.",
            full = true,
            code = {
                "id: push_to_git",
                "namespace: prod",
                "",
                "tasks:",
                "  - id: commit_and_push",
                "    type: io.kestra.plugin.git.Push",
                "    namespaceFiles:",
                "      enabled: true",
                "    flows:",
                "      enabled: true",
                "    url: https://github.com/kestra-io/scripts",
                "    branch: kestra",
                "    username: git_username",
                "    password: \"{{ secret('GITHUB_ACCESS_TOKEN') }}\"",
                "    commitMessage: \"add flows and scripts {{ now() }}\"",
                "",
                "triggers:",
                "  - id: schedule_push",
                "    type: io.kestra.core.models.triggers.types.Schedule",
                "    cron: \"*/15 * * * *\""
            }
        ),
        @Example(
            title = "Clone the main branch, generate a file in a script, and then push that new file to Git. " +
                "Since we're in a working directory with a `.git` directory, you don't need to specify the URL in the Push task. " +
                "However, the Git credentials always need to be explicitly provided on both Clone and Push tasks (unless using task defaults).",
            full = true,
            code = {
                "id: push_new_file_to_git",
                "namespace: dev",
                "",
                "inputs:",
                "  - id: commit_message",
                "    type: STRING",
                "    defaults: add a new file to Git",
                "",
                "tasks:",
                "  - id: wdir",
                "    type: io.kestra.core.tasks.flows.WorkingDirectory",
                "    tasks:",
                "      - id: clone",
                "        type: io.kestra.plugin.git.Clone",
                "        branch: main",
                "        url: https://github.com/kestra-io/scripts",
                "      - id: generate_data",
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
                "          to_commit/avg_order.txt: \"{{ outputs.generate_data.vars.average_order }}\"",
                "        addFilesPattern:",
                "          - to_commit",
                "        commitMessage: \"{{ inputs.commit_message }}\""
            }
        )
    }
)
@Deprecated
public class Push extends AbstractGitTask implements RunnableTask<Push.Output>, NamespaceFilesInterface, InputFilesInterface {
    @Schema(
        title = "The optional directory associated with the clone operation.",
        description = "If the directory isn't set, the current directory will be used."
    )
    @PluginProperty(dynamic = true)
    private String directory;

    @Schema(
        title = "The branch to which files should be committed and pushed.",
        description = "If the branch doesn't exist yet, it will be created."
    )
    @NotNull
    private String branch;

    @Schema(
        title = "Commit message."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String commitMessage;

    private NamespaceFiles namespaceFiles;

    @Schema(
        title = "Whether to push flows from the current namespace to Git."
    )
    @PluginProperty
    @Builder.Default
    private FlowFiles flows = FlowFiles.builder().build();

    private Object inputFiles;

    @Schema(
        title = "Patterns of files to add to the commit. Default is `.` which means all files.",
        description = "A directory name (e.g. `dir` to add `dir/file1` and `dir/file2`) can also be given to add all files in the directory, recursively. File globs (e.g. `*.py`) are not yet supported."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private List<String> addFilesPattern = List.of(".");

    @Schema(title = "Commit author.")
    @PluginProperty
    private Author author;

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

        Git git = Git.open(basePath.toFile());

        if (Optional.ofNullable(git.getRepository().getBranch()).map(b -> !b.equals(branch)).orElse(true)) {
            git.checkout()
                .setName(branch)
                .setCreateBranch(true)
                .call();
        }

        if (this.url != null) {
            RmCommand rm = git.rm();
            Stream<String> previouslyTrackedRelativeFilePaths = Arrays.stream(basePath.toFile().listFiles())
                .filter(file -> !file.isDirectory() || !file.getName().equals(".git"))
                .map(File::toPath)
                .map(basePath::relativize)
                .map(Path::toString);
            previouslyTrackedRelativeFilePaths.forEach(rm::addFilepattern);
            rm.call();
        }

        if (this.inputFiles != null) {
            FilesService.inputFiles(runContext, this.inputFiles);
        }

        Map<String, String> flowProps = Optional.ofNullable((Map<String, String>) runContext.getVariables().get("flow")).orElse(Collections.emptyMap());
        String tenantId = flowProps.get("tenantId");
        String namespace = flowProps.get("namespace");
        if (this.namespaceFiles != null) {

            NamespaceFilesService namespaceFilesService = runContext.getApplicationContext().getBean(NamespaceFilesService.class);
            namespaceFilesService.inject(
                runContext,
                tenantId,
                namespace,
                runContext.tempDir(),
                this.namespaceFiles
            );
        }

        if (Boolean.TRUE.equals(this.flows.enabled)) {
            FlowRepositoryInterface flowRepository = runContext.getApplicationContext().getBean(FlowRepositoryInterface.class);

            List<FlowWithSource> flows;
            if (Boolean.TRUE.equals(this.flows.childNamespaces)) {
                flows = flowRepository.findWithSource(null, tenantId, namespace, null);
            } else {
                flows = flowRepository.findByNamespaceWithSource(tenantId, namespace);
            }

            Path flowsDirectory = this.flows.gitDirectory == null
                ? basePath
                : basePath.resolve(runContext.render(this.flows.gitDirectory));

            // Create flow directory if it doesn't exist
            flowsDirectory.toFile().mkdirs();

            flows.forEach(throwConsumer(flowWithSource -> FileUtils.writeStringToFile(
                flowsDirectory.resolve(flowWithSource.getNamespace() + "." + flowWithSource.getId() + ".yml").toFile(),
                flowWithSource.getSource(),
                StandardCharsets.UTF_8
            )));
        }

        logger.info(
            "Pushing to {}/tree/{}",
            git.getRepository().getConfig().getString("remote", "origin", "url"),
            git.getRepository().getBranch()
        );

        AddCommand add = git.add();
        runContext.render(this.addFilesPattern).forEach(add::addFilepattern);
        add.call();

        ObjectId commitId = null;
        try {
            commitId = git.commit()
                .setAllowEmpty(false)
                .setMessage(runContext.render(this.commitMessage))
                .setAuthor(author(runContext))
                .call()
                .getId();
            authentified(git.push(), runContext).call();
        } catch (EmptyCommitException e) {
            logger.info("No changes to commit. Skipping push.");
        }

        git.close();

        return Output.builder()
            .commitId(
                Optional.ofNullable(commitId)
                    .map(ObjectId::getName)
                    .orElse(null)
            ).build();
    }

    private PersonIdent author(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.author == null) {
            return null;
        }
        if (this.author.email != null && this.author.name != null) {
            return new PersonIdent(runContext.render(this.author.name), runContext.render(this.author.email));
        }
        if (this.author.email != null && this.username != null) {
            return new PersonIdent(runContext.render(this.username), runContext.render(this.author.email));
        }

        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "ID of the commit pushed."
        )
        @Nullable
        private final String commitId;
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @Jacksonized
    public static class FlowFiles {
        @Schema(
            title = "Whether to push flows as YAML files to Git."
        )
        @PluginProperty
        @Builder.Default
        private Boolean enabled = true;

        @Schema(
            title = "Whether flows from child namespaces should be included."
        )
        @PluginProperty
        @Builder.Default
        private Boolean childNamespaces = true;

        @Schema(
            title = "To which directory flows should be pushed (relative to `directory`).",
            description = "The default is `_flows`. This is the same directory name that you can see in the VS Code Editor."
        )
        @PluginProperty(dynamic = true)
        @Builder.Default
        private String gitDirectory = "_flows";
    }

    @Builder
    @Getter
    public static class Author {
        @Schema(title = "The commit author name, if null the username will be used instead")
        @PluginProperty(dynamic = true)
        private String name;

        @Schema(title = "The commit author email, if null no author will be set on this commit")
        @PluginProperty(dynamic = true)
        private String email;
    }
}
