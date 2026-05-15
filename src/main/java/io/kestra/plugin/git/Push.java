package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.EmptyCommitException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.InputFilesInterface;
import io.kestra.core.models.tasks.NamespaceFiles;
import io.kestra.core.models.tasks.NamespaceFilesInterface;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.Rethrow;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.eclipse.jgit.lib.Constants.R_HEADS;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Deprecated(since = "1.0.0", forRemoval = true)
@Schema(
    deprecated = true,
    title = "Deprecated: push mixed files to Git",
    description = "Use PushFlows for flows and PushNamespaceFiles for namespace files. Keeps committing working-directory changes (including `inputFiles`) and pushes to Git; branch is created if missing. Prefer serialized pushes to avoid conflicts."
)
@Plugin(
    examples = {
        @Example(
            title = "Push flows and namespace files to a Git repository every 15 minutes.",
            full = true,
            code = """
                id: push_to_git
                namespace: company.team

                tasks:
                  - id: commit_and_push
                    type: io.kestra.plugin.git.Push
                    namespaceFiles:
                      enabled: true
                    flows:
                      enabled: true
                    url: https://github.com/kestra-io/scripts
                    branch: kestra
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    commitMessage: "add flows and scripts {{ now() }}"

                triggers:
                  - id: schedule_push
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/15 * * * *"
                """
        ),
        @Example(
            title = "Clone the main branch, generate a file in a script, and then push that new file to Git. " +
                "Since we're in a working directory with a `.git` directory, you don't need to specify the URL in the Push task. " +
                "However, the Git credentials always need to be explicitly provided on both Clone and Push tasks (unless using task defaults).",
            full = true,
            code = """
                id: push_new_file_to_git
                namespace: company.team

                inputs:
                  - id: commit_message
                    type: STRING
                    defaults: add a new file to Git

                tasks:
                  - id: wdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: clone
                        type: io.kestra.plugin.git.Clone
                        branch: main
                        url: https://github.com/kestra-io/scripts
                      - id: generate_data
                        type: io.kestra.plugin.scripts.python.Commands
                        taskRunner:
                          type: io.kestra.plugin.scripts.runner.docker.Docker
                        containerImage: ghcr.io/kestra-io/pydata:latest
                        commands:
                          - python generate_data/generate_orders.py
                      - id: push
                        type: io.kestra.plugin.git.Push
                        username: git_username
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: feature_branch
                        inputFiles:
                          to_commit/avg_order.txt: "{{ outputs.generate_data.vars.average_order }}"
                        addFilesPattern:
                          - to_commit
                        commitMessage: "{{ inputs.commit_message }}"
                """
        )
    }
)
public class Push extends AbstractCloningTask implements RunnableTask<Push.Output>, NamespaceFilesInterface, InputFilesInterface {
    @Schema(
        title = "Target directory",
        description = "Working-directory subfolder containing the Git repo; defaults to the working directory root."
    )
    @PluginProperty(group = "destination")
    private Property<String> directory;

    @Schema(
        title = "Branch to push",
        description = "Created if absent."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> branch;

    @Schema(
        title = "Commit message"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> commitMessage;

    @PluginProperty(group = "source")
    private NamespaceFiles namespaceFiles;

    @Schema(
        title = "Whether to push flows from the current namespace to Git"
    )
    @PluginProperty(group = "advanced")
    @Builder.Default
    private FlowFiles flows = FlowFiles.builder().build();

    @PluginProperty(group = "source")
    private Object inputFiles;

    @Schema(
        title = "File patterns to stage",
        description = "Defaults to `.` (all files). Directories are added recursively; globs are not supported."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<List<String>> addFilesPattern = Property.ofValue(List.of("."));

    @Schema(title = "Commit author")
    @PluginProperty(group = "connection")
    private Author author;

    private boolean branchExists(RunContext runContext, String branch) throws Exception {
        if (this.url == null) {
            try (Git git = Git.open(runContext.workingDir().resolve(Path.of(runContext.render(this.directory).as(String.class).orElse(null))).toFile())) {
                return git.branchList().setListMode(ListBranchCommand.ListMode.REMOTE).call().stream()
                    .anyMatch(ref -> ref.getName().equals(R_HEADS + branch));
            }
        }

        return authentified(Git.lsRemoteRepository().setRemote(runContext.render(url).as(String.class).orElse(null)), runContext)
            .callAsMap()
            .containsKey(R_HEADS + branch);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        Path basePath = runContext.workingDir().path();
        if (this.directory != null) {
            basePath = runContext.workingDir().resolve(Path.of(runContext.render(this.directory).as(String.class).orElseThrow()));
        }

        String branch = runContext.render(this.branch).as(String.class).orElse(null);

        configureHttpTransport(runContext);

        // we add this method to configure ssl to allow self-signed certs
        configureEnvironmentWithSsl(runContext);

        if (this.url != null) {
            boolean branchExists = branchExists(runContext, branch);

            Clone cloneHead = Clone.builder()
                .depth(Property.ofValue(1))
                .url(this.url)
                .directory(this.directory)
                .username(this.username)
                .password(this.password)
                .privateKey(this.privateKey)
                .passphrase(this.passphrase)
                .cloneSubmodules(this.cloneSubmodules)
                .build();

            if (branchExists) {
                cloneHead.toBuilder()
                    .branch(Property.ofValue(branch))
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
            List<String> previouslyTrackedRelativeFilePaths = Optional.ofNullable(basePath.toFile().listFiles()).stream().flatMap(Arrays::stream)
                .filter(file -> !file.isDirectory() || !file.getName().equals(".git"))
                .map(File::toPath)
                .map(basePath::relativize)
                .map(Path::toString)
                .toList();

            if (!previouslyTrackedRelativeFilePaths.isEmpty()) {
                previouslyTrackedRelativeFilePaths.forEach(rm::addFilepattern);
                rm.call();
            }
        }

        if (this.inputFiles != null) {
            FilesService.inputFiles(runContext, this.inputFiles);
        }

        if (this.namespaceFiles != null && Boolean.TRUE.equals(runContext.render(this.namespaceFiles.getEnabled()).as(Boolean.class).orElse(true))) {
            runContext.storage()
                .namespace()
                .findAllFilesMatching(
                    runContext.render(this.namespaceFiles.getInclude()).asList(String.class),
                    runContext.render(this.namespaceFiles.getExclude()).asList(String.class)
                )
                .forEach(Rethrow.throwConsumer(namespaceFile ->
                {
                    InputStream content = runContext.storage().getFile(namespaceFile.uri());
                    runContext.workingDir().putFile(Path.of(namespaceFile.path()), content);
                }));
        }

        if (Boolean.TRUE.equals(runContext.render(this.flows.enabled).as(Boolean.class).orElse(true))) {

            Map<String, String> flowProps = Optional.ofNullable((Map<String, String>) runContext.getVariables().get("flow")).orElse(Collections.emptyMap());
            String tenantId = flowProps.get("tenantId");
            String namespace = flowProps.get("namespace");

            boolean includeChildren = Boolean.TRUE.equals(runContext.render(this.flows.childNamespaces).as(Boolean.class).orElse(true));
            List<FlowWithSource> flows = fetchFlowsFromKestra(kestraClient(runContext), runContext, tenantId, namespace, includeChildren);

            Path flowsDirectory = this.flows.gitDirectory == null
                ? basePath
                : basePath.resolve(runContext.render(this.flows.gitDirectory).as(String.class).orElse(null));

            // Create flow directory if it doesn't exist
            flowsDirectory.toFile().mkdirs();

            flows.forEach(
                throwConsumer(
                    flowWithSource -> FileUtils.writeStringToFile(
                        flowsDirectory.resolve(flowWithSource.getNamespace() + "." + flowWithSource.getId() + ".yml").toFile(),
                        flowWithSource.getSource().replaceAll("(?m)^revision:\\s*\\d+\\n?", ""),
                        StandardCharsets.UTF_8
                    )
                )
            );
        }

        logger.info(
            "Pushing to {}/tree/{}",
            git.getRepository().getConfig().getString("remote", "origin", "url"),
            git.getRepository().getBranch()
        );

        AddCommand add = git.add();
        runContext.render(this.addFilesPattern).asList(String.class).forEach(add::addFilepattern);
        add.call();

        ObjectId commitId = null;
        try {
            PersonIdent author = author(runContext);
            CommitCommand commitCommand = git.commit()
                .setAllowEmpty(false)
                .setMessage(runContext.render(this.commitMessage).as(String.class).orElse(null))
                .setAuthor(author);
            if (author != null) {
                commitCommand.setCommitter(author);
            }
            commitId = commitCommand.call().getId();
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

    private List<FlowWithSource> fetchFlowsFromKestra(KestraClient kestraClient, RunContext runContext, String tenantId, String namespace, boolean includeChildren) {
        try {
            var op = includeChildren ? QueryFilterOp.STARTS_WITH : QueryFilterOp.EQUALS;
            byte[] zippedFlows = kestraClient.flows().exportFlowsByQuery(
                tenantId,
                List.of(new QueryFilter().field(QueryFilterField.NAMESPACE).operation(op).value(namespace))
            );

            List<FlowWithSource> flows = new ArrayList<>();
            try (
                var bais = new ByteArrayInputStream(zippedFlows);
                var zis = new ZipInputStream(bais)
            ) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.getName().endsWith(".yml") && !entry.getName().endsWith(".yaml")) {
                        continue;
                    }
                    var yaml = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
                    try {
                        var parsed = YamlParser.parse(yaml, io.kestra.core.models.flows.Flow.class);
                        flows.add(FlowWithSource.of(parsed, yaml));
                    } catch (Exception e) {
                        runContext.logger().warn("Skipping invalid flow from entry {}: {}", entry.getName(), e.getMessage());
                    }
                }
            }
            return flows;
        } catch (IOException e) {
            throw new KestraRuntimeException("Failed to export flows from Kestra for namespace " + namespace, e);
        } catch (ApiException e) {
            throw new KestraRuntimeException("Failed to export flows from Kestra for namespace " + namespace, e);
        }
    }

    private PersonIdent author(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.author == null) {
            return null;
        }
        if (this.author.email != null && this.author.name != null) {
            return new PersonIdent(
                runContext.render(this.author.name).as(String.class).orElseThrow(),
                runContext.render(this.author.email).as(String.class).orElseThrow()
            );
        }
        if (this.author.email != null && this.username != null) {
            return new PersonIdent(
                runContext.render(this.username).as(String.class).orElseThrow(),
                runContext.render(this.author.email).as(String.class).orElseThrow()
            );
        }

        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "ID of the commit pushed"
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
            title = "Push flows from the current namespace"
        )
        @Builder.Default
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private Property<Boolean> enabled = Property.ofValue(true);

        @Schema(
            title = "Include child namespaces"
        )
        @Builder.Default
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private Property<Boolean> childNamespaces = Property.ofValue(true);

        @Schema(
            title = "Flow destination directory",
            description = "Relative to `directory`; defaults to `_flows`."
        )
        @Builder.Default
        private Property<String> gitDirectory = Property.ofValue("_flows");
    }

    @Builder
    @Getter
    public static class Author {
        @Schema(title = "Commit author name (defaults to username)")
        private Property<String> name;

        @Schema(title = "Commit author email (optional)")
        private Property<String> email;
    }
}
