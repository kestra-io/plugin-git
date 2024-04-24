package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.tasks.*;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.EmptyCommitException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;
import static org.eclipse.jgit.lib.Constants.R_HEADS;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Commit and push your saved flows to a Git repository.",
    description = """
        Using this task, you can push one or more flows from a given namespace (and optionally also child namespaces) to Git.
        Check the examples below to see how you can push all flows or only specific ones.
        You can also learn about Git integration in the Version Control with [Git documentation](https://kestra.io/docs/developer-guide/git)."""
)
@Plugin(
    examples = {
        @Example(
            title = "Automatically push all saved flows from the dev namespace and all child namespaces to a Git repository every day at 5 p.m. Before pushing to Git, the task will adjust the flow's source code to match the targetNamespace to prepare the Git branch for merging to the production namespace.",
            full = true,
            code = {
                """
                    id: push_to_git
                    namespace: system
                    \s
                    tasks:
                      - id: commit_and_push
                        type: io.kestra.plugin.git.PushFlows
                        sourceNamespace: dev # the namespace from which flows are pushed
                        targetNamespace: prod # the target production namespace; if different than sourceNamespace, the sourceNamespace in the source code will be overwritten by the targetNamespace
                        flows:
                          - "*"  # optional list of Regex strings; by default, all flows are pushed
                        includeChildNamespaces: true # optional boolean, false by default
                        gitDirectory: _flows
                        url: https://github.com/kestra-io/scripts # required string
                        username: git_username # required string needed for Auth with Git
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}" # optional, required for private repositories
                        branch: kestra # optional, uses "kestra" by default
                        commitMessage: "add flows {{ now() }}" # optional string
                        dryRun: true  # if true, you'll see what files will be added, modified or deleted based on the Git version without overwriting the files yet
                    \s
                    triggers:
                      - id: schedule_push
                        type: io.kestra.core.models.triggers.types.Schedule
                        cron: "0 17 * * *" # release/push to Git every day at 5pm"""
            }
        ),
        @Example(
            title = "Manually push a single flow to Git if the input push is set to true.",
            full = true,
            code = {
                """
                    id: myflow
                    namespace: prod
                    \s
                    inputs:
                      - id: push
                        type: BOOLEAN
                        defaults: false
                    \s
                    tasks:
                      - id: if
                        type: io.kestra.core.tasks.flows.If
                        condition: "{{ inputs.push == true}}"
                        then:
                          - id: commit_and_push
                            type: io.kestra.plugin.git.PushFlows
                            sourceNamespace: prod # optional; if you prefer templating, you can use "{{ flow.namespace }}"
                            targetNamespace: prod # optional; by default, set to the same namespace as defined in sourceNamespace
                            flows: myflow # if you prefer templating, you can use "{{ flow.id }}"
                            url: https://github.com/kestra-io/scripts
                            username: git_username
                            password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                            branch: kestra
                            commitMessage: "add flow {{ flow.namespace ~ '.' ~ flow.id }}"\s"""
            }
        )
    }
)
public class PushFlows extends AbstractGitTask implements RunnableTask<PushFlows.Output> {
    private static final Pattern SSH_URL_PATTERN = Pattern.compile("git@(?:ssh\\.)?([^:]+):(?:v\\d*/)?(.*)");

    @Schema(
        title = "The branch to which files should be committed and pushed.",
        description = "If the branch doesn't exist yet, it will be created."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String branch = "kestra";

    @Schema(
        title = "Directory to which flows should be pushed.",
        description = """
            If not set, flows will be pushed to a Git directory named _flows and will optionally also include subdirectories named after the child namespaces.
            If you prefer, you can specify an arbitrary path, e.g., kestra/flows, allowing you to push flows to that specific Git directory.
            If the `includeChildNamespaces` property is set to true, this task will also push all flows from child namespaces into their corresponding nested directories, e.g., flows from the child namespace called prod.marketing will be added to the marketing folder within the _flows folder.
            Note that the targetNamespace (here prod) is specified in the flow code; therefore, kestra will not create the prod directory within _flows. You can use the PushFlows task to push flows from the sourceNamespace, and use SyncFlows to then sync PR-approved flows to the targetNamespace, including all child namespaces."""
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String gitDirectory = "_flows";

    @Schema(
        title = "The source namespace from which flows should be synced to the `gitDirectory`."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String sourceNamespace = "{{ flow.namespace }}";

    @Schema(
        title = "The target namespace, intended as the production namespace.",
        description = "If set, the `sourceNamespace` will be overwritten to the `targetNamespace` in the flow source code to prepare your branch for merging into the production namespace."
    )
    @PluginProperty(dynamic = true)
    private String targetNamespace;

    @Schema(
        title = "A list of Regex strings that declare which flows should be included in the Git commit.",
        description = """
            By default, all flows from the specified sourceNamespace will be pushed (and optionally adjusted to match the targetNamespace before pushing to Git).
            If you want to push only the current flow, you can use the "{{flow.id}}" expression or specify the flow ID explicitly, e.g. myflow.
            Given that this is a list of Regex strings, you can include as many flows as you wish, provided that the user is authorized to access that namespace."""
    )
    @PluginProperty(dynamic = true)
    private List<String> flows;

    @Schema(
        title = "Whether you want to push flows from child namespaces as well.",
        description = """
            By default, it’s false, so the task will push only flows from the explicitly declared namespace without pushing flows from child namespaces. If set to true, flows from child namespaces will be pushed to child directories in Git. See the example below for a practical explanation:
            
            | Source namespace in the flow code |       Git directory path       |  Synced to target namespace   |
            | --------------------------------- | ------------------------------ | ----------------------------- |
            | namespace: dev                    | _flows/flow1.yml               | namespace: prod               |
            | namespace: dev                    | _flows/flow2.yml               | namespace: prod               |
            | namespace: dev.marketing          | _flows/marketing/flow3.yml     | namespace: prod.marketing     |
            | namespace: dev.marketing          | _flows/marketing/flow4.yml     | namespace: prod.marketing     |
            | namespace: dev.marketing.crm      | _flows/marketing/crm/flow5.yml | namespace: prod.marketing.crm |
            | namespace: dev.marketing.crm      | _flows/marketing/crm/flow6.yml | namespace: prod.marketing.crm |"""
    )
    @PluginProperty
    @Builder.Default
    private boolean includeChildNamespaces = false;

    @Schema(
        title = "If true, the task will only output modifications without pushing any flows to Git yet. If false (default), all listed flows will be pushed to Git immediately."
    )
    @PluginProperty
    @Builder.Default
    private boolean dryRun = false;

    @Schema(
        title = "Git commit message.",
        defaultValue = "Add flows from `sourceNamespace` namespace"
    )
    @PluginProperty(dynamic = true)
    private String commitMessage;

    @Schema(
        title = "The commit author email.",
        description = "If null, no author will be set on this commit."
    )
    @PluginProperty(dynamic = true)
    private String authorEmail;

    @Schema(
        title = "The commit author name.",
        description = "If null, the username will be used instead.",
        defaultValue = "`username`"
    )
    @PluginProperty(dynamic = true)
    private String authorName;

    private boolean branchExists(RunContext runContext, String branch) throws Exception {
        return authentified(Git.lsRemoteRepository().setRemote(runContext.render(url)), runContext)
            .callAsMap()
            .containsKey(R_HEADS + branch);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        Path basePath = runContext.tempDir();

        String renderedBranch = runContext.render(this.branch);
        boolean branchExists = branchExists(runContext, renderedBranch);

        Clone cloneHead = Clone.builder()
            .depth(1)
            .url(this.url)
            .username(this.username)
            .password(this.password)
            .privateKey(this.privateKey)
            .passphrase(this.passphrase)
            .build();

        if (branchExists) {
            cloneHead.toBuilder()
                .branch(renderedBranch)
                .build()
                .run(runContext);
        } else {
            logger.info("Branch {} does not exist, creating it", renderedBranch);

            cloneHead.run(runContext);
        }


        Git git = Git.open(basePath.toFile());
        if (!branchExists) {
            git.checkout()
                .setName(renderedBranch)
                .setCreateBranch(true)
                .call();
        }

        String renderedGitDirectory = runContext.render(this.gitDirectory);
        Path flowDirectory = runContext.resolve(Path.of(renderedGitDirectory));
        flowDirectory.toFile().mkdirs();

        FlowRepositoryInterface flowRepository = runContext.getApplicationContext().getBean(FlowRepositoryInterface.class);

        Map<String, String> flowProps = Optional.ofNullable((Map<String, String>) runContext.getVariables().get("flow")).orElse(Collections.emptyMap());
        String tenantId = flowProps.get("tenantId");
        List<FlowWithSource> flowsToPush;
        String renderedSourceNamespace = runContext.render(this.sourceNamespace);
        if (Boolean.TRUE.equals(this.includeChildNamespaces)) {
            flowsToPush = flowRepository.findWithSource(null, tenantId, renderedSourceNamespace, null);
        } else {
            flowsToPush = flowRepository.findByNamespaceWithSource(tenantId, renderedSourceNamespace);
        }

        Stream<FlowWithSource> filteredFlowsToPush = flowsToPush.stream();
        List<String> renderedFlowsRegexes = Optional.ofNullable(this.flows)
            .map(throwFunction(runContext::render))
            .orElse(null);
        if (renderedFlowsRegexes != null) {
            filteredFlowsToPush = filteredFlowsToPush.filter(flowWithSource -> {
                String flowId = flowWithSource.getId();
                return renderedFlowsRegexes.stream().anyMatch(flowId::matches);
            });
        }

        Map<Path, String> flowSourceByPath = filteredFlowsToPush.collect(Collectors.toMap(flowWithSource -> {
            Path path = flowDirectory;
            if (flowWithSource.getNamespace().length() > renderedSourceNamespace.length()) {
                path = path.resolve(flowWithSource.getNamespace().substring(renderedSourceNamespace.length() + 1).replace(".", "/"));
            }

            return path.resolve(flowWithSource.getId() + ".yml");
        }, FlowWithSource::getSource));

        // We remove any flow file from remote that is no longer present in the Flow repository
        RmCommand rm = git.rm();
        try(Stream<Path> paths = Files.walk(flowDirectory)) {
            Stream<Path> filteredPaths = paths.filter(path -> !flowSourceByPath.containsKey(path) && !path.getFileName().toString().equals(".git"));

            filteredPaths
                .map(path -> basePath.relativize(path).toString())
                .forEach(rm::addFilepattern);
        }
        rm.call();

        String renderedTargetNamespace = runContext.render(this.targetNamespace);
        flowSourceByPath.forEach(throwBiConsumer((path, source) -> {
            String overwrittenSource = source;
            if (renderedTargetNamespace != null) {
                overwrittenSource = source.replaceAll("(?m)^(\\s*namespace:\\s*)" + renderedSourceNamespace, "$1" + renderedTargetNamespace);
            }
            FileUtils.writeStringToFile(
                path.toFile(),
                overwrittenSource,
                StandardCharsets.UTF_8
            );
        }));

        AddCommand add = git.add();
        add.addFilepattern(renderedGitDirectory);
        add.call();

        DiffFormatter diffFormatter = new DiffFormatter(null);
        diffFormatter.setRepository(git.getRepository());
        File diffFile = runContext.tempFile(".ion").toFile();
        try(BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            git.diff().setCached(true).call().stream()
                .map(throwFunction(diffEntry -> {
                    EditList editList = diffFormatter.toFileHeader(diffEntry).toEditList();
                    int additions = 0;
                    int deletions = 0;
                    int changes = 0;
                    for (Edit edit : editList) {
                        int modifications = edit.getLengthB() - edit.getLengthA();
                        if (modifications > 0) {
                            additions += modifications;
                        } else if (modifications < 0) {
                            deletions += -modifications;
                        } else {
                            changes += edit.getLengthB();
                        }
                    }

                    String file = diffEntry.getNewPath();
                    if (diffEntry.getChangeType() == DiffEntry.ChangeType.DELETE) {
                        file = diffEntry.getOldPath();
                    }
                    return Map.of(
                        "file", file,
                        "additions", "+" + additions,
                        "deletions", "-" + deletions,
                        "changes", Integer.toString(changes)
                    );
                }))
                .map(throwFunction(map -> JacksonMapper.ofIon().writeValueAsString(map)))
                .forEach(throwConsumer(ionDiff -> {
                    diffWriter.write(ionDiff);
                    diffWriter.write("\n");
                }));

            diffWriter.flush();
        }

        URI diffFileStorageUri = runContext.storage().putFile(diffFile);

        String commitURL = null;
        String commitId = null;
        ObjectId commit;
        try {
            String httpUrl = getHttpUrl(runContext);
            if (this.dryRun) {
                logger.info(
                    "Dry run — no changes will be pushed to {} for now until you set the `dryRun` parameter to false",
                    httpUrl
                );
            } else {
                logger.info(
                    "Pushing to {} on branch {}",
                    httpUrl,
                    renderedBranch
                );

                String message = Optional.ofNullable(this.commitMessage)
                    .map(throwFunction(runContext::render))
                    .orElse("Add flows from " + renderedSourceNamespace + " namespace");
                commit = git.commit()
                    .setAllowEmpty(false)
                    .setMessage(message)
                    .setAuthor(author(runContext))
                    .call()
                    .getId();
                authentified(git.push(), runContext).call();

                commitId = commit.getName();
                commitURL = buildCommitUrl(httpUrl, renderedBranch, commitId);

                logger.info("Pushed to " + commitURL);
            }
        } catch (EmptyCommitException e) {
            logger.info("No changes to commit. Skipping push.");
        }

        git.close();

        return Output.builder()
            .commitId(commitId)
            .commitURL(commitURL)
            .flows(diffFileStorageUri)
            .build();
    }

    private PersonIdent author(RunContext runContext) throws IllegalVariableEvaluationException {
        String name = Optional.ofNullable(this.authorName).orElse(this.username);
        if (this.authorEmail == null || name == null) {
            return null;
        }

        return new PersonIdent(runContext.render(name), runContext.render(this.authorEmail));
    }

    private String buildCommitUrl(String httpUrl, String branch, String commitId) throws IllegalVariableEvaluationException {
        if (commitId == null) {
            return null;
        }

        String commitSubroute = httpUrl.contains("bitbucket.org") ? "commits" : "commit";
        String commitUrl = httpUrl + "/" + commitSubroute + "/" + commitId;
        if (commitUrl.contains("azure.com")) {
            commitUrl = commitUrl + "?refName=refs%2Fheads%2F" + branch;
        }

        return commitUrl;
    }

    private String getHttpUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        String httpUrl = runContext.render(this.url);
        // SSH URL
        Matcher sshUrlMatcher = SSH_URL_PATTERN.matcher(httpUrl);
        if (sshUrlMatcher.matches()) {
            httpUrl = sshUrlMatcher.group(1) + "/" + sshUrlMatcher.group(2);

            if (httpUrl.contains("azure.com")) {
                int orgFromProjectSeparatorIndex = httpUrl.lastIndexOf("/");
                httpUrl = httpUrl.substring(0, orgFromProjectSeparatorIndex) + "/_git/" + httpUrl.substring(orgFromProjectSeparatorIndex + 1);
            }

            httpUrl = "https://" + httpUrl;
        } else if (httpUrl.contains("@")) {
            httpUrl = httpUrl.replaceFirst("//.*@", "//");
        }

        return httpUrl;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "ID of the commit pushed."
        )
        @Nullable
        private final String commitId;

        @Schema(
            title = "URL to see what’s included in the commit.",
            description = "Example format for GitHub: https://github.com/username/your_repo/commit/{commitId}."
        )
        @Nullable
        private final String commitURL;

        private final URI flows;
    }
}
