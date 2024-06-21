package io.kestra.plugin.git;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;

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
                    namespace: company.team
                    \s
                    tasks:
                      - id: commit_and_push
                        type: io.kestra.plugin.git.PushFlows
                        sourceNamespace: dev # the namespace from which flows are pushed
                        targetNamespace: prod # the target production namespace; if different than sourceNamespace, the sourceNamespace in the source code will be overwritten by the targetNamespace
                        flows: "*"  # optional list of glob patterns; by default, all flows are pushed
                        includeChildNamespaces: true # optional boolean, false by default
                        gitDirectory: _flows
                        url: https://github.com/kestra-io/scripts # required string
                        username: git_username # required string needed for Auth with Git
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: kestra # optional, uses "kestra" by default
                        commitMessage: "add flows {{ now() }}" # optional string
                        dryRun: true  # if true, you'll see what files will be added, modified or deleted based on the state in Git without overwriting the files yet
                    \s
                    triggers:
                      - id: schedule_push
                        type: io.kestra.plugin.core.trigger.Schedule
                        cron: "0 17 * * *" # release/push to Git every day at 5pm"""
            }
        ),
        @Example(
            title = "Manually push a single flow to Git if the input push is set to true.",
            full = true,
            code = {
                """
                    id: myflow
                    namespace: company.team
                    \s
                    inputs:
                      - id: push
                        type: BOOLEAN
                        defaults: false
                    \s
                    tasks:
                      - id: if
                        type: io.kestra.plugin.core.flow.If
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
public class PushFlows extends AbstractPushTask<PushFlows.Output> {
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
        title = "List of glob patterns or a single one that declare which flows should be included in the Git commit.",
        description = """
            By default, all flows from the specified sourceNamespace will be pushed (and optionally adjusted to match the targetNamespace before pushing to Git).
            If you want to push only the current flow, you can use the "{{flow.id}}" expression or specify the flow ID explicitly, e.g. myflow.
            Given that this is a list of glob patterns, you can include as many flows as you wish, provided that the user is authorized to access that namespace.
            Note that each glob pattern try to match the file name OR the relative path starting from `gitDirectory`""",
        oneOf = {String.class, String[].class},
        defaultValue = "**"
    )
    @PluginProperty(dynamic = true)
    private Object flows;

    @Schema(
        title = "Whether you want to push flows from child namespaces as well.",
        description = """
            By default, it’s `false`, so the task will push only flows from the explicitly declared namespace without pushing flows from child namespaces. If set to `true`, flows from child namespaces will be pushed to child directories in Git. See the example below for a practical explanation:
            
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
        title = "Git commit message.",
        defaultValue = "Add flows from `sourceNamespace` namespace"
    )
    @Override
    public String getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse("Add flows from " + this.sourceNamespace + " namespace");
    }

    @Override
    public Object globs() {
        return this.flows;
    }

    @Override
    public String fetchedNamespace() {
        return this.sourceNamespace;
    }

    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path flowDirectory, List<String> globs) throws IllegalVariableEvaluationException {
        FlowRepositoryInterface flowRepository = ((DefaultRunContext)runContext).getApplicationContext().getBean(FlowRepositoryInterface.class);

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
        if (globs != null) {
            List<PathMatcher> matchers = globs.stream().map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob)).toList();
            filteredFlowsToPush = filteredFlowsToPush.filter(flowWithSource -> {
                String flowId = flowWithSource.getId();
                return matchers.stream().anyMatch(matcher -> matcher.matches(Path.of(flowId)));
            });
        }

        Map<Path, Supplier<InputStream>> flowSourceByPath = filteredFlowsToPush.collect(Collectors.toMap(flowWithSource -> {
            Path path = flowDirectory;
            if (flowWithSource.getNamespace().length() > renderedSourceNamespace.length()) {
                path = path.resolve(flowWithSource.getNamespace().substring(renderedSourceNamespace.length() + 1).replace(".", "/"));
            }

            return path.resolve(flowWithSource.getId() + ".yml");
        }, throwFunction(flowWithSource -> (throwSupplier(() -> {
            String modifiedSource = flowWithSource.getSource().replaceAll("(?m)^(\\s*namespace:\\s*)" + runContext.render(sourceNamespace), "$1" + runContext.render(targetNamespace));
            return new ByteArrayInputStream(modifiedSource.getBytes());
        })))));
        return flowSourceByPath;
    }

    @Override
    protected Output output(AbstractPushTask.Output pushOutput, URI diffFileStorageUri) {
        return Output.builder()
            .commitId(pushOutput.getCommitId())
            .commitURL(pushOutput.getCommitURL())
            .flows(diffFileStorageUri)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractPushTask.Output {
        @Schema(
            title = "A file containing all changes pushed (or not in case of dry run) to Git.",
            description = """
                The output format is a ION file with one row per files, each row containing the number of added, deleted and changed lines.
                A row looks as follows: `{changes:"3",file:"_flows/first-flow.yml",deletions:"-5",additions:"+10"}`"""
        )
        private URI flows;

        @Override
        @VisibleForTesting
        public URI diffFileUri() {
            return this.flows;
        }
    }
}
