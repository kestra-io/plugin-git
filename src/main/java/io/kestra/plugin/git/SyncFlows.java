package io.kestra.plugin.git;

import io.kestra.core.exceptions.FlowProcessingException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithException;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.services.FlowService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Sync flows from Git to Kestra.",
    description = """
        This task syncs flows from a given Git branch to a Kestra `namespace`. If the `delete` property is set to true, any flow available in kestra but not present in the `gitDirectory` will be deleted, considering Git as a single source of truth for your flows. Check the [Version Control with Git](https://kestra.io/docs/version-control-cicd/git) documentation for more details."""
)
@Plugin(
    examples = {
        @Example(
            title = "Sync all flows and scripts for selected namespaces from Git to Kestra every full hour. Note that this is a [System Flow](https://kestra.io/docs/concepts/system-flows), so make sure to adjust the Scope to SYSTEM in the UI filter to see this flow or its executions.",
            full = true,
            code = """
                id: git_sync
                namespace: system

                tasks:
                  - id: sync
                    type: io.kestra.plugin.core.flow.ForEach
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.SyncFlows
                        targetNamespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'flows/' ~ taskrun.value}}"
                        includeChildNamespaces: false

                      - id: scripts
                        type: io.kestra.plugin.git.SyncNamespaceFiles
                        namespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'scripts/' ~ taskrun.value}}"

                pluginDefaults:
                  - type: io.kestra.plugin.git
                    values:
                      username: anna-geller
                      url: https://github.com/anna-geller/product
                      password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                      branch: main
                      dryRun: false

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        ),
        @Example(
            title = "Sync flows from a Git repository. This flow can run either on a schedule (using the [Schedule](https://kestra.io/docs/workflow-components/triggers/schedule-trigger) trigger) or anytime you push a change to a given Git branch (using the [Webhook](https://kestra.io/docs/workflow-components/triggers/webhook-trigger) trigger).",
            full = true,
            code = """
                id: sync_flows_from_git
                namespace: system

                tasks:
                  - id: git
                    type: io.kestra.plugin.git.SyncFlows
                    gitDirectory: flows # optional; set to _flows by default
                    targetNamespace: git # required
                    includeChildNamespaces: true # optional; by default, it's set to false to allow explicit definition
                    delete: true # optional; by default, it's set to false to avoid destructive behavior
                    url: https://github.com/kestra-io/flows # required
                    branch: main
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    dryRun: true  # if true, the task will only log which flows from Git will be added/modified or deleted in kestra without making any changes in kestra backend yet

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        )
    }
)
public class SyncFlows extends AbstractSyncTask<Flow, SyncFlows.Output> {
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    @Schema(
        title = "The branch from which flows will be synced to Kestra."
    )
    @Builder.Default
    private Property<String> branch = Property.of("main");

    @Schema(
        title = "The target namespace to which flows from the `gitDirectory` should be synced.",
        description = """
            If the top-level namespace specified in the flow source code is different than the `targetNamespace`, it will be overwritten by this target namespace. This facilitates moving between environments and projects. If `includeChildNamespaces` property is set to true, the top-level namespace in the source code will also be overwritten by the `targetNamespace` in children namespaces.

            For example, if the `targetNamespace` is set to `prod` and `includeChildNamespaces` property is set to `true`, then:
            - `namespace: dev` in flow source code will be overwritten by `namespace: prod`,
            - `namespace: dev.marketing.crm` will be overwritten by `namespace: prod.marketing.crm`.

            See the table below for a practical explanation:

            | Source namespace in the flow code |       Git directory path       |  Synced to target namespace   |
            | --------------------------------- | ------------------------------ | ----------------------------- |
            | namespace: dev                    | _flows/flow1.yml               | namespace: prod               |
            | namespace: dev                    | _flows/flow2.yml               | namespace: prod               |
            | namespace: dev.marketing          | _flows/marketing/flow3.yml     | namespace: prod.marketing     |
            | namespace: dev.marketing          | _flows/marketing/flow4.yml     | namespace: prod.marketing     |
            | namespace: dev.marketing.crm      | _flows/marketing/crm/flow5.yml | namespace: prod.marketing.crm |
            | namespace: dev.marketing.crm      | _flows/marketing/crm/flow6.yml | namespace: prod.marketing.crm |
            """
    )
    @NotNull
    private Property<String> targetNamespace;

    @Schema(
        title = "Directory from which flows should be synced.",
        description = """
            If not set, this task assumes your branch has a Git directory named `_flows` (equivalent to the default `gitDirectory` of the [PushFlows](https://kestra.io/docs/how-to-guides/pushflows) task).

            If `includeChildNamespaces` property is set to `true`, this task will push all flows from nested subdirectories into their corresponding child namespaces, e.g. if `targetNamespace` is set to `prod`, then:

            - flows from the `_flows` directory will be synced to the `prod` namespace,
            - flows from the `_flows/marketing` subdirectory in Git will be synced to the `prod.marketing` namespace,
            - flows from the `_flows/marketing/crm` subdirectory will be synced to the `prod.marketing.crm` namespace."""
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.of("_flows");

    @Schema(
        title = "Whether you want to sync flows from child namespaces as well.",
        description = "It’s `false` by default so that we sync only flows from the explicitly declared `gitDirectory` without traversing child directories. If set to `true`, flows from subdirectories in Git will be synced to child namespace in Kestra using the dot notation `.` for each subdirectory in the folder structure."
    )
    @Builder.Default
    private Property<Boolean> includeChildNamespaces = Property.of(false);

    @Schema(
        title = "Whether you want to delete flows present in kestra but not present in Git.",
        description = "It’s `false` by default to avoid destructive behavior. Use this property with caution because when set to `true` and `includeChildNamespaces` is also set to `true`, this task will delete all flows from the `targetNamespace` and all its child namespaces that are not present in Git rather than only overwriting the changes."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.of(false);

    @Schema(
        title = "Ignore flows when they have validation failure",
        description = "Due to breaking changes, some flows may not be valid anymore by the time of the synchronisation. To avoid synchronizing flows that are no longer valid, set this property to true."
    )
    @Builder.Default
    private Property<Boolean> ignoreInvalidFlows = Property.of(false);

    @Getter(AccessLevel.NONE)
    private FlowService flowService;


    private FlowService flowService(RunContext runContext) {
        if (flowService == null) {
            flowService = ((DefaultRunContext) runContext).getApplicationContext().getBean(FlowService.class);
        }
        return flowService;
    }


    @Override
    public Property<String> fetchedNamespace() {
        return this.targetNamespace;
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, Flow flow) {
        flowService(runContext).delete(FlowWithSource.of(flow, ""));
    }

    @Override
    protected Flow simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, FlowProcessingException {
        if (inputStream == null) {
            return null;
        }

        return flowService(runContext).importFlow(runContext.flowInfo().tenantId(), SyncFlows.replaceNamespace(renderedNamespace, uri, inputStream), true);
    }

    @Override
    protected boolean mustKeep(RunContext runContext, Flow instanceResource) {
        RunContext.FlowInfo flowInfo = runContext.flowInfo();
        return flowInfo.id().equals(instanceResource.getId()) &&
            flowInfo.namespace().equals(instanceResource.getNamespace()) &&
            Objects.equals(flowInfo.tenantId(), instanceResource.getTenantId());
    }

    @Override
    protected Property<Boolean> traverseDirectories() {
        return this.includeChildNamespaces;
    }

    @Override
    protected Flow writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, FlowProcessingException {
        if (inputStream == null) {
            return null;
        }

        String flowSource = SyncFlows.replaceNamespace(renderedNamespace, uri, inputStream);

        return flowService(runContext).importFlow(runContext.flowInfo().tenantId(), flowSource);
    }

    private static String replaceNamespace(String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        String flowSource = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        String uriStr = uri.toString();
        String newNamespace = renderedNamespace + uriStr.substring(0, uriStr.lastIndexOf("/")).replace("/", ".");
        Matcher matcher = NAMESPACE_FINDER_PATTERN.matcher(flowSource);
        flowSource = matcher.replaceFirst("namespace: " + newNamespace);
        return flowSource.stripTrailing();
    }

    @Override
    protected SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, Flow flowBeforeUpdate, Flow flowAfterUpdate) {
        if (resourceUri != null && resourceUri.toString().endsWith("/")) {
            return null;
        }

        SyncState syncState;
        if (resourceUri == null) {
            syncState = SyncState.DELETED;
        } else if (flowBeforeUpdate == null) {
            syncState = SyncState.ADDED;
        } else if (flowBeforeUpdate.getRevision().equals(Objects.requireNonNull(flowAfterUpdate).getRevision())){
            syncState = SyncState.UNCHANGED;
        } else {
            syncState = SyncState.UPDATED;
        }

        Flow infoHolder = flowAfterUpdate == null ? flowBeforeUpdate : flowAfterUpdate;
        SyncResult.SyncResultBuilder<?, ?> builder = SyncResult.builder()
            .syncState(syncState)
            .namespace(infoHolder.getNamespace())
            .flowId(infoHolder.getId())
            .revision(infoHolder.getRevision());

        if (syncState != SyncState.DELETED) {
            builder.gitPath(renderedGitDirectory + resourceUri);
        }

        return builder.build();
    }

    @Override
    protected List<Flow> fetchResources(RunContext runContext, String renderedNamespace) throws IllegalVariableEvaluationException {
        List<Flow> flows;
        if (runContext.render(this.includeChildNamespaces).as(Boolean.class).orElseThrow()) {
            flows=  flowService(runContext).findByNamespacePrefix(runContext.flowInfo().tenantId(), renderedNamespace);
        } else {
            flows = flowService(runContext).findByNamespace(runContext.flowInfo().tenantId(), renderedNamespace);
        }
        if (runContext.render(this.ignoreInvalidFlows).as(Boolean.class).orElse(false)) {
            flows= flows.stream()
                .filter(flow -> {
                    if (flow instanceof FlowWithException flowWithException) {
                        runContext.logger().warn("Flow {} is not valid: {}", flowWithException.getId(), flowWithException.getException());
                        return false;
                    }
                    return true;
                })
                .toList();
        }
        return flows;
    }

    @Override
    protected URI toUri(String renderedNamespace, Flow resource) {
        if (resource == null) {
            return null;
        }

        String gitSimulatedNamespaceUri = resource.getNamespace().equals(renderedNamespace) ? "" : "/" + resource.getNamespace().substring(renderedNamespace.length() + 1);
        String uriWithoutExtension = gitSimulatedNamespaceUri.replace(".", "/") + "/" + resource.getId();
        return URI.create(uriWithoutExtension + ".yml");
    }

    @Override
    protected Output output(URI diffFileStorageUri) {
        return Output.builder()
            .flows(diffFileStorageUri)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractSyncTask.Output {
        @Schema(
            title = "A file containing all changes applied (or not in case of dry run) from Git.",
            description = """
                The output format is a ION file with one row per synced flow, each row containing the information whether the flow would be added, deleted or overwritten in Kestra by the state of what's in Git.

                A row looks as follows: `{gitPath:"flows/flow1.yml",syncState:"ADDED",flowId:"flow1",namespace:"prod",revision:1}`"""
        )
        private URI flows;

        @Override
        public URI diffFileUri() {
            return this.flows;
        }
    }


    @SuperBuilder
    @Getter
    public static class SyncResult extends AbstractSyncTask.SyncResult {
        private String flowId;
        private String namespace;
        private Integer revision;
    }
}
