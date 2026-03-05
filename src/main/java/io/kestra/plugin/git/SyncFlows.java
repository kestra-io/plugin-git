package io.kestra.plugin.git;

import io.kestra.core.exceptions.FlowProcessingException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowSource;
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
    title = "Sync flows from Git",
    description = "Imports flows from a Git branch into `targetNamespace`, optionally traversing child namespaces. Can rewrite namespaces to `targetNamespace`, delete missing flows when `delete` is true, and emit a diff on dry-run."
)
@Plugin(
    examples = {
        @Example(
            title = "Sync flows from a Git repository. This flow can run either on a schedule (using the [Schedule](https://kestra.io/docs/workflow-components/triggers/schedule-trigger) trigger) or anytime you push a change to a given Git branch (using the [Webhook](https://kestra.io/docs/workflow-components/triggers/webhook-trigger) trigger).",
            full = true,
            code = """
                id: sync_flows_from_git
                namespace: system

                tasks:
                  - id: git
                    type: io.kestra.plugin.git.SyncFlows
                    gitDirectory: flows
                    targetNamespace: git
                    includeChildNamespaces: true
                    delete: true
                    url: https://github.com/kestra-io/flows
                    branch: main
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    dryRun: true

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        ),
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
        )
    }
)
public class SyncFlows extends AbstractSyncTask<Flow, SyncFlows.Output> {
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    @Schema(
        title = "Branch to sync",
        description = "Defaults to `main`."
    )
    @Builder.Default
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Target namespace",
        description = "Flows are rewritten to this namespace (and nested namespaces when applicable)."
    )
    @NotNull
    private Property<String> targetNamespace;

    @Schema(
        title = "Git directory for flows",
        description = "Relative path containing flow YAML; defaults to `_flows`. Subdirectories map to child namespaces when `includeChildNamespaces` is true."
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.ofValue("_flows");

    @Schema(
        title = "Include child namespaces",
        description = "Default false. When true, subdirectories under `gitDirectory` are synced to corresponding child namespaces."
    )
    @Builder.Default
    private Property<Boolean> includeChildNamespaces = Property.ofValue(false);

    @Schema(
        title = "Delete flows missing in Git",
        description = "Default false to avoid destructive syncs. When true (and especially with `includeChildNamespaces`), removes flows not present in Git."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.ofValue(false);

    @Schema(
        title = "Ignore invalid flows",
        description = "If true, skips flows that fail validation instead of failing the task."
    )
    @Builder.Default
    private Property<Boolean> ignoreInvalidFlows = Property.ofValue(false);

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
    protected Flow simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, FlowProcessingException, IllegalVariableEvaluationException {
        if (inputStream == null) {
            return null;
        }

        String flowSource = SyncFlows.replaceNamespace(renderedNamespace, uri, inputStream);

        var flowValidated = flowService.validate(runContext.flowInfo().tenantId(), List.of(new FlowSource(null, flowSource))).getFirst();

        if (flowValidated.getConstraints() != null) {
            var ref = uri.getPath();

            if (ref.startsWith("/")) {
                ref = ref.substring(1);
            }

            if (runContext.render(this.ignoreInvalidFlows).as(Boolean.class).orElse(false)) {
                runContext.logger().warn("Invalid flow imported from Git ({}): {}", ref, flowValidated.getConstraints());
                return null;
            }

            throw new FlowProcessingException("Invalid flow imported from Git (" + ref + "): " + flowValidated.getConstraints());
        }

        return flowService(runContext).importFlow(runContext.flowInfo().tenantId(), flowSource, true);
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
    protected Flow writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, FlowProcessingException, IllegalVariableEvaluationException {
        if (inputStream == null) {
            return null;
        }

        String flowSource = SyncFlows.replaceNamespace(renderedNamespace, uri, inputStream);

        var flowValidated = flowService.validate(runContext.flowInfo().tenantId(), List.of(new FlowSource(null, flowSource))).getFirst();

        if (flowValidated.getConstraints() != null) {
            var ref = uri.getPath();

            if (ref.startsWith("/")) {
                ref = ref.substring(1);
            }

            if (runContext.render(this.ignoreInvalidFlows).as(Boolean.class).orElse(false)) {
                runContext.logger().warn("Invalid flow imported from Git ({}): {}", ref, flowValidated.getConstraints());
                return null;
            }

            throw new FlowProcessingException("Invalid flow imported from Git (" + ref + "): " + flowValidated.getConstraints());
        }

        return flowService(runContext).importFlow(runContext.flowInfo().tenantId(), flowSource, false);
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

        if (flowBeforeUpdate == null && flowAfterUpdate == null) {
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
            title = "Diff of synced flows",
            description = "ION file listing per-flow sync actions (added, deleted, overwritten, updated)."
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
