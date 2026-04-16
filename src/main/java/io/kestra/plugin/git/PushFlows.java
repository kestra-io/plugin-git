package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.core.serializers.YamlParser;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.QueryFilter;
import io.kestra.sdk.model.QueryFilterField;
import io.kestra.sdk.model.QueryFilterOp;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push flows to Git",
    description = "Exports saved flows from `sourceNamespace` (optionally child namespaces) into `gitDirectory` (default `_flows`) and pushes to Git. Can rewrite namespaces to `targetNamespace`; branch is created if missing and dry-run writes only the diff."
)
@Plugin(
    examples = {
        @Example(
            title = "Automatically push all saved flows from the dev namespace and all child namespaces to a Git repository every day at 5 p.m. Before pushing to Git, the task will adjust the flow's source code to match the targetNamespace to prepare the Git branch for merging to the production namespace. Note that the automatic conversion of `sourceNamespace` to `targetNamespace` is optional and should only be considered as a helper for facilitating the Git workflow for simple use cases — only the `namespace` property within the flow will be adjusted and if you specify namespace names within e.g. Flow triggers, those may need to be manually adjusted. **We recommend using separate Kestra instances for development and production with the same namespace names across instances.**",
            full = true,
            code = """
                id: push_to_git
                namespace: system

                tasks:
                  - id: commit_and_push
                    type: io.kestra.plugin.git.PushFlows
                    sourceNamespace: dev
                    targetNamespace: prod
                    flows: "*"
                    includeChildNamespaces: true
                    gitDirectory: _flows
                    url: https://github.com/kestra-io/scripts
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: main
                    commitMessage: "add flows {{ now() }}"
                    dryRun: true

                triggers:
                  - id: schedule_push
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 17 * * *"
                """
        ),
        @Example(
            title = "Release all flows and scripts from selected namespaces to a Git repository every Thursday at 11:00 AM. Adjust the `values` list to include the namespaces for which you want to push your code to Git. This [System Flow](https://kestra.io/docs/concepts/system-flows) will create two commits per namespace: one for the flows and one for the scripts.",
            full = true,
            code = """
                id: git_push
                namespace: system

                tasks:
                  - id: push
                    type: io.kestra.plugin.core.flow.ForEach
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.PushFlows
                        sourceNamespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'flows/' ~ taskrun.value}}"
                        includeChildNamespaces: false

                      - id: scripts
                        type: io.kestra.plugin.git.PushNamespaceFiles
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
                  - id: schedule_push_to_git
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 11 * * 4"
                """
        )
    }
)
public class PushFlows extends AbstractPushTask<PushFlows.Output> {
    @Schema(
        title = "Branch to push flows",
        description = "Defaults to `main`; created if absent."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Flow destination directory",
        description = "Relative path inside the repo; defaults to `_flows`. Child namespaces are nested under this path when `includeChildNamespaces` is true."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> gitDirectory = Property.ofValue("_flows");

    @Schema(
        title = "Source namespace",
        description = "Namespace to export flows from; defaults to the current flow namespace."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<String> sourceNamespace = Property.ofExpression("{{ flow.namespace }}");

    @Schema(
        title = "Target namespace override",
        description = "If set, rewrites the `namespace` field in exported flows to this value."
    )
    @PluginProperty(group = "source")
    private Property<String> targetNamespace;

    @Schema(
        title = "Flows to include",
        description = "Glob pattern(s) against flow IDs; defaults to all (`**`).",
        oneOf = { String.class, String[].class },
        defaultValue = "**"
    )
    @PluginProperty(dynamic = true, group = "advanced")
    private Object flows;

    @Schema(
        title = "Include child namespaces",
        description = "When true, exports flows from child namespaces into nested directories under `gitDirectory`."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<Boolean> includeChildNamespaces = Property.ofValue(false);

    @Schema(
        title = "Git commit message",
        defaultValue = "Add flows from `sourceNamespace`"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(Property.ofValue("Add flows from " + this.sourceNamespace.toString() + " namespace"));
    }

    @Override
    public Object globs() {
        return this.flows;
    }

    @Override
    public Property<String> fetchedNamespace() {
        return this.sourceNamespace;
    }

    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path flowDirectory, List<String> globs) throws IllegalVariableEvaluationException {
        Map<String, String> flowProps = Optional.ofNullable((Map<String, String>) runContext.getVariables().get("flow")).orElse(Collections.emptyMap());
        String tenantId = flowProps.get("tenantId");
        String renderedSourceNamespace = runContext.render(this.sourceNamespace).as(String.class).orElse(null);
        boolean includeChildren = Boolean.TRUE.equals(runContext.render(this.includeChildNamespaces).as(Boolean.class).orElseThrow());

        List<FlowWithSource> flowsToPush = fetchFlowsFromKestra(kestraClient(runContext), runContext, tenantId, renderedSourceNamespace, includeChildren);

        Stream<FlowWithSource> filteredFlowsToPush = flowsToPush.stream();
        if (globs != null) {
            List<PathMatcher> matchers = globs.stream().map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob)).toList();
            filteredFlowsToPush = filteredFlowsToPush.filter(flowWithSource ->
            {
                String flowId = flowWithSource.getId();
                return matchers.stream().anyMatch(matcher -> matcher.matches(Path.of(flowId)));
            });
        }

        return filteredFlowsToPush.collect(Collectors.toMap(flowWithSource ->
        {
            Path path = flowDirectory;
            if (flowWithSource.getNamespace().length() > renderedSourceNamespace.length()) {
                path = path.resolve(flowWithSource.getNamespace().substring(renderedSourceNamespace.length() + 1).replace(".", "/"));
            }

            return path.resolve(flowWithSource.getId() + ".yml");
        }, throwFunction(flowWithSource -> (throwSupplier(() ->
        {
            String renderedTargetNamespace = runContext.render(targetNamespace).as(String.class).orElse(renderedSourceNamespace);
            String modifiedSource = flowWithSource.getSource()
                .replaceAll(
                    "(?m)^(\\s*namespace:\\s*)" + renderedSourceNamespace,
                    "$1" + renderedTargetNamespace
                );
            return new ByteArrayInputStream(modifiedSource.getBytes());
        })))));
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

    private KestraClient kestraClient(RunContext runContext) throws IllegalVariableEvaluationException {
        String rKestraUrl;
        try {
            rKestraUrl = runContext.render("{{ kestra.url }}");
        } catch (IllegalVariableEvaluationException e) {
            rKestraUrl = "http://localhost:8080";
        }
        if (rKestraUrl == null || rKestraUrl.isBlank()) {
            rKestraUrl = "http://localhost:8080";
        }
        String normalizedUrl = rKestraUrl.trim().replaceAll("/+$", "");
        var builder = KestraClient.builder().url(normalizedUrl);
        Optional<SDK.Auth> autoAuth = runContext.sdk().defaultAuthentication();
        if (autoAuth.isPresent() && autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
            return builder.basicAuth(autoAuth.get().username().get(), autoAuth.get().password().get()).build();
        }
        return builder.build();
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
            title = "A file containing all changes pushed (or not in case of dry run) to Git",
            description = """
                The output format is a ION file with one row per file, each row containing the number of added, deleted, and changed lines.
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
