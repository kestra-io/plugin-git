package io.kestra.plugin.git;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.services.GitService;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.FlowWithSource;

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
    title = "Sync a single flow from Git",
    description = "Imports one flow YAML from a Git branch into a target namespace. Flow namespace is rewritten to `targetNamespace`. Supports dry-run to validate without saving."
)
@Plugin(
    examples = {
        @Example(
            title = "Sync a single flow from a Git repository to a specific namespace. This can be used to deploy or update individual flows.",
            full = true,
            code = """
                id: sync_single_flow_from_git
                namespace: system.cicd

                tasks:
                  - id: sync_my_flow
                    type: io.kestra.plugin.git.SyncFlow
                    url: https://github.com/my-org/kestra-flows
                    branch: main
                    username: my_git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    targetNamespace: dev.marketing # required
                    flowPath: "flows/marketing/flow.yml" # required
                    kestraUrl: "http://localhost:8080"
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                """
        )
    }
)
public class SyncFlow extends AbstractKestraTask implements RunnableTask<SyncFlow.Output> {

    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^\\s*namespace:\\s*(.*)$");
    private static final Pattern FLOW_ID_FINDER_PATTERN = Pattern.compile("(?m)^id:\\s*(.*)$");

    @Schema(
        title = "Branch to clone",
        description = "Defaults to `main`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> branch = Property.ofValue("main");

    @Override
    public Property<String> getBranch() {
        return this.branch;
    }

    @Schema(
        title = "Target namespace",
        description = "Replaces any namespace declared in the flow file."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> targetNamespace;

    @Schema(
        title = "Flow file path",
        description = "Relative path to the flow YAML inside the repository."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> flowPath;

    @Schema(
        title = "Dry run only",
        description = "When true, validates and logs without importing."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> dryRun = Property.ofValue(Boolean.FALSE);

    @Override
    public Output run(RunContext runContext) throws Exception {

        configureHttpTransport(runContext);

        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        GitService gitService = new GitService(this);

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()).as(String.class).orElse(null), Property.ofValue(Boolean.FALSE));
        Path cloneDir = git.getRepository().getWorkTree().toPath();

        String renderedFlowPath = runContext.render(this.flowPath).as(String.class).orElseThrow();
        Path flowFilePath = cloneDir.resolve(renderedFlowPath);

        if (!Files.exists(flowFilePath)) {
            throw new java.io.FileNotFoundException("Flow file not found at path: " + renderedFlowPath);
        }

        // Build the client only after confirming the file exists, to keep failures fast and clear
        KestraClient kestraClient = kestraClient(runContext);

        String renderedNamespace = runContext.render(this.targetNamespace).as(String.class).orElseThrow();
        String flowSource;
        try (InputStream is = Files.newInputStream(flowFilePath)) {
            flowSource = IOUtils.toString(is, StandardCharsets.UTF_8);
        }

        // Rewrite the namespace to the target namespace
        Matcher namespaceMatcher = NAMESPACE_FINDER_PATTERN.matcher(flowSource);
        flowSource = namespaceMatcher.replaceFirst("namespace: " + renderedNamespace);

        // Parse the flow id from the (potentially rewritten) YAML
        Matcher idMatcher = FLOW_ID_FINDER_PATTERN.matcher(flowSource);
        if (!idMatcher.find()) {
            throw new IllegalStateException("Cannot parse flow id from YAML at path: " + renderedFlowPath);
        }
        String flowId = idMatcher.group(1).trim();

        String tenantId = runContext.flowInfo().tenantId();
        boolean rDryRun = runContext.render(this.dryRun).as(Boolean.class).orElse(Boolean.FALSE);

        FlowWithSource result;

        if (rDryRun) {
            // Validate without importing; compute projected revision
            var violations = kestraClient.flows().validateFlows(tenantId, flowSource);
            if (!violations.isEmpty() && violations.getFirst().getConstraints() != null) {
                throw new IllegalStateException("Flow validation failed: " + violations.getFirst().getConstraints());
            }

            // Projected revision: existing + 1, or 1 for a new flow
            int projectedRevision;
            try {
                FlowWithSource existing = kestraClient.flows().flow(renderedNamespace, flowId, false, false, tenantId, null);
                projectedRevision = existing.getRevision() != null ? existing.getRevision() + 1 : 1;
            } catch (ApiException e) {
                // Flow does not exist yet
                projectedRevision = 1;
            }

            result = new FlowWithSource()
                .id(flowId)
                .namespace(renderedNamespace)
                .revision(projectedRevision);
        } else {
            // Import the flow
            var tempFile = toNamedTempFile(flowId + ".yaml", flowSource.stripTrailing());
            kestraClient.flows().importFlows(true, tenantId, tempFile);

            // Fetch the saved flow to populate the output
            result = kestraClient.flows().flow(renderedNamespace, flowId, false, false, tenantId, null);
        }

        git.close();

        return Output.builder()
            .flowId(result.getId())
            .namespace(result.getNamespace())
            .revision(result.getRevision())
            .build();
    }

    private java.io.File toNamedTempFile(String fileName, String yaml) {
        try {
            Path tmpPath = Files.createTempDirectory("kestra-import")
                .resolve(fileName.endsWith(".yaml") ? fileName : fileName + ".yaml");

            Files.createDirectories(tmpPath.getParent());
            Files.writeString(tmpPath, yaml, StandardCharsets.UTF_8);

            return tmpPath.toFile();
        } catch (java.io.IOException e) {
            throw new io.kestra.core.exceptions.KestraRuntimeException("Failed to create named file for: " + fileName, e);
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The ID of the synced flow")
        private final String flowId;
        @Schema(title = "The namespace of the synced flow")
        private final String namespace;
        @Schema(title = "The new revision number of the flow")
        private final Integer revision;
    }
}
