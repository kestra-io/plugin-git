package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.services.FlowService;
import io.kestra.plugin.git.services.GitService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.eclipse.jgit.api.Git;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Sync a single flow from a Git repository to a Kestra namespace."
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
                """
        )
    }
)
public class SyncFlow extends AbstractGitTask implements RunnableTask<SyncFlow.Output> {

    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^\\s*namespace:\\s*(.*)$");

    @Schema(title = "The branch to clone from")
    @Builder.Default
    private Property<String> branch = Property.ofValue("main");

    @Override
    public Property<String> getBranch() {
        return this.branch;
    }

    @Schema(title = "The target namespace where the flow will be synced")
    @NotNull
    private Property<String> targetNamespace;

    @Schema(title = "The full path to the flow YAML file within the Git repository")
    @NotNull
    private Property<String> flowPath;

    @Schema(title = "If true, the task will only log the action without actually syncing the flow.")
    @Builder.Default
    private Property<Boolean> dryRun = Property.ofValue(Boolean.FALSE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        
        configureHttpTransport();
        
        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        GitService gitService = new GitService(this);
        FlowService flowService = ((DefaultRunContext) runContext).getApplicationContext().getBean(FlowService.class);

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()).as(String.class).orElse(null), Property.ofValue(Boolean.FALSE));
        Path cloneDir = git.getRepository().getWorkTree().toPath();

        String renderedFlowPath = runContext.render(this.flowPath).as(String.class).orElseThrow();
        Path flowFilePath = cloneDir.resolve(renderedFlowPath);

        if (!Files.exists(flowFilePath)) {
            throw new java.io.FileNotFoundException("Flow file not found at path: " + renderedFlowPath);
        }

        String renderedNamespace = runContext.render(this.targetNamespace).as(String.class).orElseThrow();
        String flowSource;
        try (InputStream is = Files.newInputStream(flowFilePath)) {
            flowSource = IOUtils.toString(is, StandardCharsets.UTF_8);
        }

        Matcher matcher = NAMESPACE_FINDER_PATTERN.matcher(flowSource);
        flowSource = matcher.replaceFirst("namespace: " + renderedNamespace);

        Flow flow = flowService.importFlow(runContext.flowInfo().tenantId(), flowSource.stripTrailing(), runContext.render(this.dryRun).as(Boolean.class).orElse(Boolean.FALSE));

        git.close();

        return Output.builder()
            .flowId(flow.getId())
            .namespace(flow.getNamespace())
            .revision(flow.getRevision())
            .build();
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