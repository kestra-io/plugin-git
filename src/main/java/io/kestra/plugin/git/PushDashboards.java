package io.kestra.plugin.git;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.DashboardRepositoryInterface;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Commit and push your saved dashboards to a Git repository."
)
@Plugin(
    examples = {
        @Example(
            title = "Manually push a single dashboard to Git if the input push is set to true.",
            full = true,
            code = """
                id: push_dashboards
                namespace: prod

                inputs:
                  - id: push
                    type: BOOLEAN
                    defaults: false

                tasks:
                  - id: if
                    type: io.kestra.plugin.core.flow.If
                    condition: "{{ inputs.push == true}}"
                    then:
                      - id: commit_and_push
                        type: io.kestra.plugin.git.PushDashboards
                        dashboards: mydashboard # if you prefer templating, you can use "{{ flow.id }}"
                        url: https://github.com/kestra-io/scripts
                        username: git_username
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: main
                        commitMessage: "add mydashboard from {{ flow.namespace ~ '.' ~ flow.id }}"
                """
        )
    }
)
public class PushDashboards extends AbstractPushTask<PushDashboards.Output> {
    @Schema(title = "The branch to which dashboards should be committed and pushed")
    @Builder.Default
    private Property<String> branch = Property.of("main");

    @Schema(title = "Directory to which dashboards should be pushed")
    @Builder.Default
    private Property<String> gitDirectory = Property.of("_dashboards");

    @Schema(
        title = "List of glob patterns or a single one that declares which dashboards should be included in the Git commit",
        oneOf = {String.class, String[].class},
        defaultValue = "**"
    )
    @PluginProperty(dynamic = true)
    private Object dashboards;

    @Schema(
        title = "Git commit message",
        defaultValue = "Add dashboards from flow: {{ flow.id }}"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(new Property<>("Add dashboards from flow: {{ flow.id }}"));
    }

    @Override
    public Object globs() {
        return this.dashboards;
    }

    @Override
    public Property<String> fetchedNamespace() {
        // Dashboards are not linked to namespaces
        return Property.of("");
    }

    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path flowDirectory, List<String> globs) throws IllegalVariableEvaluationException {
        DashboardRepositoryInterface dashboardRepository = ((DefaultRunContext)runContext).getApplicationContext().getBean(DashboardRepositoryInterface.class);

        Map<String, String> flowProps = Optional.ofNullable((Map<String, String>) runContext.getVariables().get("flow")).orElse(Collections.emptyMap());
        String tenantId = flowProps.get("tenantId");

        List<Dashboard> dashboardsToPush = dashboardRepository.findAll(tenantId);

        Stream<Dashboard> dashboardStream = dashboardsToPush.stream();
        if (globs != null) {
            List<PathMatcher> matchers = globs.stream().map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob)).toList();
            dashboardStream = dashboardStream.filter(dashboard -> {
                String dashboardId = dashboard.getId();
                return matchers.stream().anyMatch(matcher -> matcher.matches(Path.of(dashboardId)));
            });
        }

        return dashboardStream.collect(Collectors.toMap(dashboard -> {
                Path path = flowDirectory;
                return path.resolve(dashboard.getId() + ".yml");
            },
            throwFunction(dashboard -> (throwSupplier(() -> new ByteArrayInputStream(dashboard.getSourceCode().getBytes()))))
        ));
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
