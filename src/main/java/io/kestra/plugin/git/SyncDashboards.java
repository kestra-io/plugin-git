package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.DashboardRepositoryInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.YamlParser;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Sync dashboards from Git to Kestra."
)
@Plugin(
    examples = {
        @Example(
            title = "",
            full = true,
            code = ""
        )
    }
)
public class SyncDashboards extends AbstractSyncTask<Dashboard, SyncDashboards.Output> {
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    @Schema(
        title = "The branch from which dashboards will be synced to Kestra."
    )
    @Builder.Default
    private Property<String> branch = Property.of("main");

    @Schema(
        title = "Directory from which dashboards should be synced."
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.of("_dashboards");

    @Schema(
        title = "Whether you want to delete dashboards present in kestra but not present in Git."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.of(false);

    private DashboardRepositoryInterface repository(RunContext runContext) {
        return ((DefaultRunContext) runContext).getApplicationContext().getBean(DashboardRepositoryInterface.class);
    }

    private YamlParser yamlFlowParser(RunContext runContext) {
        return ((DefaultRunContext) runContext).getApplicationContext().getBean(YamlParser.class);
    }


    @Override
    public Property<String> fetchedNamespace() {
        return null;
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, Dashboard dashboard) {
        repository(runContext).delete(runContext.flowInfo().tenantId(), dashboard.getId());
    }

    @Override
    protected Dashboard simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        if (inputStream == null) {
            return null;
        }

        String dashboardSource = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        Dashboard dashboardWithTenant = yamlFlowParser(runContext).parse(dashboardSource, Dashboard.class).toBuilder()
            .tenantId(runContext.flowInfo().tenantId())
            .sourceCode(dashboardSource)
            .build();
        DashboardRepositoryInterface dashboardRepositoryInterface = repository(runContext);

        Optional<Dashboard> prevDashboard = dashboardRepositoryInterface.get(dashboardWithTenant.getTenantId(), dashboardWithTenant.getId());
        return prevDashboard.map(previous -> {
            if (previous.equals(dashboardWithTenant) && !previous.isDeleted()) {
                return previous;
            }
            return dashboardWithTenant.toBuilder().id(previous.getId()).created(previous.getCreated()).updated(Instant.now()).build();
        }).orElseGet(() -> dashboardWithTenant.toBuilder().created(Instant.now()).updated(Instant.now()).build());
    }

    @Override
    protected Dashboard writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        if (inputStream == null) {
            return null;
        }

        String dashboardSource = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        Dashboard dashboardWithTenant = yamlFlowParser(runContext).parse(dashboardSource, Dashboard.class).toBuilder()
            .tenantId(runContext.flowInfo().tenantId())
            .sourceCode(dashboardSource)
            .build();
        DashboardRepositoryInterface dashboardRepositoryInterface = repository(runContext);

        Optional<Dashboard> prevDashboard = dashboardRepositoryInterface.get(dashboardWithTenant.getTenantId(), dashboardWithTenant.getId());
        return dashboardRepositoryInterface.save(prevDashboard.orElse(null), dashboardWithTenant, dashboardSource);
    }

    @Override
    protected SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, Dashboard dashboardBeforeUpdate, Dashboard dashboardAfterUpdate) {
        if (resourceUri != null && resourceUri.toString().endsWith("/")) {
            return null;
        }

        SyncState syncState;
        if (resourceUri == null) {
            syncState = SyncState.DELETED;
        } else if (dashboardBeforeUpdate == null) {
            syncState = SyncState.ADDED;
        } else if (dashboardBeforeUpdate.getUpdated().equals(Objects.requireNonNull(dashboardAfterUpdate).getUpdated())){
            syncState = SyncState.UNCHANGED;
        } else {
            syncState = SyncState.UPDATED;
        }

        Dashboard infoHolder = dashboardAfterUpdate == null ? dashboardBeforeUpdate : dashboardAfterUpdate;
        SyncResult.SyncResultBuilder<?, ?> builder = SyncResult.builder()
            .syncState(syncState)
            .dashboardId(infoHolder.getId())
            .updated(infoHolder.getUpdated());

        if (syncState != SyncState.DELETED) {
            builder.gitPath(renderedGitDirectory + resourceUri);
        }

        return builder.build();
    }

    @Override
    protected List<Dashboard> fetchResources(RunContext runContext, String renderedNamespace) throws IllegalVariableEvaluationException {
        return repository(runContext).findAll(runContext.flowInfo().tenantId());
    }

    @Override
    protected URI toUri(String renderedNamespace, Dashboard resource) {
        if (resource == null) {
            return null;
        }
        return URI.create("/" + resource.getId());
    }

    @Override
    protected Output output(URI diffFileStorageUri) {
        return Output.builder()
            .dashboards(diffFileStorageUri)
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
        private URI dashboards;

        @Override
        public URI diffFileUri() {
            return this.dashboards;
        }
    }


    @SuperBuilder
    @Getter
    public static class SyncResult extends AbstractSyncTask.SyncResult {
        private String dashboardId;
        private Instant updated;
    }
}
