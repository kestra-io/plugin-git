package io.kestra.plugin.git;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.core.serializers.YamlParser;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.PagedResultsDashboard;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Sync dashboards from Git",
    description = "Imports dashboards from a Git branch. Can delete dashboards missing in Git, respects `.kestraignore`, and supports dry-run diff output."
)
@Plugin(
    examples = {
        @Example(
            title = "Sync all dashboards from Git to Kestra every full hour.",
            full = true,
            code = """
                id: git_sync
                namespace: company

                tasks:
                  - id: sync_dashboards
                    type: io.kestra.plugin.git.SyncDashboards
                    gitDirectory: _dashboards
                    branch: dashboards

                pluginDefaults:
                  - type: io.kestra.plugin.git
                    values:
                      username: kestra-git-user
                      url: https://github.com/my-company/my-repo
                      password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                      branch: main
                      dryRun: false

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        ),
    }
)
public class SyncDashboards extends AbstractSyncTask<Dashboard, SyncDashboards.Output> {
    public static final Pattern NAMESPACE_FINDER_PATTERN = Pattern.compile("(?m)^namespace: (.*)$");

    @Schema(
        title = "Branch to sync",
        description = "Defaults to `main`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Git directory for dashboards",
        description = "Relative path containing dashboard YAML; defaults to `_dashboards`."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> gitDirectory = Property.ofValue("_dashboards");

    @Schema(
        title = "Delete dashboards missing in Git",
        description = "Default false to avoid destructive syncs."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> delete = Property.ofValue(false);

    @Override
    public Property<String> fetchedNamespace() {
        // Dashboards are not linked to namespaces
        return Property.ofValue("");
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, Dashboard dashboard) {
        try {
            kestraClient(runContext).dashboards().deleteDashboard(dashboard.getId(), runContext.flowInfo().tenantId());
        } catch (Exception e) {
            throw new KestraRuntimeException("Failed to delete dashboard " + dashboard.getId(), e);
        }
    }

    @Override
    protected Dashboard simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        return fetchDashboard(runContext, inputStream, true);
    }

    @Override
    protected Dashboard writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        return fetchDashboard(runContext, inputStream, false);
    }

    protected Dashboard fetchDashboard(RunContext runContext, InputStream inputStream, boolean dryRun) throws IOException {
        if (inputStream == null) {
            return null;
        }
        String dashboardSource = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        var tenantId = runContext.flowInfo().tenantId();
        Dashboard parsedDashboard = YamlParser.parse(dashboardSource, Dashboard.class).toBuilder()
            .tenantId(tenantId)
            .sourceCode(dashboardSource)
            .build();

        // Look up the existing dashboard (if any) via the SDK search
        Optional<Dashboard> prevDashboard = findExistingDashboard(runContext, tenantId, parsedDashboard.getId());

        if (dryRun) {
            return prevDashboard.map(previous ->
            {
                // Compare source to detect changes
                String prevSource = previous.getSourceCode() != null ? previous.getSourceCode() : "";
                if (prevSource.replace("\r\n", "\n").strip().equals(dashboardSource.replace("\r\n", "\n").strip())) {
                    return previous;
                }
                return parsedDashboard.toBuilder().id(previous.getId()).created(previous.getCreated()).updated(Instant.now()).build();
            }).orElseGet(() -> parsedDashboard.toBuilder().created(Instant.now()).updated(Instant.now()).build());
        } else {
            // Skip createDashboard if content is unchanged to preserve the updated timestamp
            // (UNCHANGED detection in wrapper() compares before/after updated timestamps)
            boolean contentChanged = prevDashboard.map(previous -> {
                String prevSource = previous.getSourceCode() != null ? previous.getSourceCode() : "";
                return !prevSource.replace("\r\n", "\n").strip().equals(dashboardSource.replace("\r\n", "\n").strip());
            }).orElse(true);

            if (!contentChanged) {
                return prevDashboard.get();
            }

            try {
                kestraClient(runContext).dashboards().createDashboard(tenantId, dashboardSource);
            } catch (Exception e) {
                throw new KestraRuntimeException("Failed to save dashboard " + parsedDashboard.getId(), e);
            }
            // Re-fetch to get the server-assigned updated timestamp
            return findExistingDashboard(runContext, tenantId, parsedDashboard.getId())
                .orElseGet(() -> parsedDashboard.toBuilder().updated(Instant.now()).build());
        }
    }

    private Optional<Dashboard> findExistingDashboard(RunContext runContext, String tenantId, String dashboardId) {
        try {
            KestraClient kestraClient = kestraClient(runContext);
            int page = 1;
            int size = 200;
            PagedResultsDashboard pagedResults;
            do {
                pagedResults = kestraClient.dashboards().searchDashboards(page, size, tenantId, null, null);
                for (var sdkDash : pagedResults.getResults()) {
                    if (dashboardId.equals(sdkDash.getId())) {
                        // Fetch YAML source via raw HTTP call
                        String sourceCode = fetchDashboardSourceCode(kestraClient, runContext, tenantId, dashboardId);
                        Dashboard parsed = YamlParser.parse(sourceCode, Dashboard.class);  // preserves getUpdated() from updated: injection
                        return Optional.of(parsed.toBuilder()
                            .tenantId(tenantId)
                            .sourceCode(sourceCode.replaceAll("(?m)^updated:.*\\R?", ""))
                            .build());
                    }
                }
                page++;
            } while (pagedResults.getResults().size() == size);
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    protected SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, Dashboard dashboardBeforeUpdate,
        Dashboard dashboardAfterUpdate) {
        if (resourceUri != null && resourceUri.toString().endsWith("/")) {
            return null;
        }

        SyncState syncState;
        if (resourceUri == null) {
            syncState = SyncState.DELETED;
        } else if (dashboardBeforeUpdate == null) {
            syncState = SyncState.ADDED;
        } else if (Objects.equals(dashboardBeforeUpdate.getUpdated(), Objects.requireNonNull(dashboardAfterUpdate).getUpdated())) {
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
    protected List<Dashboard> fetchResources(RunContext runContext, String renderedNamespace) {
        try {
            KestraClient kestraClient = kestraClient(runContext);
            var tenantId = runContext.flowInfo().tenantId();
            List<Dashboard> dashboards = new ArrayList<>();
            int page = 1;
            int size = 200;
            PagedResultsDashboard pagedResults;
            do {
                pagedResults = kestraClient.dashboards().searchDashboards(page, size, tenantId, null, null);
                for (var sdkDash : pagedResults.getResults()) {
                    try {
                        String sourceCode = fetchDashboardSourceCode(kestraClient, runContext, tenantId, sdkDash.getId());
                        Dashboard parsed = YamlParser.parse(sourceCode, Dashboard.class);
                        dashboards.add(parsed.toBuilder()
                            .tenantId(tenantId)
                            .sourceCode(sourceCode.replaceAll("(?m)^updated:.*\\R?", ""))
                            .build());
                    } catch (Exception e) {
                        runContext.logger().warn("Skipping dashboard {} — failed to fetch source: {}", sdkDash.getId(), e.getMessage());
                    }
                }
                page++;
            } while (pagedResults.getResults().size() == size);
            return dashboards;
        } catch (Exception e) {
            throw new KestraRuntimeException("Failed to fetch dashboards from Kestra", e);
        }
    }

    private String fetchDashboardSourceCode(KestraClient kestraClient, RunContext runContext, String tenantId, String dashboardId) throws Exception {
        String basePath = kestraClient.dashboards().getApiClient().getBasePath();
        String encodedId = URLEncoder.encode(dashboardId, StandardCharsets.UTF_8);
        String path = tenantId == null
            ? "/api/v1/dashboards/" + encodedId
            : "/api/v1/" + tenantId + "/dashboards/" + encodedId;

        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(URI.create(basePath + path))
            .addHeader("Accept", "application/x-yaml");
        Optional<SDK.Auth> autoAuth = runContext.sdk().defaultAuthentication();
        if (autoAuth.isPresent()) {
            if (autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                String encoded = Base64.getEncoder().encodeToString(
                    (autoAuth.get().username().get() + ":" + autoAuth.get().password().get()).getBytes(StandardCharsets.UTF_8)
                );
                requestBuilder.addHeader("Authorization", "Basic " + encoded);
            } else if (autoAuth.get().apiToken().isPresent()) {
                requestBuilder.addHeader("Authorization", "Bearer " + autoAuth.get().apiToken().get());
            }
        }

        try (var httpClient = HttpClient.builder()
                .runContext(runContext)
                .configuration(HttpConfiguration.builder().build())
                .build()) {
            var response = httpClient.request(requestBuilder.build(), String.class);
            return response.getBody();
        }
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
            title = "Diff of synced dashboards",
            description = "ION file listing per-dashboard sync actions (added, deleted, updated, unchanged)."
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
