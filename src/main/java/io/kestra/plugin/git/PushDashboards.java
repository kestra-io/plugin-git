package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.SDK;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.PagedResultsDashboard;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;


@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push dashboards to Git",
    description = "Commits saved dashboards to Git under `gitDirectory` (default `_dashboards`), filtered by glob patterns. Creates the branch if missing; supports dry-run diff generation."
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
    @Schema(
        title = "Branch to push dashboards",
        description = "Defaults to `main`; created if it does not exist."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Dashboard destination directory",
        description = "Relative path inside the repository; defaults to `_dashboards`."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> gitDirectory = Property.ofValue("_dashboards");

    @Schema(
        title = "Dashboards to include",
        description = "Glob pattern(s) matching dashboard IDs; defaults to all (`**`).",
        oneOf = { String.class, String[].class },
        defaultValue = "**"
    )
    @PluginProperty(dynamic = true, group = "advanced")
    private Object dashboards;

    @Schema(
        title = "Git commit message",
        defaultValue = "Add dashboards from flow: {{ flow.id }}"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(Property.ofExpression("Add dashboards from flow: {{ flow.id }}"));
    }

    @Override
    public Object globs() {
        return this.dashboards;
    }

    @Override
    public Property<String> fetchedNamespace() {
        // Dashboards are not linked to namespaces
        return Property.ofValue("");
    }

    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path flowDirectory, List<String> globs) throws IllegalVariableEvaluationException {
        Map<String, String> flowProps = Optional.ofNullable((Map<String, String>) runContext.getVariables().get("flow")).orElse(Collections.emptyMap());
        String tenantId = flowProps.get("tenantId");
        KestraClient kestraClient = kestraClient(runContext);

        List<io.kestra.sdk.model.Dashboard> allDashboards = new ArrayList<>();
        int page = 1;
        int size = 200;
        PagedResultsDashboard pagedResults;
        do {
            try {
                pagedResults = kestraClient.dashboards().searchDashboards(page, size, tenantId, null, null);
            } catch (Exception e) {
                throw new KestraRuntimeException("Failed to fetch dashboards from Kestra", e);
            }
            allDashboards.addAll(pagedResults.getResults());
            page++;
        } while (pagedResults.getResults().size() == size);

        Stream<io.kestra.sdk.model.Dashboard> dashboardStream = allDashboards.stream();
        if (globs != null) {
            List<PathMatcher> matchers = globs.stream().map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob)).toList();
            dashboardStream = dashboardStream.filter(dashboard ->
            {
                String dashboardId = dashboard.getId();
                return matchers.stream().anyMatch(matcher -> matcher.matches(Path.of(dashboardId)));
            });
        }

        return dashboardStream.collect(Collectors.toMap(
            dashboard -> flowDirectory.resolve(dashboard.getId() + ".yml"),
            dashboard -> () ->
            {
                try {
                    String sourceCode = fetchDashboardSourceCode(kestraClient, runContext, tenantId, dashboard.getId());
                    String cleanSourceCode = sourceCode.replaceAll("(?m)^updated:.*\\R?", "");
                    return new ByteArrayInputStream(cleanSourceCode.getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new KestraRuntimeException("Failed to fetch dashboard source for " + dashboard.getId(), e);
                }
            }
        ));
    }

    private String fetchDashboardSourceCode(KestraClient kestraClient, RunContext runContext, String tenantId, String dashboardId) throws Exception {
        String basePath = kestraClient.dashboards().getApiClient().getBasePath();
        String encodedId = URLEncoder.encode(dashboardId, StandardCharsets.UTF_8);
        String path = tenantId == null
            ? "/api/v1/dashboards/" + encodedId
            : "/api/v1/" + tenantId + "/dashboards/" + encodedId;

        String username = null;
        String password = null;
        if (auth != null) {
            Optional<String> maybeUsername = runContext.render(auth.getUsername()).as(String.class);
            Optional<String> maybePassword = runContext.render(auth.getPassword()).as(String.class);
            if (maybeUsername.isPresent() && maybePassword.isPresent()) {
                username = maybeUsername.get();
                password = maybePassword.get();
            } else if (runContext.render(auth.getAuto()).as(Boolean.class).orElse(Boolean.TRUE)) {
                Optional<SDK.Auth> autoAuth = runContext.sdk().defaultAuthentication();
                if (autoAuth.isPresent() && autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                    username = autoAuth.get().username().get();
                    password = autoAuth.get().password().get();
                }
            }
        } else {
            Optional<SDK.Auth> autoAuth = runContext.sdk().defaultAuthentication();
            if (autoAuth.isPresent() && autoAuth.get().username().isPresent() && autoAuth.get().password().isPresent()) {
                username = autoAuth.get().username().get();
                password = autoAuth.get().password().get();
            }
        }

        HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
            .uri(URI.create(basePath + path))
            .addHeader("Accept", "application/x-yaml");
        if (username != null && password != null) {
            String encoded = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
            requestBuilder.addHeader("Authorization", "Basic " + encoded);
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
