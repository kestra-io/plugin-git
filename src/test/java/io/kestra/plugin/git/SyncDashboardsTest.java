package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.DashboardRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.utils.Rethrow;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class SyncDashboardsTest extends AbstractGitTest {
    public static final String BRANCH = "sync-dashboards";
    public static final String GIT_DIRECTORY = "to_clone/_dashboards";
    public static final String TENANT_ID = "my-tenant";
    public static final String FLOW_ID = "self_flow";
    public static final String NAMESPACE = "my.namespace";

    private static final Map<String, Instant> previousRevisionByUid = new HashMap<>();

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private YamlParser yamlFlowParser;

    @Inject
    private DashboardRepositoryInterface dashboardRepositoryInterface;

    @BeforeEach
    void init() {
        dashboardRepositoryInterface.findAll(TENANT_ID).forEach(dashboard -> {
            Dashboard deleted = dashboardRepositoryInterface.delete(TENANT_ID, dashboard.getId());
            previousRevisionByUid.put(deleted.uid(), deleted.getUpdated());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void syncDashboards_noDryRun(boolean delete) throws Exception {

        /*
            1. First dashboard - exists locally and on the Git server, title should be updated
            2. Second dashboard - local dashboard only, should be deleted if `delete` property is set to true
            3. Third dashboard - exists only on the Git server, should be added
            4. Fourth dashboard - same on local and sever, should be unchanged
        */
        DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, TENANT_ID, "First Dashboard - local ", "first-dashboard"); //1
        DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, TENANT_ID, "Local Dashboard - local", "local-dashboard"); //2
        DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, TENANT_ID, "Same Dashboard - local and server", "same-dashboard"); //4

        RunContext runContext = runContext();

        List<Dashboard> dashboards = dashboardRepositoryInterface.findAll(TENANT_ID);

        assertThat(dashboards, hasSize(3));
        dashboards.forEach(d -> previousRevisionByUid.put(d.uid(), d.getUpdated()));

        SyncDashboards task = SyncDashboards.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .delete(Property.of(delete))
            .build();

        SyncDashboards.Output syncOutput = task.run(runContext);

        dashboards = dashboardRepositoryInterface.findAll(TENANT_ID);

        if (delete) {
            assertThat(dashboards, hasSize(3));
            assertThat(dashboards.stream().map(Dashboard::getId).toList(), containsInAnyOrder("first-dashboard", "same-dashboard", "new-dashboard"));
        } else {
            assertThat(dashboards, hasSize(4));
            assertThat(dashboards.stream().map(Dashboard::getId).toList(), containsInAnyOrder("first-dashboard", "same-dashboard", "new-dashboard", "local-dashboard"));
        }


        List<DashboardDiffOutput> expectedDiffs = new ArrayList<>(
            List.of(
                DashboardDiffOutput.builder().gitPath("to_clone/_dashboards/first-dashboard.yml").syncState("UPDATED").dashboardId("first-dashboard").build(),
                DashboardDiffOutput.builder().gitPath("to_clone/_dashboards/new-dashboard.yml").syncState("ADDED").dashboardId("new-dashboard").build(),
                DashboardDiffOutput.builder().gitPath("to_clone/_dashboards/same-dashboard.yml").syncState("UNCHANGED").dashboardId("same-dashboard").build()
            )
        );

        if (delete) {
            //If delete is true `local-dashboard` should be deleted
            expectedDiffs.add(DashboardDiffOutput.builder().gitPath(null).syncState("DELETED").dashboardId("local-dashboard").build());
        }

        assertDiffs(runContext, syncOutput.diffFileUri(), expectedDiffs);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void syncDashboards_dryRunSetToTrue(boolean delete) throws Exception {

        /*
        Dry run is set to true
            1. First dashboard - exists locally and on the Git server, title should be updated
            2. Second dashboard - local dashboard only, should be deleted if `delete` property is set to true
            3. Third dashboard - exists only on the Git server, should be added
            4. Fourth dashboard - same on local and sever, should be unchanged
        */
        DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, TENANT_ID, "First Dashboard - local ", "first-dashboard"); //1
        DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, TENANT_ID, "Local Dashboard - local", "local-dashboard"); //2
        DashboardUtils.createDashboard(dashboardRepositoryInterface, yamlFlowParser, TENANT_ID, "Same Dashboard - local and server", "same-dashboard"); //4

        RunContext runContext = runContext();

        List<Dashboard> dashboards = dashboardRepositoryInterface.findAll(TENANT_ID);

        assertThat(dashboards, hasSize(3));
        dashboards.forEach(d -> previousRevisionByUid.put(d.uid(), d.getUpdated()));

        SyncDashboards task = SyncDashboards.builder()
            .url(new Property<>("{{url}}"))
            .username(new Property<>("{{pat}}"))
            .password(new Property<>("{{pat}}"))
            .branch(new Property<>("{{branch}}"))
            .gitDirectory(new Property<>("{{gitDirectory}}"))
            .delete(Property.of(delete))
            .dryRun(Property.of(true))
            .build();

        SyncDashboards.Output syncOutput = task.run(runContext);

        dashboards = dashboardRepositoryInterface.findAll(TENANT_ID);

        //No changes to local files
        assertThat(dashboards, hasSize(3));
        assertThat(dashboards.stream().map(Dashboard::getId).toList(), containsInAnyOrder("first-dashboard", "local-dashboard", "same-dashboard"));


        List<DashboardDiffOutput> expectedDiffs = new ArrayList<>(
            List.of(
                DashboardDiffOutput.builder().gitPath("to_clone/_dashboards/first-dashboard.yml").syncState("UPDATED").dashboardId("first-dashboard").build(),
                DashboardDiffOutput.builder().gitPath("to_clone/_dashboards/new-dashboard.yml").syncState("ADDED").dashboardId("new-dashboard").build(),
                DashboardDiffOutput.builder().gitPath("to_clone/_dashboards/same-dashboard.yml").syncState("UNCHANGED").dashboardId("same-dashboard").build()
            )
        );

        if (delete) {
            //If delete is true `local-dashboard` should be deleted
            expectedDiffs.add(DashboardDiffOutput.builder().gitPath(null).syncState("DELETED").dashboardId("local-dashboard").build());
        }

        assertDiffs(runContext, syncOutput.diffFileUri(), expectedDiffs);
    }

    private static void assertDiffs(RunContext runContext, URI diffFileUri, List<DashboardDiffOutput> expectedDiffs) throws IOException {
        String diffSummary = IOUtils.toString(runContext.storage().getFile(diffFileUri), StandardCharsets.UTF_8);
        List<Map<String, Object>> diffList = diffSummary.lines()
            .map(Rethrow.throwFunction(diffMap -> JacksonMapper.ofIon().readValue(diffMap, new TypeReference<Map<String, Object>>() {
            })))
            .toList();

        assertThat(diffList.size(), equalTo(expectedDiffs.size()));

        for (DashboardDiffOutput expectedDiff : expectedDiffs) {
            Optional<Map<String, Object>> actualDiff = diffList.stream().filter(m -> expectedDiff.getDashboardId().equals(m.get("dashboardId"))).findFirst();
            assertThat(actualDiff.isPresent(), is(true));
            assertThat(actualDiff.get().get("gitPath"), equalTo(expectedDiff.getGitPath()));
            assertThat(actualDiff.get().get("syncState"), equalTo(expectedDiff.getSyncState()));
        }
    }

    private RunContext runContext() {
        return runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", SyncDashboardsTest.TENANT_ID,
                "namespace", SyncDashboardsTest.NAMESPACE,
                "id", SyncDashboardsTest.FLOW_ID
            ),
            "url", repositoryUrl,
            "pat", pat,
            "branch", SyncDashboardsTest.BRANCH,
            "namespace", SyncDashboardsTest.NAMESPACE,
            "gitDirectory", SyncDashboardsTest.GIT_DIRECTORY
        ));
    }

    static DashboardDiffOutput diffMapToDashboardDiffOutput(Map<String, Object> diffMap) {
        return DashboardDiffOutput.builder()
            .dashboardId((String) diffMap.get("dashboardId"))
            .gitPath((String) diffMap.get("gitPath"))
            .syncState((String) diffMap.get("syncState"))
            .build();
    }

    @Builder
    @Getter
    public static class DashboardDiffOutput {
        String gitPath;
        String syncState;
        String dashboardId;
    }
}
