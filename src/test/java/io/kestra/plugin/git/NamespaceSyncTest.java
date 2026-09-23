package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.git.shared.AbstractGitTask;
import io.kestra.plugin.git.shared.SourceOfTruth;
import io.kestra.plugin.git.shared.SourceOfTruthOverrides;
import io.kestra.plugin.git.shared.testkit.AbstractGitTest;
import io.kestra.plugin.git.shared.testkit.MockKestraApiServer;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
public class NamespaceSyncTest extends AbstractGitTest {
    private static final String BRANCH_PREFIX = "namespace-sync";
    private static final String GIT_DIRECTORY = "kestra";
    private static final String TENANT_ID = TenantService.MAIN_TENANT;
    private static final String NAMESPACE = "company.team";

    private String branch;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FlowRepositoryInterface flowRepository;

    @Inject
    private StorageInterface storageInterface;

    private MockKestraApiServer server;

    @BeforeEach
    void startMockServer() throws IOException {
        server = MockKestraApiServer.start(flowRepository);
    }

    @AfterEach
    void stopMockServer() {
        server.close();
    }

    @BeforeEach
    void initBranch() {
        branch = BRANCH_PREFIX + "-" + Long.toHexString(System.nanoTime());
    }

    @BeforeEach
    void cleanState() throws Exception {
        RunContext runContext = runContextFactory.of();

        flowRepository.findAllForAllTenants()
            .forEach(f -> flowRepository.delete(FlowWithSource.of(f, "")));

        var namespaceFiles = runContext.storage().namespace(NAMESPACE).all();
        for (NamespaceFile nsFile : namespaceFiles) {
            runContext.storage().namespace(NAMESPACE).delete(nsFile);
        }
    }

    @Test
    void kestraToGit_dryRun_diff() throws Exception {
        RunContext rc = runContext();

        createFlowInKestra("alpha", NAMESPACE);
        putNsFile(rc, "scripts/hello.sh", "echo hello");

        NamespaceSync task = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(true))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        NamespaceSync.Output out = task.run(rc);

        List<AbstractGitTask.DiffLine> lines = readIon(out.getDiff(), rc);
        assertFalse(lines.isEmpty());
        assertNull(out.getCommitId());
        assertNull(out.getCommitURL());
    }

    @Test
    void kestraToGit_apply_writesIntoGit() throws Exception {
        RunContext rc = runContext();

        String suf = Long.toHexString(System.nanoTime());
        String flowId = "alpha-" + suf;
        String cfgRel = "data/config-" + suf + ".json";

        createFlowInKestra(flowId, NAMESPACE);
        putNsFile(rc, cfgRel, "{\"x\":" + System.currentTimeMillis() + "}");

        NamespaceSync task = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(false))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        NamespaceSync.Output out = task.run(rc);
        assertNotNull(out.getCommitId());

        RunContext cloneCtx = runContextFactory.of();
        Clone.builder()
            .url(Property.ofValue(repositoryUrl))
            .username(Property.ofValue(pat))
            .password(Property.ofValue(pat))
            .branch(Property.ofValue(branch))
            .build()
            .run(cloneCtx);

        Path base = cloneCtx.workingDir().path()
            .resolve(GIT_DIRECTORY)
            .resolve(NAMESPACE);

        assertTrue(Files.exists(base.resolve("flows/" + flowId + ".yaml")));
        assertTrue(Files.exists(base.resolve("files/" + cfgRel)));

        if (out.getDiff() != null) {
            String ion = IOUtils.toString(cloneCtx.storage().getFile(out.getDiff()), StandardCharsets.UTF_8);
            assertFalse(ion.isBlank());
        }
    }

    @Test
    void gitToKestra_keepMissing() throws Exception {
        RunContext rc = runContext();

        String suf = Long.toHexString(System.nanoTime());
        String baseId = "base-" + suf;
        String extraId = "extra-" + suf;

        createFlowInKestra(baseId, NAMESPACE);
        runKestraToGitApply();
        createFlowInKestra(extraId, NAMESPACE);

        NamespaceSync task = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.GIT))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(false))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        task.run(rc);

        List<Flow> flows = flowRepository.findByNamespace(TENANT_ID, NAMESPACE);
        Set<String> ids = new HashSet<>();
        flows.forEach(f -> ids.add(f.getId()));
        assertThat(ids, containsInAnyOrder(baseId, extraId));
    }

    @Test
    void kestraToGit_apply_excludesDraftFlows() throws Exception {
        RunContext rc = runContext();

        String suf = Long.toHexString(System.nanoTime());
        String flowId = "alpha-" + suf;
        String draftFlowId = "draft-" + suf;

        createFlowInKestra(flowId, NAMESPACE);
        createDraftFlow(flowRepository, TENANT_ID, draftFlowId, NAMESPACE);

        NamespaceSync task = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(false))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        NamespaceSync.Output out = task.run(rc);
        assertNotNull(out.getCommitId());

        RunContext cloneCtx = runContextFactory.of();
        Clone.builder()
            .url(Property.ofValue(repositoryUrl))
            .username(Property.ofValue(pat))
            .password(Property.ofValue(pat))
            .branch(Property.ofValue(branch))
            .build()
            .run(cloneCtx);

        Path base = cloneCtx.workingDir().path()
            .resolve(GIT_DIRECTORY)
            .resolve(NAMESPACE);

        assertTrue(Files.exists(base.resolve("flows/" + flowId + ".yaml")));
        assertFalse(Files.exists(base.resolve("flows/" + draftFlowId + ".yaml")));
    }

    @Test
    void namespaceCheck_404_reportsStatusRouteAndResolvedKestraUrl() throws Exception {
        // The mock server used by every other test never 404s on GET /namespaces/{id} (as real OSS Kestra
        // doesn't either), so a dedicated server is needed here to force the 404 branch.
        var httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.createContext("/api/v1/" + TENANT_ID + "/namespaces/" + NAMESPACE, exchange ->
        {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        httpServer.start();

        try {
            var kestraUrl = "http://localhost:" + httpServer.getAddress().getPort();
            NamespaceSync task = NamespaceSync.builder()
                .url(Property.ofExpression("{{url}}"))
                .username(Property.ofExpression("{{pat}}"))
                .password(Property.ofExpression("{{pat}}"))
                .branch(Property.ofExpression("{{branch}}"))
                .namespace(Property.ofExpression("{{namespace}}"))
                .kestraUrl(Property.ofValue(kestraUrl))
                .build();

            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> task.run(runContext())
            );

            assertThat(exception.getMessage(), containsString("HTTP 404"));
            assertThat(exception.getMessage(), containsString("/api/v1/" + TENANT_ID + "/namespaces/" + NAMESPACE));
            assertThat(exception.getMessage(), containsString(kestraUrl));
        } finally {
            httpServer.stop(0);
        }
    }

    @Test
    void mixed_pushesFlowsToGitAndPullsFilesFromGit_inOneRun() throws Exception {
        RunContext rc = runContext();

        String suf = Long.toHexString(System.nanoTime());
        String flowId = "push-" + suf;
        String fileRel = "pulled/data-" + suf + ".txt";

        seedGitOnlyFile(rc, fileRel);
        createFlowInKestra(flowId, NAMESPACE);

        NamespaceSync.Output out = mixedTask(NamespaceSync.WhenMissingInSource.KEEP, false).run(rc);

        Path base = clonedNamespaceBase();
        assertTrue(Files.exists(base.resolve("flows/" + flowId + ".yaml")), "flow should have been pushed to Git");

        assertTrue(
            rc.storage().namespace(NAMESPACE).all().stream().anyMatch(f -> f.path().toString().replace("\\", "/").endsWith(fileRel)),
            "file should have been pulled back into Kestra"
        );

        // Mixed direction must record both sides (createIonDiff alone would only see the flow push, not the file pull)
        List<AbstractGitTask.DiffLine> diffs = readIon(out.getDiff(), rc);
        assertTrue(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FLOW));
        assertTrue(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FILE));
    }

    @Test
    void mixed_dryRun_diffContainsBothDirectionsAndAppliesNothing() throws Exception {
        RunContext rc = runContext();

        String suf = Long.toHexString(System.nanoTime());
        String flowId = "dryflow-" + suf;
        String fileRel = "dry/data-" + suf + ".txt";

        seedGitOnlyFile(rc, fileRel);
        createFlowInKestra(flowId, NAMESPACE);

        NamespaceSync.Output out = mixedTask(NamespaceSync.WhenMissingInSource.KEEP, true).run(rc);
        assertNull(out.getCommitId());

        List<AbstractGitTask.DiffLine> diffs = readIon(out.getDiff(), rc);
        assertTrue(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FLOW && d.getKey().contains(flowId)));
        assertTrue(diffs.stream().anyMatch(d -> d.getKind() == AbstractGitTask.Kind.FILE));

        assertTrue(
            rc.storage().namespace(NAMESPACE).all().stream().noneMatch(f -> f.path().toString().replace("\\", "/").endsWith(fileRel)),
            "dry run must not have applied the pull"
        );
    }

    @Test
    void mixed_whenMissingInSourceDelete_deletesOnCorrectSidePerKind() throws Exception {
        RunContext rc = runContext();

        String suf = Long.toHexString(System.nanoTime());
        String gitOnlyFlowId = "gitonly-" + suf;
        String kestraOnlyFileRel = "extra/only-in-kestra-" + suf + ".txt";

        createFlowInKestra(gitOnlyFlowId, NAMESPACE);
        runKestraToGitApply();
        flowRepository.findByNamespaceWithSource(TENANT_ID, NAMESPACE).stream()
            .filter(f -> gitOnlyFlowId.equals(f.getId()))
            .forEach(f -> flowRepository.delete(f));

        putNsFile(rc, kestraOnlyFileRel, "kestra-only");

        mixedTask(NamespaceSync.WhenMissingInSource.DELETE, false).run(rc);

        // flows: Kestra is the source of truth -> missing-from-Kestra flow is deleted from Git
        Path base = clonedNamespaceBase();
        assertFalse(Files.exists(base.resolve("flows/" + gitOnlyFlowId + ".yaml")));

        // Namespace Files: Git is the source of truth -> missing-from-Git file is deleted from Kestra
        assertTrue(
            rc.storage().namespace(NAMESPACE).all().stream().noneMatch(f -> f.path().toString().replace("\\", "/").endsWith(kestraOnlyFileRel))
        );
    }

    @Test
    void noOverride_and_degenerateOverride_produceTheSameDiff() throws Exception {
        String suf = Long.toHexString(System.nanoTime());
        String flowId = "match-" + suf;
        createFlowInKestra(flowId, NAMESPACE);

        NamespaceSync withoutOverride = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(true))
            .kestraUrl(Property.ofValue(server.url()))
            .build();
        RunContext rcWithoutOverride = runContext();
        List<AbstractGitTask.DiffLine> diffsWithoutOverride = readIon(withoutOverride.run(rcWithoutOverride).getDiff(), rcWithoutOverride);

        NamespaceSync withDegenerateOverride = withoutOverride.toBuilder()
            .sourceOfTruthOverrides(
                SourceOfTruthOverrides.builder()
                    .flows(Property.ofValue(SourceOfTruth.KESTRA))
                    .namespaceFiles(Property.ofValue(SourceOfTruth.KESTRA))
                    .build()
            )
            .build();
        RunContext rcWithOverride = runContext();
        List<AbstractGitTask.DiffLine> diffsWithOverride = readIon(withDegenerateOverride.run(rcWithOverride).getDiff(), rcWithOverride);

        assertEquals(
            diffsWithoutOverride.stream().map(d -> d.getKind() + ":" + d.getKey() + ":" + d.getAction()).sorted().toList(),
            diffsWithOverride.stream().map(d -> d.getKind() + ":" + d.getKey() + ":" + d.getAction()).sorted().toList()
        );
    }

    @Test
    void includeChildNamespaces_gitOnlyChildWithOnlyFlows_staysKestraSourced_notTouchedByFileSourceOfTruth() throws Exception {
        String suf = Long.toHexString(System.nanoTime());
        String childNamespace = NAMESPACE + ".childonly-" + suf;
        String flowId = "childflow-" + suf;

        seedGitOnlyChildFlow(childNamespace, flowId);

        // flows stay Kestra-sourced (default); only Namespace Files are Git-sourced. With whenMissingInSource
        // DELETE, a child namespace holding only a Git-only flows/ directory must never be pulled into the sync
        // via includeChildNamespaces — otherwise its Git-only flow (missing from Kestra) would be wrongly deleted
        // from Git, even though flows are not Git-sourced.
        NamespaceSync task = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .includeChildNamespaces(Property.ofValue(true))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .sourceOfTruthOverrides(SourceOfTruthOverrides.builder().namespaceFiles(Property.ofValue(SourceOfTruth.GIT)).build())
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.DELETE))
            .dryRun(Property.ofValue(false))
            .kestraUrl(Property.ofValue(server.url()))
            .build();

        task.run(runContext());

        Path childBase = clonedBase(childNamespace);
        assertTrue(
            Files.exists(childBase.resolve("flows/" + flowId + ".yaml")),
            "flows stay Kestra-sourced: a Git-only child namespace with only flows/ must not be auto-created nor have its flow deleted"
        );
    }

    private void deleteNsFile(RunContext rc, String rel) throws Exception {
        var toDelete = rc.storage().namespace(NAMESPACE).all().stream()
            .filter(f -> f.path().toString().replace("\\", "/").endsWith(rel))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Namespace file not found: " + rel));
        rc.storage().namespace(NAMESPACE).delete(toDelete);
    }

    /** Pushes {@code rel} to Git via a uniform Kestra->Git run, then removes it from Kestra, leaving it Git-only. */
    private void seedGitOnlyFile(RunContext rc, String rel) throws Exception {
        putNsFile(rc, rel, "from-git");
        runKestraToGitApply();
        deleteNsFile(rc, rel);
    }

    /** A mixed-direction task: flows pushed Kestra->Git (default), Namespace Files pulled Git->Kestra (override). */
    private NamespaceSync mixedTask(NamespaceSync.WhenMissingInSource whenMissingInSource, boolean dryRun) {
        return NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .sourceOfTruthOverrides(SourceOfTruthOverrides.builder().namespaceFiles(Property.ofValue(SourceOfTruth.GIT)).build())
            .whenMissingInSource(Property.ofValue(whenMissingInSource))
            .dryRun(Property.ofValue(dryRun))
            .kestraUrl(Property.ofValue(server.url()))
            .build();
    }

    /** Clones the branch fresh and returns the working-tree path for {@link #NAMESPACE} under {@link #GIT_DIRECTORY}. */
    private Path clonedNamespaceBase() throws Exception {
        return clonedBase(NAMESPACE);
    }

    /** Clones the branch fresh and returns the working-tree path for {@code namespace} under {@link #GIT_DIRECTORY}. */
    private Path clonedBase(String namespace) throws Exception {
        RunContext cloneCtx = runContextFactory.of();
        Clone.builder()
            .url(Property.ofValue(repositoryUrl))
            .username(Property.ofValue(pat))
            .password(Property.ofValue(pat))
            .branch(Property.ofValue(branch))
            .build()
            .run(cloneCtx);
        return cloneCtx.workingDir().path().resolve(GIT_DIRECTORY).resolve(namespace);
    }

    /**
     * Pushes {@code flowId} from Kestra to Git for {@code childNamespace} directly (bypassing
     * {@code includeChildNamespaces}, since the mock Kestra API server used here does not implement the
     * namespaces-search endpoint it relies on), then deletes it from Kestra — leaving the child namespace with a
     * Git-only {@code flows/} directory and no matching Kestra namespace.
     */
    private void seedGitOnlyChildFlow(String childNamespace, String flowId) throws Exception {
        createFlowInKestra(flowId, childNamespace);

        Map<String, Object> ctx = new HashMap<>(
            Map.of(
                "flow", Map.of("tenantId", TENANT_ID, "namespace", childNamespace),
                "url", repositoryUrl,
                "pat", pat,
                "branch", branch,
                "namespace", childNamespace,
                "gitDirectory", GIT_DIRECTORY
            )
        );
        RunContext rc = runContextFactory.of(ctx);

        NamespaceSync push = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(false))
            .kestraUrl(Property.ofValue(server.url()))
            .build();
        push.run(rc);

        flowRepository.findByNamespaceWithSource(TENANT_ID, childNamespace).stream()
            .filter(f -> flowId.equals(f.getId()))
            .forEach(flowRepository::delete);
    }

    private RunContext runContext() {
        Map<String, Object> ctx = new HashMap<>(
            Map.of(
                "flow", Map.of(
                    "tenantId", TENANT_ID,
                    "namespace", NAMESPACE
                ),
                "url", repositoryUrl,
                "pat", pat,
                "branch", branch,
                "namespace", NAMESPACE,
                "gitDirectory", GIT_DIRECTORY
            )
        );
        var rc = runContextFactory.of(ctx);
        runContextFactory.initializer().forExecutor((DefaultRunContext) rc);
        return rc;
    }

    private void createFlowInKestra(String id, String namespace) {
        String src = """
            id: %s
            namespace: %s

            tasks:
              - id: say
                type: io.kestra.plugin.core.log.Log
                message: hello
            """.formatted(id, namespace);
        GenericFlow f = GenericFlow.fromYaml(TENANT_ID, src);
        flowRepository.create(f.toBuilder().source(src).build());
    }

    private void putNsFile(RunContext rc, String rel, String content) throws Exception {
        try (InputStream in = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            rc.storage().namespace(NAMESPACE).putFile(Path.of(rel), in);
        }
    }

    private void runKestraToGitApply() throws Exception {
        RunContext rc = runContext();
        NamespaceSync push = NamespaceSync.builder()
            .url(Property.ofExpression("{{url}}"))
            .username(Property.ofExpression("{{pat}}"))
            .password(Property.ofExpression("{{pat}}"))
            .branch(Property.ofExpression("{{branch}}"))
            .gitDirectory(Property.ofExpression("{{gitDirectory}}"))
            .namespace(Property.ofExpression("{{namespace}}"))
            .sourceOfTruth(Property.ofValue(SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.DELETE))
            .dryRun(Property.ofValue(false))
            .kestraUrl(Property.ofValue(server.url()))
            .build();
        push.run(rc);
    }

    private List<AbstractGitTask.DiffLine> readIon(URI uri, RunContext rc) throws Exception {
        try (InputStream in = rc.storage().getFile(uri)) {
            return JacksonMapper.ofIon().readValue(in, new TypeReference<>() {
            });
        }
    }
}
