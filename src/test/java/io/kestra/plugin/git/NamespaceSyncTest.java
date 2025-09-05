package io.kestra.plugin.git;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.models.property.Property;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageContext;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
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

    @BeforeEach
    void initBranch() {
        branch = BRANCH_PREFIX + "-" + Long.toHexString(System.nanoTime());
    }

    @BeforeEach
    void cleanState() throws Exception {
        flowRepository.findAllForAllTenants()
            .forEach(f -> flowRepository.delete(FlowWithSource.of(f, "")));

        URI prefix = URI.create(StorageContext.namespaceFilePrefix(NAMESPACE));
        var uris = storageInterface.allByPrefix(TENANT_ID, NAMESPACE, prefix, true);
        for (URI uri : uris) {
            storageInterface.delete(TENANT_ID, NAMESPACE, uri);
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
            .sourceOfTruth(Property.ofValue(NamespaceSync.SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(true))
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
            .sourceOfTruth(Property.ofValue(NamespaceSync.SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(false))
            .build();

        NamespaceSync.Output out = task.run(rc);
        assertNotNull(out.getCommitId());

        RunContext cloneCtx = runContextFactory.of();
        Clone.builder()
            .url(new Property<>(repositoryUrl))
            .username(new Property<>(pat))
            .password(new Property<>(pat))
            .branch(new Property<>(branch))
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
            .sourceOfTruth(Property.ofValue(NamespaceSync.SourceOfTruth.GIT))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.KEEP))
            .dryRun(Property.ofValue(false))
            .build();

        task.run(rc);

        List<Flow> flows = flowRepository.findByNamespace(TENANT_ID, NAMESPACE);
        Set<String> ids = new HashSet<>();
        flows.forEach(f -> ids.add(f.getId()));
        assertThat(ids, containsInAnyOrder(baseId, extraId));
    }

    private RunContext runContext() {
        Map<String, Object> ctx = new HashMap<>(Map.of(
            "flow", Map.of(
                "tenantId", TENANT_ID,
                "namespace", NAMESPACE
            ),
            "url", repositoryUrl,
            "pat", pat,
            "branch", branch,
            "namespace", NAMESPACE,
            "gitDirectory", GIT_DIRECTORY
        ));
        return runContextFactory.of(ctx);
    }

    private void createFlowInKestra(String id, String namespace) {
        String src = """
            id: %s
            namespace: %s

            tasks:
              - id: say
                type: io.kestra.core.tasks.log.Log
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
            .sourceOfTruth(Property.ofValue(NamespaceSync.SourceOfTruth.KESTRA))
            .whenMissingInSource(Property.ofValue(NamespaceSync.WhenMissingInSource.DELETE))
            .dryRun(Property.ofValue(false))
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
