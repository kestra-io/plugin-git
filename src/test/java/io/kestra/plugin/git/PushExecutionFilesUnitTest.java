package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class PushExecutionFilesUnitTest {
    private static final String NAMESPACE = "unit-test-ns";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void globsMatchOutputFileKeysAsGlobs() throws Exception {
        var uri = storedFile("file1.txt");
        var executionContext = executionContext(
            Map.of(
                "task1", Map.of(
                    "outputFiles", Map.of(
                        "report.csv", uri.toString(),
                        "report_csv", uri.toString(),
                        "reports/nested.csv", uri.toString(),
                        "notes.txt", uri.toString()
                    )
                )
            )
        );

        var content = task().instanceResourcesContentByPath(executionContext, Path.of("base"), List.of("*.csv"));

        assertThat(content.keySet(), containsInAnyOrder(Path.of("base/report.csv"), Path.of("base/reports/nested.csv")));
    }

    @Test
    void duplicateOutputFileNamesKeepOne() throws Exception {
        var uri1 = storedFile("file1.txt");
        var uri2 = storedFile("file2.txt");
        var executionContext = executionContext(
            Map.of(
                "task1", Map.of("outputFiles", Map.of("common.log", uri1.toString())),
                "task2", Map.of("outputFiles", Map.of("common.log", uri2.toString(), "other.txt", uri2.toString()))
            )
        );

        var content = task().instanceResourcesContentByPath(executionContext, Path.of("base"), null);

        assertThat(content.keySet(), containsInAnyOrder(Path.of("base/common.log"), Path.of("base/other.txt")));
    }

    private PushExecutionFiles task() {
        return PushExecutionFiles.builder()
            .id(IdUtils.create())
            .type(PushExecutionFiles.class.getName())
            .build();
    }

    private RunContext executionContext(Map<String, Object> outputs) {
        return runContextFactory.of(
            Map.of(
                "flow", Map.of("tenantId", TenantService.MAIN_TENANT, "namespace", NAMESPACE),
                "outputs", outputs
            )
        );
    }

    private URI storedFile(String name) throws Exception {
        var runContext = runContextFactory.of(Map.of("flow", Map.of("tenantId", TenantService.MAIN_TENANT, "namespace", NAMESPACE)));
        return runContext.storage().namespace(NAMESPACE)
            .putFile(Path.of(name), new ByteArrayInputStream(name.getBytes(StandardCharsets.UTF_8)))
            .getFirst()
            .uri();
    }
}
