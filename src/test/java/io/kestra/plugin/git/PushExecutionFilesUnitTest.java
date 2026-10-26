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

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@KestraTest
class PushExecutionFilesUnitTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void instanceResourcesContentByPath_supportsRegexSpecialCharsInGlobsAndHandlesDuplicates() throws Exception {
        String namespace = "unit-test-ns";

        RunContext runContext = runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", TenantService.MAIN_TENANT,
                "namespace", namespace
            )
        ));

        URI uri1 = runContext.storage().namespace(namespace)
            .putFile(Path.of("file1.txt"), new ByteArrayInputStream("content1".getBytes(StandardCharsets.UTF_8)))
            .getFirst()
            .uri();
        URI uri2 = runContext.storage().namespace(namespace)
            .putFile(Path.of("file2.txt"), new ByteArrayInputStream("content2".getBytes(StandardCharsets.UTF_8)))
            .getFirst()
            .uri();
        URI uri3 = runContext.storage().namespace(namespace)
            .putFile(Path.of("file3.txt"), new ByteArrayInputStream("content3".getBytes(StandardCharsets.UTF_8)))
            .getFirst()
            .uri();

        Map<String, Object> outputs = Map.of(
            "task1", Map.of(
                "outputFiles", Map.of(
                    "report-(1).csv", uri1.toString(),
                    "common.log", uri1.toString(),
                    "report_csv", uri1.toString()
                )
            ),
            "task2", Map.of(
                "outputFiles", Map.of(
                    "report-(2).csv", uri2.toString(),
                    "common.log", uri2.toString(),
                    "other.txt", uri3.toString()
                )
            )
        );

        RunContext executionContext = runContextFactory.of(Map.of(
            "flow", Map.of(
                "tenantId", TenantService.MAIN_TENANT,
                "namespace", namespace
            ),
            "outputs", outputs
        ));

        PushExecutionFiles task = PushExecutionFiles.builder()
            .id("push-execution-files")
            .type(PushExecutionFiles.class.getName())
            .build();

        // 1. Verify globs with regex special chars (like parens) do not throw PatternSyntaxException
        // and properly match using PathMatcherPredicate rather than unescaped String.matches
        assertDoesNotThrow(() -> {
            var content = task.instanceResourcesContentByPath(executionContext, Path.of("base"), List.of("report-(*).csv"));
            assertThat(content.keySet(), containsInAnyOrder(Path.of("base/report-(1).csv"), Path.of("base/report-(2).csv")));
            assertThat(content.containsKey(Path.of("base/report_csv")), is(false));
        });

        // 2. Verify duplicate keys from multiple task outputs do not throw Duplicate key IllegalStateException
        assertDoesNotThrow(() -> {
            var content = task.instanceResourcesContentByPath(executionContext, Path.of("base"), null);
            assertThat(content.containsKey(Path.of("base/common.log")), is(true));
            assertThat(content.keySet(), hasItems(
                Path.of("base/report-(1).csv"),
                Path.of("base/report-(2).csv"),
                Path.of("base/common.log"),
                Path.of("base/other.txt"),
                Path.of("base/report_csv")
            ));
        });
    }
}
