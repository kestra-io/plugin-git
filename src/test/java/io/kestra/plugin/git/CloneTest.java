package io.kestra.plugin.git;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@MicronautTest
class CloneTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void publicRepository() throws Exception {
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url("https://github.com/kestra-io/plugin-template")
            .build();

        Clone.Output runOutput = task.run(runContext);

        assertThat(FileUtils.listFiles(Path.of(runOutput.getDirectory()).toFile(), null, true).size(), greaterThan(1));
    }
}
