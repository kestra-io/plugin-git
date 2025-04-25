package io.kestra.plugin.git;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class CloneTest extends AbstractGitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void publicRepository() throws Exception {
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.of("https://github.com/kestra-io/plugin-template"))
            .build();

        Clone.Output runOutput = task.run(runContext);

        Collection<File> files = FileUtils.listFiles(Path.of(runOutput.getDirectory()).toFile(), null, true);
        assertThat(files, hasItems(
            hasProperty("path", endsWith("README.md")),
            hasProperty("path", containsString(".git"))
        ));
    }

    @Test
    void privateRepository() throws Exception {
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(new Property<>(repositoryUrl))
            .username(new Property<>(pat))
            .password(new Property<>(pat))
            .build();

        Clone.Output runOutput = task.run(runContext);

        Collection<File> files = FileUtils.listFiles(Path.of(runOutput.getDirectory()).toFile(), null, true);
        assertThat(files, hasItems(
            hasProperty("path", endsWith("README.md")),
            hasProperty("path", containsString(".git"))
        ));
    }
}
