package io.kestra.plugin.git;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.TransportException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void cloneSelfHostedGiteaRepo() throws Exception {
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(giteaRepoUrl))
            .username(Property.ofValue(giteaUserName))
            .password(Property.ofValue(giteaPat))
            .trustedCaPemPath(Property.ofValue(giteaCaPemPath))
            .build();

        Clone.Output runOutput = task.run(runContext);

        Collection<File> files = FileUtils.listFiles(Path.of(runOutput.getDirectory()).toFile(), null, true);
        assertThat(files, hasItems(
            hasProperty("path", endsWith("README.md")),
            hasProperty("path", containsString(".git"))
        ));
    }

    @Test
    void cloneSelfHostedGiteaRepo_shouldFailWithCertError() {
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(giteaRepoUrl))
            .username(Property.ofValue(giteaUserName))
            .password(Property.ofValue(giteaPat))
            .build();

        TransportException ex = assertThrows(TransportException.class, () -> task.run(runContext));

        assertThat(
            ex.getMessage(),
            containsString("Secure connection to https://localhost:3443/gitea_admin/kestra-test.git could not be established because of SSL problems")
        );
    }

    @Test
    void appliesGitConfig_coreFileModeFalse_and_ignoresPermChanges() throws Exception {
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue("https://github.com/kestra-io/plugin-template"))
            .gitConfig(Property.ofValue(Map.of(
                "core.fileMode", false
            )))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git git = Git.open(repoPath.toFile())) {
            boolean fileMode = git.getRepository().getConfig().getBoolean("core", null, "fileMode", true);
            assertThat(fileMode, is(false));
        }


        Path testFile = repoPath.resolve("filemode_test.sh");
        java.nio.file.Files.writeString(testFile, "#!/bin/sh\necho hi\n");
        Files.setPosixFilePermissions(testFile, PosixFilePermissions.fromString("rw-r--r--"));

        try (Git git = Git.open(repoPath.toFile())) {
            git.add().addFilepattern("filemode_test.sh").call();
            git.commit().setMessage("baseline").call();

            Files.setPosixFilePermissions(testFile, PosixFilePermissions.fromString("rwxr-xr-x"));

            var status = git.status().call();

            assertThat(status.getModified().isEmpty() && status.getChanged().isEmpty() &&
                    status.getAdded().isEmpty() && status.getRemoved().isEmpty() && status.getUncommittedChanges().isEmpty(),
                is(true)
            );
        }
    }

}
