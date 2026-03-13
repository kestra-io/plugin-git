package io.kestra.plugin.git;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.lib.PersonIdent;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

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
            .url(Property.ofValue("https://github.com/kestra-io/plugin-template"))
            .build();

        Clone.Output runOutput = task.run(runContext);

        Collection<File> files = FileUtils.listFiles(Path.of(runOutput.getDirectory()).toFile(), null, true);
        assertThat(
            files, hasItems(
                hasProperty("path", endsWith("README.md")),
                hasProperty("path", containsString(".git"))
            )
        );
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
        assertThat(
            files, hasItems(
                hasProperty("path", endsWith("README.md")),
                hasProperty("path", containsString(".git"))
            )
        );
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
        assertThat(
            files, hasItems(
                hasProperty("path", endsWith("README.md")),
                hasProperty("path", containsString(".git"))
            )
        );
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
            .gitConfig(
                Property.ofValue(
                    Map.of(
                        "core.fileMode", false
                    )
                )
            )
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

            assertThat(
                status.getModified().isEmpty() && status.getChanged().isEmpty() &&
                    status.getAdded().isEmpty() && status.getRemoved().isEmpty() && status.getUncommittedChanges().isEmpty(),
                is(true)
            );
        }
    }

    @Test
    void cloneAtSpecificCommit() throws Exception {
        // Given a local repo with 2 commits
        Path remote = Files.createTempDirectory("git-remote-");
        Path file1 = remote.resolve("file1.txt");
        Path file2 = remote.resolve("file2.txt");

        String firstCommitSha;
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file1, "first\n");
            git.add().addFilepattern("file1.txt").call();
            git.commit().setMessage("first").call();
            firstCommitSha = git.getRepository().resolve("HEAD").name();

            Files.writeString(file2, "second\n");
            git.add().addFilepattern("file2.txt").call();
            git.commit().setMessage("second").call();
        }

        // When cloning at a specific commit
        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .commit(Property.ofValue(firstCommitSha))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        // Then the repo is cloned at the specified commit
        try (Git cloned = Git.open(repoPath.toFile())) {
            String fullBranch = cloned.getRepository().getFullBranch();
            assertThat(fullBranch, is(firstCommitSha));
        }

        assertThat(Files.exists(repoPath.resolve("file1.txt")), is(true));
        assertThat(Files.exists(repoPath.resolve("file2.txt")), is(false));

        assertThat(repoPath.resolve(".git").toFile().exists(), is(true));
    }

    @Test
    void cloneIntoNonEmptyDirectory() throws Exception {
        // Given a local repo to clone from
        Path remote = Files.createTempDirectory("git-remote-");
        Path remoteFile = remote.resolve("repo-file.txt");

        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remoteFile, "from repo\n");
            git.add().addFilepattern("repo-file.txt").call();
            git.commit().setMessage("initial").call();
        }

        // And a working directory that already has files (simulating WorkingDirectory inputFiles)
        RunContext runContext = runContextFactory.of();
        Path workingDir = runContext.workingDir().path();
        Files.writeString(workingDir.resolve("pre-existing.txt"), "I was here first\n");

        // When cloning into the non-empty working directory
        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .build();

        Clone.Output out = task.run(runContext);

        // Then both the pre-existing file and the cloned file should be present
        Path repoPath = Path.of(out.getDirectory());
        assertThat(Files.exists(repoPath.resolve("pre-existing.txt")), is(true));
        assertThat(Files.readString(repoPath.resolve("pre-existing.txt")), is("I was here first\n"));
        assertThat(Files.exists(repoPath.resolve("repo-file.txt")), is(true));
        assertThat(Files.readString(repoPath.resolve("repo-file.txt")), is("from repo\n"));
        assertThat(repoPath.resolve(".git").toFile().exists(), is(true));
    }

    @Test
    void cloneIntoNonEmptySubdirectory() throws Exception {
        // Given a local repo to clone from
        Path remote = Files.createTempDirectory("git-remote-");

        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remote.resolve("repo-file.txt"), "from repo\n");
            git.add().addFilepattern("repo-file.txt").call();
            git.commit().setMessage("initial").call();
        }

        // And a working directory with a non-empty subdirectory
        RunContext runContext = runContextFactory.of();
        Path subDir = runContext.workingDir().resolve(Path.of("myrepo"));
        Files.createDirectories(subDir);
        Files.writeString(subDir.resolve("pre-existing.txt"), "I was here first\n");

        // When cloning into the non-empty subdirectory
        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .directory(Property.ofValue("myrepo"))
            .build();

        Clone.Output out = task.run(runContext);

        // Then both files should be present
        Path repoPath = Path.of(out.getDirectory());
        assertThat(Files.exists(repoPath.resolve("pre-existing.txt")), is(true));
        assertThat(Files.exists(repoPath.resolve("repo-file.txt")), is(true));
        assertThat(repoPath.resolve(".git").toFile().exists(), is(true));
    }

    @Test
    void cloneAtSpecificTag() throws Exception {
        // Given a local repo with 2 commits and a tag
        Path remote = Files.createTempDirectory("git-remote-");
        Path file1 = remote.resolve("file1.txt");

        String tagName = "v1.0";
        String tagCommitSha;

        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file1, "first\n");
            git.add().addFilepattern("file1.txt").call();
            git.commit().setMessage("first").call();
            tagCommitSha = git.getRepository().resolve("HEAD").name();

            git.tag()
                .setName(tagName)
                .setMessage("Release " + tagName)
                .setTagger(new PersonIdent("Test User", "test@example.com"))
                .call();

            Files.writeString(file1, "second\n");
            git.add().addFilepattern("file1.txt").call();
            git.commit().setMessage("second").call();
        }

        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .tag(Property.ofValue(tagName))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git cloned = Git.open(repoPath.toFile())) {
            String headCommit = cloned.getRepository().findRef("HEAD").getObjectId().name();

            assertThat(headCommit, is(tagCommitSha));
        }

        String content = Files.readString(repoPath.resolve("file1.txt"));
        assertThat(content, is("first\n"));
    }

}
