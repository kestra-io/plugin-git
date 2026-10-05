package io.kestra.plugin.git;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.lib.PersonIdent;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.git.shared.testkit.AbstractGitTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CloneTest extends AbstractGitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private DispatchQueueInterface<LogEntry> logQueue;

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
            .url(Property.ofValue(repositoryUrl))
            .username(Property.ofValue(pat))
            .password(Property.ofValue(pat))
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

        Path remote = Files.createTempDirectory("git-remote-filemode-");
        PersonIdent author = new PersonIdent("Test User", "test@example.com");
        try (Git remoteGit = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remote.resolve("README.md"), "hello\n");
            remoteGit.add().addFilepattern("README.md").call();
            remoteGit.commit().setMessage("init").setAuthor(author).setCommitter(author).call();
        }

        Clone task = Clone.builder()
            .url(Property.ofValue("file://" + remote))
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
        Files.writeString(testFile, "#!/bin/sh\necho hi\n");
        Files.setPosixFilePermissions(testFile, PosixFilePermissions.fromString("rw-r--r--"));

        try (Git git = Git.open(repoPath.toFile())) {
            git.add().addFilepattern("filemode_test.sh").call();
            git.commit().setMessage("baseline").setAuthor(author).setCommitter(author).call();

            Files.setPosixFilePermissions(testFile, PosixFilePermissions.fromString("rwxr-xr-x"));

            var status = git.status().addPath("filemode_test.sh").call();

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

    @Test
    void cloneOnlyConfiguredBranches() throws Exception {
        // Given a local remote with two branches
        Path remote = Files.createTempDirectory("git-remote-");
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            String initialBranch = git.getRepository().getBranch();

            Files.writeString(remote.resolve("main.txt"), "main\n");
            git.add().addFilepattern("main.txt").call();
            git.commit().setMessage("main").call();

            git.checkout().setCreateBranch(true).setName("feature/only").call();
            Files.writeString(remote.resolve("feature.txt"), "feature\n");
            git.add().addFilepattern("feature.txt").call();
            git.commit().setMessage("feature").call();

            git.checkout().setName(initialBranch).call();

            git.checkout().setCreateBranch(true).setName("extra/branch").call();
            Files.writeString(remote.resolve("extra.txt"), "extra\n");
            git.add().addFilepattern("extra.txt").call();
            git.commit().setMessage("extra").call();

            git.checkout().setName(initialBranch).call();
        }

        RunContext runContext = runContextFactory.of();
        Files.writeString(runContext.workingDir().path().resolve("pre-existing.txt"), "trigger fallback\n");

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .branch(Property.ofValue("feature/only"))
            .cloneAllBranches(Property.ofValue(false))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git cloned = Git.open(repoPath.toFile())) {
            assertNotNull(cloned.getRepository().exactRef("refs/remotes/origin/feature/only"));
            assertNull(cloned.getRepository().exactRef("refs/remotes/origin/extra/branch"));
        }
    }

    @Test
    void cloneNoTagsDoesNotFetchTags() throws Exception {
        // Given a local remote with a tag
        Path remote = Files.createTempDirectory("git-remote-");
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remote.resolve("tagged.txt"), "v1\n");
            git.add().addFilepattern("tagged.txt").call();
            git.commit().setMessage("first").call();
            git.tag().setName("v1.0").call();
        }

        RunContext runContext = runContextFactory.of();
        Files.writeString(runContext.workingDir().path().resolve("pre-existing.txt"), "trigger fallback\n");

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .noTags(Property.ofValue(true))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git cloned = Git.open(repoPath.toFile())) {
            assertNull(cloned.getRepository().findRef("refs/tags/v1.0"));
        }
    }

    @Test
    void cloneNoTagsMainPath() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-");
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remote.resolve("tagged.txt"), "v1\n");
            git.add().addFilepattern("tagged.txt").call();
            git.commit().setMessage("first").call();
            git.tag().setName("v1.0").call();
        }

        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .noTags(Property.ofValue(true))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git cloned = Git.open(repoPath.toFile())) {
            assertNull(cloned.getRepository().findRef("refs/tags/v1.0"));
        }
    }

    @Test
    void cloneSingleBranchWithoutBranchFailsFast() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-");
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remote.resolve("main.txt"), "main\n");
            git.add().addFilepattern("main.txt").call();
            git.commit().setMessage("main").call();
        }

        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .cloneAllBranches(Property.ofValue(false))
            .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("`branch` must be set"));
    }

    @Test
    void cloneSingleBranchDefaultsToCheckoutBranchWhenNoBranchesConfigured() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-");
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            String initialBranch = git.getRepository().getBranch();

            Files.writeString(remote.resolve("main.txt"), "main\n");
            git.add().addFilepattern("main.txt").call();
            git.commit().setMessage("main").call();

            git.checkout().setCreateBranch(true).setName("feature/only").call();
            Files.writeString(remote.resolve("feature.txt"), "feature\n");
            git.add().addFilepattern("feature.txt").call();
            git.commit().setMessage("feature").call();

            git.checkout().setName(initialBranch).call();
        }

        RunContext runContext = runContextFactory.of();
        Files.writeString(runContext.workingDir().path().resolve("pre-existing.txt"), "trigger fallback\n");

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .branch(Property.ofValue("feature/only"))
            .cloneAllBranches(Property.ofValue(false))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git cloned = Git.open(repoPath.toFile())) {
            assertNotNull(cloned.getRepository().exactRef("refs/remotes/origin/feature/only"));
            assertNull(cloned.getRepository().exactRef("refs/remotes/origin/master"));
            assertNull(cloned.getRepository().exactRef("refs/remotes/origin/main"));
        }
    }

    @Test
    void cloneCommitWithNoTagsDoesNotFetchTagsInPostCloneCheckout() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-");
        Path file = remote.resolve("file.txt");
        String firstCommitSha;
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file, "first\n");
            git.add().addFilepattern("file.txt").call();
            git.commit().setMessage("first").call();
            firstCommitSha = git.getRepository().resolve("HEAD").name();

            git.tag().setName("v1.0").call();

            Files.writeString(file, "second\n");
            git.add().addFilepattern("file.txt").call();
            git.commit().setMessage("second").call();
        }

        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .commit(Property.ofValue(firstCommitSha))
            .noTags(Property.ofValue(true))
            .build();

        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());

        try (Git cloned = Git.open(repoPath.toFile())) {
            assertThat(cloned.getRepository().getFullBranch(), is(firstCommitSha));
            assertNull(cloned.getRepository().findRef("refs/tags/v1.0"));
        }
    }

    @Test
    void cloneTagWithNoTagsFailsFast() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-");
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(remote.resolve("file.txt"), "first\n");
            git.add().addFilepattern("file.txt").call();
            git.commit().setMessage("first").call();
            git.tag().setName("v1.0").call();
        }

        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .tag(Property.ofValue("v1.0"))
            .noTags(Property.ofValue(true))
            .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("`tag` cannot be used with `noTags: true`"));
    }

    @Test
    void cloneExistingRepositoryIncrementalUpdate() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-incremental-");
        Path file1 = remote.resolve("file1.txt");
        Path file2 = remote.resolve("file2.txt");

        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file1, "first\n");
            git.add().addFilepattern("file1.txt").call();
            git.commit().setMessage("first").setSign(false).call();
        }

        RunContext runContext = runContextFactory.of();

        Clone task = Clone.builder()
            .url(Property.ofValue(remote.toUri().toString()))
            .build();

        Clone.Output out1 = task.run(runContext);
        Path repoPath = Path.of(out1.getDirectory());
        assertThat(Files.exists(repoPath.resolve("file1.txt")), is(true));
        assertThat(Files.readString(repoPath.resolve("file1.txt")).trim(), is("first"));
        assertThat(Files.exists(repoPath.resolve("file2.txt")), is(false));

        // Add a second commit to the remote repository
        try (Git git = Git.open(remote.toFile())) {
            Files.writeString(file2, "second\n");
            git.add().addFilepattern("file2.txt").call();
            git.commit().setMessage("second").setSign(false).call();
        }

        // Run clone again on the exact same directory (which already contains a git repo)
        Clone.Output out2 = task.run(runContext);
        assertThat(out2.getDirectory(), is(out1.getDirectory()));
        assertThat(Files.exists(repoPath.resolve("file1.txt")), is(true));
        assertThat(Files.exists(repoPath.resolve("file2.txt")), is(true));
        assertThat(Files.readString(repoPath.resolve("file2.txt")).trim(), is("second"));
    }

    @Test
    void cloneWithCacheProperty() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-cache-");
        Path file1 = remote.resolve("file1.txt");
        Path file2 = remote.resolve("file2.txt");

        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file1, "first\n");
            git.add().addFilepattern("file1.txt").call();
            git.commit().setMessage("first").setSign(false).call();
        }

        // RunContext 1 with cache enabled using real mock context (with namespace & flow)
        Clone task1 = Clone.builder()
            .id("clone-cache-1")
            .type(Clone.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .cache(Property.ofValue(true))
            .build();
        RunContext runContext1 = TestsUtils.mockRunContext(runContextFactory, task1, Map.of());

        Clone.Output out1 = task1.run(runContext1);
        Path repoPath1 = Path.of(out1.getDirectory());
        assertThat(Files.exists(repoPath1.resolve("file1.txt")), is(true));

        // Verify cache file was stored in internal storage
        String objectId = Clone.computeCacheObjectId(remote.toUri().toString(), null);
        var cacheFile1 = runContext1.storage().getCacheFile("git-cache", objectId, null);
        assertThat("Cache file should exist in storage after first clone", cacheFile1.isPresent(), is(true));

        // Add second commit to remote
        try (Git git = Git.open(remote.toFile())) {
            Files.writeString(file2, "second\n");
            git.add().addFilepattern("file2.txt").call();
            git.commit().setMessage("second").setSign(false).call();
        }

        // RunContext 2 (fresh directory) with cache enabled - restores cache and updates to commit 2
        Clone task2 = Clone.builder()
            .id("clone-cache-2")
            .type(Clone.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .cache(Property.ofValue(true))
            .build();
        RunContext runContext2 = TestsUtils.mockRunContext(runContextFactory, task2, Map.of());

        Clone.Output out2 = task2.run(runContext2);
        Path repoPath2 = Path.of(out2.getDirectory());
        assertThat(Files.exists(repoPath2.resolve("file1.txt")), is(true));
        assertThat(Files.exists(repoPath2.resolve("file2.txt")), is(true));
        assertThat(Files.readString(repoPath2.resolve("file2.txt")).trim(), is("second"));

        // RunContext 3: without new commits on remote, cache is restored and kept up-to-date
        Clone task3 = Clone.builder()
            .id("clone-cache-3")
            .type(Clone.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .cache(Property.ofValue(true))
            .build();
        RunContext runContext3 = TestsUtils.mockRunContext(runContextFactory, task3, Map.of());

        Clone.Output out3 = task3.run(runContext3);
        Path repoPath3 = Path.of(out3.getDirectory());
        assertThat(Files.exists(repoPath3.resolve("file1.txt")), is(true));
        assertThat(Files.exists(repoPath3.resolve("file2.txt")), is(true));
        assertThat(Files.readString(repoPath3.resolve("file2.txt")).trim(), is("second"));
    }

    @Test
    void cloneWithCorruptedCache_shouldRecoverGracefully() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-corrupt-");
        Path file1 = remote.resolve("file1.txt");

        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file1, "first\n");
            git.add().addFilepattern("file1.txt").call();
            git.commit().setMessage("first").setSign(false).call();
        }

        Clone task = Clone.builder()
            .id("clone-corrupted-cache")
            .type(Clone.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .cache(Property.ofValue(true))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Seed storage with a corrupt non-zip file
        String objectId = Clone.computeCacheObjectId(remote.toUri().toString(), null);
        Path garbage = runContext.workingDir().createTempFile(".bin");
        Files.writeString(garbage, "NOT_A_VALID_ZIP_ARCHIVE_DATA_GARBAGE");
        runContext.storage().putCacheFile(garbage.toFile(), "git-cache", objectId);

        // Run clone: should detect corrupt cache, clean up, and successfully clone
        Clone.Output out = task.run(runContext);
        Path repoPath = Path.of(out.getDirectory());
        assertThat(Files.exists(repoPath.resolve("file1.txt")), is(true));
        assertThat(Files.readString(repoPath.resolve("file1.txt")).trim(), is("first"));

        // Verify corrupted cache was replaced by a valid cache
        var newCache = runContext.storage().getCacheFile("git-cache", objectId, null);
        assertThat(newCache.isPresent(), is(true));
    }

    @Test
    void cloneShallowCachedThenPinOlderCommit_shouldUnshallowAndSucceed() throws Exception {
        Path remote = Files.createTempDirectory("git-remote-shallow-");
        Path file1 = remote.resolve("file1.txt");
        Path file2 = remote.resolve("file2.txt");
        Path file3 = remote.resolve("file3.txt");

        String commit1Sha;
        try (Git git = Git.init().setDirectory(remote.toFile()).call()) {
            Files.writeString(file1, "commit 1\n");
            git.add().addFilepattern("file1.txt").call();
            commit1Sha = git.commit().setMessage("commit 1").setSign(false).call().name();

            Files.writeString(file2, "commit 2\n");
            git.add().addFilepattern("file2.txt").call();
            git.commit().setMessage("commit 2").setSign(false).call();

            Files.writeString(file3, "commit 3\n");
            git.add().addFilepattern("file3.txt").call();
            git.commit().setMessage("commit 3").setSign(false).call();
        }

        // Run 1: seed cache with default depth: 1 (shallow)
        Clone task1 = Clone.builder()
            .id("clone-shallow-seed")
            .type(Clone.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .cache(Property.ofValue(true))
            .build();
        RunContext runContext1 = TestsUtils.mockRunContext(runContextFactory, task1, Map.of());
        Clone.Output out1 = task1.run(runContext1);
        assertThat(Files.exists(Path.of(out1.getDirectory()).resolve("file3.txt")), is(true));

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        // Run 2: fresh directory with cache enabled, pinning older commit 1
        Clone task2 = Clone.builder()
            .id("clone-pin-older")
            .type(Clone.class.getName())
            .url(Property.ofValue(remote.toUri().toString()))
            .cache(Property.ofValue(true))
            .commit(Property.ofValue(commit1Sha))
            .build();
        RunContext runContext2 = TestsUtils.mockRunContext(runContextFactory, task2, Map.of());
        Clone.Output out2 = task2.run(runContext2);
        Path repoPath2 = Path.of(out2.getDirectory());
        assertThat(Files.exists(repoPath2.resolve("file1.txt")), is(true));
        assertThat(Files.exists(repoPath2.resolve("file2.txt")), is(false));
        assertThat(Files.exists(repoPath2.resolve("file3.txt")), is(false));
        assertThat(Files.readString(repoPath2.resolve("file1.txt")).trim(), is("commit 1"));

        boolean fallbackWarning = logs.stream()
            .anyMatch(l -> l.getMessage() != null && l.getMessage().contains("falling back to normal clone"));
        assertThat("Expected cached update with unshallow instead of fallback to normal clone", fallbackWarning, is(false));

        boolean unshallowLog = logs.stream()
            .anyMatch(l -> l.getMessage() != null && l.getMessage().contains("unshallowing repository"));
        assertThat("Expected unshallowing log to confirm unshallow fetch was executed", unshallowLog, is(true));
    }
}
