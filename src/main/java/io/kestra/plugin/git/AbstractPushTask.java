package io.kestra.plugin.git;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.plugin.git.services.GitService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.DiffCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.EmptyCommitException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.kestra.core.utils.Rethrow.*;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.eclipse.jgit.transport.RemoteRefUpdate.Status.*;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractPushTask<O extends AbstractPushTask.Output> extends AbstractCloningTask implements RunnableTask<O> {
    private static final List<RemoteRefUpdate.Status> REJECTION_STATUS = Arrays.asList(
        REJECTED_NONFASTFORWARD,
        REJECTED_NODELETE,
        REJECTED_REMOTE_CHANGED,
        REJECTED_OTHER_REASON
    );

    private static final TypeReference<List<String>> TYPE_REFERENCE = new TypeReference<>() {};
    public static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    protected Property<String> commitMessage;

    @Schema(
        title = "Dry run only",
        description = "When true, writes a diff file without pushing. Default false pushes immediately."
    )
    @Builder.Default
    private Property<Boolean> dryRun = Property.ofValue(false);

    @Schema(
        title = "Commit author email",
        description = "If null, no author is set."
    )
    private Property<String> authorEmail;

    @Schema(
        title = "Commit author name",
        description = "Defaults to `username` when empty."
    )
    private Property<String> authorName;

    @Schema(
        title = "Delete removed resources",
        description = "If true (default), removes Git files that no longer exist in Kestra."
    )
    @Builder.Default
    private Property<Boolean> delete = Property.ofValue(true);

    public abstract Property<String> getCommitMessage();

    public abstract Property<String> getGitDirectory();

    public abstract Object globs();

    public abstract Property<String> fetchedNamespace();

    private Path createGitDirectory(RunContext runContext) throws IllegalVariableEvaluationException {
        Path flowDirectory = runContext.workingDir().resolve(Path.of(runContext.render(this.getGitDirectory()).as(String.class).orElse(null)));
        flowDirectory.toFile().mkdirs();
        return flowDirectory;
    }

    protected abstract Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path baseDirectory, List<String> globs) throws Exception;

    /**
     * Removes any file from the remote that is no longer present on the instance
     */
    private void deleteOutdatedResources(Git git, Path basePath, Map<Path, Supplier<InputStream>> contentByPath, List<String> globs, KestraIgnore kestraIgnore) throws IOException, GitAPIException {
        if (!Files.exists(basePath)) return;

        var workTree = git.getRepository().getWorkTree().toPath().toRealPath();
        var baseDir = basePath.toRealPath();
        var prefix = workTree.relativize(baseDir).toString().replace(File.separatorChar, '/');

        var keep = new HashSet<String>();
        for (var k : contentByPath.keySet()) {
            var rel = (prefix.isEmpty() ? baseDir.relativize(k.normalize()) : Path.of(prefix).resolve(baseDir.relativize(k.normalize())))
                .toString().replace(File.separatorChar, '/');
            keep.add(rel);
        }

        var matchers = (globs == null || globs.isEmpty()) ? null
            : globs.stream().map(g -> FileSystems.getDefault().getPathMatcher("glob:" + g)).toArray(PathMatcher[]::new);

        var rm = git.rm();
        var changed = new AtomicBoolean(false);

        Files.walkFileTree(baseDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                // to avoid to visit .git unnecessarily
                return ".git".equals(dir.getFileName().toString())
                    ? FileVisitResult.SKIP_SUBTREE
                    : CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!attrs.isRegularFile()) return CONTINUE;
                Path real = file.toRealPath();
                if (!real.startsWith(workTree)) return CONTINUE;

                Path baseRel = baseDir.relativize(real);
                String rel = toUnix(prefix.isEmpty() ? baseRel : Path.of(prefix).resolve(baseRel));

                String filename = real.getFileName().toString();
                if (".kestraignore".equals(filename)) return CONTINUE;
                if (kestraIgnore != null) {
                    String relToBase = baseDir.relativize(real).toString().replace('\\', '/');
                    if (kestraIgnore.isIgnoredFile(relToBase, false)) return CONTINUE;
                }

                if (keep.contains(rel)) return CONTINUE;

                String gitRel = toUnix(workTree.relativize(real));
                if (matchers != null) {
                    String stem = getStem(filename);
                    if (!matchesAny(matchers, gitRel, filename, stem)) return CONTINUE;
                }
                rm.addFilepattern(gitRel);
                changed.set(true);
                return CONTINUE;
            }
        });

        if (changed.get()) {
            rm.call();
        }
    }

    private static String toUnix(Path path) {
        return path.toString().replace(File.separatorChar, '/');
    }

    private static String getStem(String filename) {
        int dot = filename.lastIndexOf('.');
        return dot > 0 ? filename.substring(0, dot) : filename;
    }

    private static boolean matchesAny(PathMatcher[] matchers, String... candidates) {
        if (matchers == null) return true;
        for (String s : candidates) {
            Path p = Path.of(s);
            for (PathMatcher m : matchers) {
                if (m.matches(p)) return true;
            }
        }
        return false;
    }

    private void writeResourceFiles(Map<Path, Supplier<InputStream>> contentByPath) throws Exception {
        contentByPath.forEach(throwBiConsumer((path, content) -> this.writeResourceFile(path, content.get())));
    }

    protected void writeResourceFile(Path path, InputStream inputStream) throws IOException {
        if (!path.getParent().toFile().exists()) {
            path.getParent().toFile().mkdirs();
        }
        Files.copy(inputStream, path, REPLACE_EXISTING);
    }

    private URI createDiffFile(RunContext runContext, Git git) throws IOException, GitAPIException, IllegalVariableEvaluationException {
        File diffFile = runContext.workingDir().createTempFile(".ion").toFile();
        boolean dryRunValue = runContext.render(this.dryRun).as(Boolean.class).orElseThrow();

        try (DiffFormatter diffFormatter = new DiffFormatter(null);
             BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            diffFormatter.setRepository(git.getRepository());

            DiffCommand diff = git.diff();
            if (dryRunValue) {
                diff = diff.setCached(true);
            } else {
                diff = diff.setOldTree(treeIterator(git, "HEAD~1"))
                    .setNewTree(treeIterator(git, "HEAD"));
            }

            diff.call()
                .stream().sorted(Comparator.comparing(AbstractPushTask::getPath))
                .map(throwFunction(diffEntry -> {
                    EditList editList = diffFormatter.toFileHeader(diffEntry).toEditList();
                    int additions = 0;
                    int deletions = 0;
                    int changes = 0;
                    for (Edit edit : editList) {
                        int modifications = edit.getLengthB() - edit.getLengthA();
                        if (modifications > 0) {
                            additions += modifications;
                        } else if (modifications < 0) {
                            deletions += -modifications;
                        } else {
                            changes += edit.getLengthB();
                        }
                    }


                    return Map.of(
                        "file", AbstractPushTask.getPath(diffEntry),
                        "additions", "+" + additions,
                        "deletions", "-" + deletions,
                        "changes", Integer.toString(changes)
                    );
                }))
                .map(throwFunction(JacksonMapper.ofIon()::writeValueAsString))
                .forEach(throwConsumer(ionDiff -> {
                    diffWriter.write(ionDiff);
                    diffWriter.write("\n");
                    runContext.logger().debug(ionDiff);
                }));

            diffWriter.flush();
        }

        return runContext.storage().putFile(diffFile);
    }

    private static CanonicalTreeParser treeIterator(Git git, String ref) throws IOException {
        try (ObjectReader reader = git.getRepository().newObjectReader()) {
            CanonicalTreeParser treeIter = new CanonicalTreeParser();
            ObjectId oldTree = git.getRepository().resolve(ref + "^{tree}");
            if (oldTree != null) {
                treeIter.reset(reader, oldTree);
            }
            return treeIter;
        }
    }

    private static String getPath(DiffEntry diffEntry) {
        return diffEntry.getChangeType() == DiffEntry.ChangeType.DELETE ? diffEntry.getOldPath() : diffEntry.getNewPath();
    }

    private Output push(Git git, RunContext runContext, GitService gitService) throws Exception {
        Logger logger = runContext.logger();

        String commitURL = null;
        String commitId = null;
        ObjectId commit;
        try {
            String httpUrl = gitService.getHttpUrl(runContext.render(this.url).as(String.class).orElse(null));
            if (runContext.render(this.getDryRun()).as(Boolean.class).orElseThrow()) {
                logger.info(
                    "Dry run — no changes will be pushed to {} for now until you set the `dryRun` parameter to false",
                    httpUrl
                );
            } else {
                String renderedBranch = runContext.render(this.getBranch()).as(String.class).orElse(null);
                logger.info(
                    "Pushing to {} on branch {}",
                    httpUrl,
                    renderedBranch
                );

                String message = runContext.render(this.getCommitMessage()).as(String.class).orElse(null);
                ObjectId head = git.getRepository().resolve(Constants.HEAD);
                commit = git.commit()
                    .setAllowEmpty(false)
                    .setMessage(message)
                    .setAuthor(author(runContext))
                    .call()
                    .getId();
                if (head == null) {
                    git.branchRename().setNewName(renderedBranch).call();
                }
                Iterable<PushResult> pushResults = authentified(git.push(), runContext).call();

                for (PushResult pushResult : pushResults) {
                    Optional<RemoteRefUpdate.Status> pushStatus = pushResult.getRemoteUpdates().stream()
                        .map(RemoteRefUpdate::getStatus)
                        .filter(REJECTION_STATUS::contains)
                        .findFirst();

                    if (pushStatus.isPresent()) {
                        throw new KestraRuntimeException(pushResult.getMessages());
                    }
                }

                commitId = commit.getName();
                commitURL = buildCommitUrl(httpUrl, renderedBranch, commitId);

                logger.info("Pushed to " + commitURL);
            }
        } catch (EmptyCommitException e) {
            logger.info("No changes to commit. Skipping push.");
        }

        return Output.builder()
            .commitURL(commitURL)
            .commitId(commitId)
            .build();
    }

    private PersonIdent author(RunContext runContext) throws IllegalVariableEvaluationException {
        String name = runContext.render(this.authorName).as(String.class).orElse(runContext.render(this.username).as(String.class).orElse(null));
        String email = runContext.render(this.authorEmail).as(String.class).orElse(null);
        if (email == null || name == null) {
            return null;
        }

        return new PersonIdent(name, email);
    }

    public O run(RunContext runContext) throws Exception {
        configureHttpTransport(runContext);
        
        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        GitService gitService = new GitService(this);

        gitService.namespaceAccessGuard(runContext, this.fetchedNamespace());

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()).as(String.class).orElse(null), this.cloneSubmodules);

        Path localGitDirectory = this.createGitDirectory(runContext);

        List<String> globs = switch (this.globs()) {
            case List<?> globList -> ((List<String>) globList).stream().map(throwFunction(runContext::render)).toList();
            case String globString -> {
                String renderedValue = runContext.render(globString);
                try {
                    yield MAPPER.readValue(renderedValue, TYPE_REFERENCE);
                } catch (JsonProcessingException e) {
                    yield Collections.singletonList(renderedValue);
                }
            }
            case null, default -> null;
        };

        Map<Path, Supplier<InputStream>> contentByPath = this.instanceResourcesContentByPath(runContext, localGitDirectory, globs);

        this.writeResourceFiles(contentByPath);

        KestraIgnore kestraIgnore = new KestraIgnore(localGitDirectory);

        Map<Path, Supplier<InputStream>> filteredContentByPath = new LinkedHashMap<>();
        for (Map.Entry<Path, Supplier<InputStream>> e : contentByPath.entrySet()) {
            Path p = e.getKey().normalize();
            String filename = p.getFileName() != null ? p.getFileName().toString() : "";

            if (".kestraignore".equals(filename)) {
                filteredContentByPath.put(e.getKey(), e.getValue());
                continue;
            }

            String rel = localGitDirectory.relativize(p).toString().replace('\\', '/');
            if (!kestraIgnore.isIgnoredFile(rel, false)) {
                filteredContentByPath.put(e.getKey(), e.getValue());
            } else {
                runContext.logger().debug("Skipped ignored file: {}", rel);
            }

        }

        contentByPath = filteredContentByPath;

        boolean rDelete = runContext.render(this.delete).as(Boolean.class).orElse(true);
        if (rDelete) {
            this.deleteOutdatedResources(git, localGitDirectory, contentByPath, globs, kestraIgnore);
        }

        var workTree = git.getRepository().getWorkTree().toPath().toRealPath();

        if (contentByPath.isEmpty()) {
            runContext.logger().info("No content to push - skipping Git operations.");
            git.close();
            return output(Output.builder().build(), null);
        }

        AddCommand add = git.add();

        for (Path p : contentByPath.keySet()) {
            String gitRel = workTree.relativize(p.toRealPath()).toString().replace('\\', '/');
            add.addFilepattern(gitRel);
        }
        add.call();

        Output pushOutput = this.push(git, runContext, gitService);

        URI diffFileStorageUri = this.createDiffFile(runContext, git);

        git.close();

        return output(pushOutput, diffFileStorageUri);
    }

    protected abstract O output(Output pushOutput, URI diffFileStorageUri);

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "ID of the commit pushed"
        )
        @Nullable
        private String commitId;

        @Schema(
            title = "URL to see what’s included in the commit",
            description = "Example format for GitHub: https://github.com/username/your_repo/commit/{commitId}"
        )
        @Nullable
        private String commitURL;

        public URI diffFileUri() {
            return null;
        }
    }
}
