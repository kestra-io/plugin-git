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
import org.eclipse.jgit.api.RmCommand;
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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;
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
        title = "If `true`, the task will only output modifications without pushing any file to Git yet. If `false` (default), all listed files will be pushed to Git immediately."
    )
    @Builder.Default
    private Property<Boolean> dryRun = Property.of(false);

    @Schema(
        title = "The commit author email.",
        description = "If null, no author will be set on this commit."
    )
    private Property<String> authorEmail;

    @Schema(
        title = "The commit author name.",
        description = "If null, the username will be used instead.",
        defaultValue = "`username`"
    )
    private Property<String> authorName;

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
    private void deleteOutdatedResources(Git git, Path basePath, Map<Path, Supplier<InputStream>> contentByPath, List<String> globs) throws IOException, GitAPIException {
        try (Stream<Path> paths = Files.walk(basePath)) {
            Stream<Path> filteredPathsStream = paths.filter(path ->
                !contentByPath.containsKey(path) &&
                    !path.getFileName().toString().equals(".git") &&
                    !path.equals(basePath)
            );

            if (globs != null) {
                List<PathMatcher> matchers = globs.stream().map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob)).toList();
                filteredPathsStream = filteredPathsStream.filter(path -> matchers.stream().anyMatch(matcher ->
                    matcher.matches(path) ||
                        matcher.matches(path.getFileName())
                ));
            }

            List<String> filteredPaths = filteredPathsStream
                .map(path -> git.getRepository().getWorkTree().toPath().relativize(path).toString())
                .toList();
            if (filteredPaths.isEmpty()) {
                return;
            }

            RmCommand rm = git.rm();
            filteredPaths.forEach(rm::addFilepattern);
            rm.call();
        }
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

    private String buildCommitUrl(String httpUrl, String branch, String commitId) {
        if (commitId == null) {
            return null;
        }

        String commitSubroute = httpUrl.contains("bitbucket.org") ? "commits" : "commit";
        String commitUrl = httpUrl + "/" + commitSubroute + "/" + commitId;
        if (commitUrl.contains("azure.com")) {
            commitUrl = commitUrl + "?refName=refs%2Fheads%2F" + branch;
        }

        return commitUrl;
    }

    public O run(RunContext runContext) throws Exception {
        GitService gitService = new GitService(this);

        gitService.namespaceAccessGuard(runContext, this.fetchedNamespace());

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()).as(String.class).orElse(null), this.cloneSubmodules);

        Path localGitDirectory = this.createGitDirectory(runContext);

        List<String> globs = switch (this.globs()) {
            case List<?> globList -> ((List<String>) globList).stream().map(throwFunction(runContext::render)).toList();
            case String globString -> {
                String renderedValue =  runContext.render(globString);
                try {
                    yield MAPPER.readValue(renderedValue, TYPE_REFERENCE);
                } catch (JsonProcessingException e) {
                    yield  Collections.singletonList(renderedValue);
                }
            }
            case null, default -> null;
        };

        Map<Path, Supplier<InputStream>> contentByPath = this.instanceResourcesContentByPath(runContext, localGitDirectory, globs);

        this.deleteOutdatedResources(git, localGitDirectory, contentByPath, globs);

        this.writeResourceFiles(contentByPath);

        AddCommand add = git.add();
        add.addFilepattern(runContext.render(this.getGitDirectory()).as(String.class).orElse(null));
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
            title = "ID of the commit pushed."
        )
        @Nullable
        private String commitId;

        @Schema(
            title = "URL to see what’s included in the commit.",
            description = "Example format for GitHub: https://github.com/username/your_repo/commit/{commitId}."
        )
        @Nullable
        private String commitURL;

        public URI diffFileUri() {
            return null;
        }
    }
}