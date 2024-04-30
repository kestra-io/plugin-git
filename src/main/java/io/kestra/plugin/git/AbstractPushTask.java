package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.EmptyCommitException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractPushTask<O extends AbstractPushTask.Output> extends AbstractCloningTask implements RunnableTask<O> {
    @PluginProperty(dynamic = true)
    protected String commitMessage;

    @Schema(
        title = "If `true`, the task will only output modifications without pushing any file to Git yet. If `false` (default), all listed files will be pushed to Git immediately."
    )
    @PluginProperty
    @Builder.Default
    private boolean dryRun = false;

    @Schema(
        title = "The commit author email.",
        description = "If null, no author will be set on this commit."
    )
    @PluginProperty(dynamic = true)
    private String authorEmail;

    @Schema(
        title = "The commit author name.",
        description = "If null, the username will be used instead.",
        defaultValue = "`username`"
    )
    @PluginProperty(dynamic = true)
    private String authorName;

    public abstract String getCommitMessage();

    public abstract String getGitDirectory();

    public abstract Object regexes();

    public abstract String fetchedNamespace();

    private Path createGitDirectory(RunContext runContext) throws IllegalVariableEvaluationException {
        Path flowDirectory = runContext.resolve(Path.of(runContext.render(this.getGitDirectory())));
        flowDirectory.toFile().mkdirs();
        return flowDirectory;
    }

    protected abstract Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path baseDirectory, List<String> regexes) throws Exception;

    /**
     * Removes any file from the remote that is no longer present on the instance
     */
    private void deleteOutdatedResources(Git git, Path basePath, Map<Path, Supplier<InputStream>> contentByPath, List<String> regexes) throws IOException, GitAPIException {
        try (Stream<Path> paths = Files.walk(basePath)) {
            Stream<Path> filteredPathsStream = paths.filter(path ->
                    !contentByPath.containsKey(path) &&
                        !path.getFileName().toString().equals(".git") &&
                        !path.equals(basePath)
                );

            if (regexes != null) {
                filteredPathsStream = filteredPathsStream.filter(path -> regexes.stream().anyMatch(regex ->
                    path.getFileName().toString().matches(regex)
                        || path.toString().matches(regex)
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

    private static URI createDiffFile(RunContext runContext, Git git) throws IOException, GitAPIException {
        File diffFile = runContext.tempFile(".ion").toFile();
        try(DiffFormatter diffFormatter = new DiffFormatter(null);
            BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            diffFormatter.setRepository(git.getRepository());

            git.diff().setCached(true).call().stream()
                .sorted(Comparator.comparing(AbstractPushTask::getPath))
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

    private static String getPath(DiffEntry diffEntry) {
        return diffEntry.getChangeType() == DiffEntry.ChangeType.DELETE ? diffEntry.getOldPath() : diffEntry.getNewPath();
    }

    private Output push(Git git, RunContext runContext, GitService gitService) throws Exception {
        Logger logger = runContext.logger();

        String commitURL = null;
        String commitId = null;
        ObjectId commit;
        try {
            String httpUrl = gitService.getHttpUrl(runContext.render(this.url));
            if (this.isDryRun()) {
                logger.info(
                    "Dry run — no changes will be pushed to {} for now until you set the `dryRun` parameter to false",
                    httpUrl
                );
            } else {
                String renderedBranch = runContext.render(this.getBranch());
                logger.info(
                    "Pushing to {} on branch {}",
                    httpUrl,
                    renderedBranch
                );

                String message = runContext.render(this.getCommitMessage());
                commit = git.commit()
                    .setAllowEmpty(false)
                    .setMessage(message)
                    .setAuthor(author(runContext))
                    .call()
                    .getId();
                authentified(git.push(), runContext).call();

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
        String name = Optional.ofNullable(this.authorName).orElse(runContext.render(this.username));
        String authorEmail = this.authorEmail;
        if (authorEmail == null || name == null) {
            return null;
        }

        return new PersonIdent(runContext.render(name), runContext.render(authorEmail));
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
        this.detectPasswordLeaks();

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()), this.cloneSubmodules);

        Path localGitDirectory = this.createGitDirectory(runContext);

        List<String> regexes = Optional.ofNullable(this.regexes())
            .map(regexObject -> regexObject instanceof List<?> ? (List<String>) regexObject : Collections.singletonList((String) regexObject))
            .map(throwFunction(runContext::render))
            .orElse(null);
        Map<Path, Supplier<InputStream>> contentByPath = this.instanceResourcesContentByPath(runContext, localGitDirectory, regexes);

        this.deleteOutdatedResources(git, localGitDirectory, contentByPath, regexes);

        this.writeResourceFiles(contentByPath);

        AddCommand add = git.add();
        add.addFilepattern(runContext.render(this.getGitDirectory()));
        add.call();

        URI diffFileStorageUri = AbstractPushTask.createDiffFile(runContext, git);

        Output pushOutput = this.push(git, runContext, gitService);

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