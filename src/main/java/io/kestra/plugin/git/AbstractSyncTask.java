package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.NamespaceFilesService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.KestraIgnore;
import io.kestra.plugin.git.services.GitService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jgit.api.Git;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractSyncTask<O extends AbstractSyncTask.Output> extends AbstractCloningTask implements RunnableTask<O> {

    @Schema(
            title = "If `true`, the task will only output modifications without performing any modification to Kestra. If `false` (default), all listed modifications will be applied."
    )
    @PluginProperty
    @Builder.Default
    private boolean dryRun = false;

    @Schema(
            title = "If `true`, any resource available in Kestra but not present in the gitDirectory will be deleted"
    )
    @PluginProperty
    @Builder.Default
    private boolean delete = false;

    public abstract String getGitDirectory();

    public abstract String fetchedNamespace();

    private Path createGitDirectory(RunContext runContext) throws IllegalVariableEvaluationException {
        Path flowDirectory = runContext.resolve(Path.of(runContext.render(this.getGitDirectory())));
        flowDirectory.toFile().mkdirs();
        return flowDirectory;
    }

    protected Map<URI, Supplier<InputStream>> gitResourcesContentByUri(Path baseDirectory) throws IOException {
        try (Stream<Path> paths = Files.walk(baseDirectory)) {
            return paths.skip(1).collect(Collectors.toMap(
                    gitPath -> URI.create("/" + baseDirectory.relativize(gitPath) + (gitPath.toFile().isDirectory() ? "/" : "")),
                    throwFunction(path -> throwSupplier(() -> {
                        if (Files.isDirectory(path)) {
                            return null;
                        }
                        return Files.newInputStream(path);
                    }))
            ));
        }
    }

    /**
     * Removes any resource from the instance that is not on Git.
     */
    private List<URI> deleteOutdatedResources(NamespaceFilesService namespaceFilesService, String tenantId, String renderedNamespace, Set<URI> gitUris, List<URI> instanceFilledUris) throws IOException {
        List<URI> deleted = new ArrayList<>();
        instanceFilledUris.forEach(throwConsumer(instanceUri -> {
            if (!gitUris.contains(instanceUri)) {
                if (!this.dryRun) {
                    this.deleteResource(namespaceFilesService, tenantId, renderedNamespace, instanceUri);
                }
                deleted.add(instanceUri);
            }
        }));

        return deleted;
    }

    protected abstract void deleteResource(NamespaceFilesService namespaceFilesService, String tenantId, String renderedNamespace, URI instanceUri) throws IOException;

    private void writeResources(RunContext runContext, KestraIgnore kestraIgnore, Path gitDirectory, String tenantId, String renderedNamespace, Map<URI, Supplier<InputStream>> contentByPath) throws IOException {
        contentByPath.entrySet().stream().sorted(Comparator.comparingInt(e -> StringUtils.countMatches(e.getKey().toString(), "/"))).forEach(throwConsumer(e -> {
            if (!kestraIgnore.isIgnoredFile(gitDirectory + e.getKey().toString(), true)) {
                this.writeResource(runContext, tenantId, renderedNamespace, e.getKey(), e.getValue().get());
            }
        }));
    }

    protected abstract void writeResource(RunContext runContext, String tenantId, String renderedNamespace, URI uri, InputStream inputStream) throws IOException;

    protected abstract SyncResult wrapper(String renderedGitDirectory, String localPath, SyncState syncState);

    private URI createDiffFile(RunContext runContext, KestraIgnore kestraIgnore, Path gitDirectory, Set<URI> gitFilledUris, List<URI> instanceFilledUris, List<URI> deleted) throws IOException, IllegalVariableEvaluationException {
        File diffFile = runContext.tempFile(".ion").toFile();

        try (BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            List<SyncResult> syncResults = new ArrayList<>();

            String renderedGitDirectory = runContext.render(this.getGitDirectory());
            if (deleted != null) {
                deleted.stream().map(throwFunction(path -> wrapper(renderedGitDirectory, path.toString(), SyncState.DELETED))).forEach(syncResults::add);
            }

            gitFilledUris.stream()
                    .filter(uri -> !kestraIgnore.isIgnoredFile(gitDirectory + uri.toString(), true))
                    .map(throwFunction(uri -> wrapper(renderedGitDirectory, uri.toString(), instanceFilledUris.contains(uri) ? SyncState.OVERWRITTEN : SyncState.ADDED)))
                    .forEach(syncResults::add);

            syncResults.stream().sorted((s1, s2) -> {
                        if (s1.getGitPath() == null) {
                            return s2.getGitPath() == null ? 0 : -1;
                        }
                        if (s2.getGitPath() == null) {
                            return 1;
                        }

                        return s1.getGitPath().compareTo(s2.getGitPath());
                    })
                    .map(throwFunction(JacksonMapper.ofIon()::writeValueAsString))
                    .forEach(throwConsumer(syncResultStr -> {
                        diffWriter.write(syncResultStr);
                        diffWriter.write("\n");
                        runContext.logger().debug(syncResultStr);
                    }));
        }

        return runContext.storage().putFile(diffFile);
    }

    public O run(RunContext runContext) throws Exception {
        GitService gitService = new GitService(this);

        gitService.namespaceAccessGuard(runContext, this.fetchedNamespace());
        this.detectPasswordLeaks();

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()), this.cloneSubmodules);

        Path localGitDirectory = this.createGitDirectory(runContext);

        Map<URI, Supplier<InputStream>> gitContentByUri = this.gitResourcesContentByUri(localGitDirectory);
        String renderedNamespace = runContext.render(this.fetchedNamespace());
        List<URI> instanceFilledUris = this.instanceFilledUris(runContext, runContext.tenantId(), renderedNamespace);
        List<URI> deleted = null;
        if (this.delete) {
            deleted = this.deleteOutdatedResources(
                    runContext.getApplicationContext().getBean(NamespaceFilesService.class),
                    runContext.tenantId(),
                    renderedNamespace,
                    gitContentByUri.keySet(),
                    instanceFilledUris
            );
        }

        KestraIgnore kestraIgnore = new KestraIgnore(localGitDirectory);
        if (!this.dryRun) {
            this.writeResources(runContext, kestraIgnore, localGitDirectory, runContext.tenantId(), renderedNamespace, gitContentByUri);
        }

        URI diffFileStorageUri = this.createDiffFile(runContext, kestraIgnore, localGitDirectory, gitContentByUri.keySet(), instanceFilledUris, deleted);

        git.close();

        return output(diffFileStorageUri);
    }

    protected abstract List<URI> instanceFilledUris(RunContext runContext, String tenantId, String renderedNamespace) throws IOException;

    protected abstract O output(URI diffFileStorageUri);

    @SuperBuilder
    @Getter
    public abstract static class Output implements io.kestra.core.models.tasks.Output {
        public abstract URI diffFileUri();
    }

    @SuperBuilder
    @Getter
    public abstract static class SyncResult {
        private String gitPath;
        private SyncState syncState;
    }

    public enum SyncState {
        ADDED,
        DELETED,
        OVERWRITTEN
    }
}