package io.kestra.plugin.git;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
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
import org.slf4j.Logger;

import java.io.*;
import java.lang.reflect.ParameterizedType;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;

/**
 *
 * @param <S> Service class
 * @param <T> Resource type
 * @param <O> Output class
 */
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractSyncTask<S, T, O extends AbstractSyncTask.Output> extends AbstractCloningTask implements RunnableTask<O> {

    @Schema(
        title = "If `true`, the task will only output modifications without performing any modification to Kestra. If `false` (default), all listed modifications will be applied."
    )
    @PluginProperty
    @Builder.Default
    private boolean dryRun = false;

    public abstract boolean isDelete();

    public abstract String getGitDirectory();

    public abstract String fetchedNamespace();

    private Path createGitDirectory(RunContext runContext) throws IllegalVariableEvaluationException {
        Path syncDirectory = runContext.resolve(Path.of(runContext.render(this.getGitDirectory())));
        syncDirectory.toFile().mkdirs();
        return syncDirectory;
    }

    protected Map<URI, Supplier<InputStream>> gitResourcesContentByUri(Path baseDirectory) throws IOException {
        try (Stream<Path> paths = Files.walk(baseDirectory)) {
            Stream<Path> filtered = paths.skip(1);
            if (!this.traverseDirectories()) {
                filtered = filtered.filter(Files::isRegularFile);
            }
            return filtered.collect(Collectors.toMap(
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

    protected boolean traverseDirectories() {
        return true;
    }

    /**
     * Removes any resource from the instance that is not on Git.
     */
    private List<T> deleteOutdatedResources(RunContext runContext, S service, String renderedNamespace, Set<URI> gitUris, List<T> instanceFilledResources) throws IOException {
        List<T> deleted = new ArrayList<>();
        instanceFilledResources.forEach(throwConsumer(instanceResource -> {
            if (!gitUris.contains(toUri(gitUris, renderedNamespace, instanceResource)) && !this.mustKeep(runContext, instanceResource)) {
                if (!this.dryRun) {
                    this.deleteResource(service, runContext.tenantId(), renderedNamespace, instanceResource);
                }
                deleted.add(instanceResource);
            }
        }));

        return deleted;
    }
    
    protected boolean mustKeep(RunContext runContext, T instanceResource) {
        return false;
    }

    protected abstract void deleteResource(S service, String tenantId, String renderedNamespace, T instanceResource) throws IOException;

    private void writeResources(Logger logger, S service, KestraIgnore kestraIgnore, Path gitDirectory, String tenantId, String renderedNamespace, Map<URI, Supplier<InputStream>> contentByPath) throws IOException {
        contentByPath.entrySet().stream().sorted(Comparator.comparingInt(e -> StringUtils.countMatches(e.getKey().toString(), "/")))
            .filter(e -> !kestraIgnore.isIgnoredFile(gitDirectory + e.getKey().toString(), true))
            .forEach(throwConsumer(e -> this.writeResource(logger, service, tenantId, renderedNamespace, e.getKey(), e.getValue().get())));
    }

    protected abstract void writeResource(Logger logger, S service, String tenantId, String renderedNamespace, URI uri, InputStream inputStream) throws IOException;

    protected abstract SyncResult wrapper(String renderedGitDirectory, String renderedNamespace, URI resourceURI, Supplier<InputStream> gitContent, T resourceBeforeUpdate) throws IOException;

    private URI createDiffFile(RunContext runContext, KestraIgnore kestraIgnore, Path gitDirectory, String renderedNamespace, Map<URI, Supplier<InputStream>> gitContentByUri, Map<URI, T> instanceFilledResourcesByUri, List<T> deletedResources) throws IOException, IllegalVariableEvaluationException {
        File diffFile = runContext.tempFile(".ion").toFile();

        try (BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            List<SyncResult> syncResults = new ArrayList<>();

            String renderedGitDirectory = runContext.render(this.getGitDirectory());
            if (deletedResources != null) {
                deletedResources.stream().map(throwFunction(deletedResource -> wrapper(renderedGitDirectory, renderedNamespace, toUri(gitContentByUri.keySet(), renderedNamespace, deletedResource), null, deletedResource))).forEach(syncResults::add);
            }

            gitContentByUri.entrySet().stream()
                .filter(e -> !kestraIgnore.isIgnoredFile(gitDirectory + e.getKey().toString(), true))
                .flatMap(throwFunction(e -> {
                    SyncResult wrapper = wrapper(renderedGitDirectory, renderedNamespace, e.getKey(), e.getValue(), instanceFilledResourcesByUri.get(e.getKey()));
                    return wrapper == null ? Stream.empty() : Stream.of(wrapper);
                })).forEach(syncResults::add);

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
        this.detectPasswordLeaks();
        GitService gitService = new GitService(this);

        gitService.namespaceAccessGuard(runContext, this.fetchedNamespace());

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()), this.cloneSubmodules);

        Path localGitDirectory = this.createGitDirectory(runContext);
        Map<URI, Supplier<InputStream>> gitContentByUri = this.gitResourcesContentByUri(localGitDirectory);
        Set<URI> gitUris = gitContentByUri.keySet();


        String renderedNamespace = runContext.render(this.fetchedNamespace());
        @SuppressWarnings("unchecked")
        S service = runContext.getApplicationContext().getBean((Class<S>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
        List<T> instanceFilledResources = this.instanceFilledResources(service, runContext.tenantId(), renderedNamespace);
        List<T> deletedResources = null;
        if (this.isDelete()) {
            deletedResources = this.deleteOutdatedResources(
                runContext,
                service,
                renderedNamespace,
                gitUris,
                instanceFilledResources
            );
        }

        KestraIgnore kestraIgnore = new KestraIgnore(localGitDirectory);
        if (!this.dryRun) {
            this.writeResources(runContext.logger(), service, kestraIgnore, localGitDirectory, runContext.tenantId(), renderedNamespace, gitContentByUri);
        }

        Map<URI, T> instanceFilledResourcesByUri = instanceFilledResources.stream().collect(Collectors.toMap(resource -> this.toUri(gitUris, renderedNamespace, resource), Function.identity()));
        URI diffFileStorageUri = this.createDiffFile(runContext, kestraIgnore, localGitDirectory, renderedNamespace, gitContentByUri, instanceFilledResourcesByUri, deletedResources);

        git.close();

        return output(diffFileStorageUri);
    }

    protected abstract List<T> instanceFilledResources(S service, String tenantId, String renderedNamespace) throws IOException;

    protected abstract URI toUri(Set<URI> gitUris, String renderedNamespace, T resource);

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