package io.kestra.plugin.git;

import io.kestra.core.exceptions.FlowProcessingException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
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
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jgit.api.Git;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.*;
import static java.lang.Integer.MAX_VALUE;

/**
 *
 * @param <T> Resource type
 * @param <O> Output class
 */
@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractSyncTask<T, O extends AbstractSyncTask.Output> extends AbstractCloningTask implements RunnableTask<O> {

    @Schema(
        title = "If `true`, the task will only output modifications without performing any modification to Kestra. If `false` (default), all listed modifications will be applied."
    )
    @Builder.Default
    private Property<Boolean> dryRun = Property.of(false);

    public abstract Property<Boolean> getDelete();

    public abstract Property<String> getGitDirectory();

    public abstract Property<String> fetchedNamespace();

    private Path createGitDirectory(RunContext runContext) throws IllegalVariableEvaluationException {
        String gitDirectoryPath = runContext.render(this.getGitDirectory()).as(String.class).orElse(null);
        Path syncDirectory = runContext.workingDir().resolve(Path.of(gitDirectoryPath));

        if (!Files.exists(syncDirectory)) {
            throw new IllegalArgumentException(String.format(
                "The directory '%s' was not found in the git repository. " +
                    "Verify if the path exists in the specified branch '%s'.",
                gitDirectoryPath,
                runContext.render(this.getBranch()).as(String.class).orElse("main")
            ));
        }

        if (!Files.isDirectory(syncDirectory)) {
            throw new IllegalArgumentException(String.format(
                "The path '%s' exists but is not a directory. " +
                    "SyncFlows task requires a directory containing flow files.",
                gitDirectoryPath
            ));
        }

        syncDirectory.toFile().mkdirs();
        return syncDirectory;
    }

    protected Map<URI, Supplier<InputStream>> gitResourcesContentByUri(Path baseDirectory, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        try (Stream<Path> paths = Files.walk(baseDirectory, runContext.render(this.traverseDirectories()).as(Boolean.class).orElseThrow() ? MAX_VALUE : 1)) {
            Stream<Path> filtered = paths.skip(1);
            KestraIgnore kestraIgnore = new KestraIgnore(baseDirectory);
            filtered = filtered.filter(path -> !kestraIgnore.isIgnoredFile(path.toString(), true));
            filtered = filtered.filter(path -> !path.toString().contains(".git"));

            return filtered.collect(Collectors.toMap(
                gitPath -> URI.create(("/" + baseDirectory.relativize(gitPath) + (gitPath.toFile().isDirectory() ? "/" : "")).replace("\\", "/")),
                throwFunction(path -> throwSupplier(() -> {
                    if (Files.isDirectory(path)) {
                        return null;
                    }
                    return Files.newInputStream(path);
                }))
            ));
        }
    }

    protected Property<Boolean> traverseDirectories() {
        return Property.of(true);
    }

    protected boolean mustKeep(RunContext runContext, T instanceResource) {
        return false;
    }

    protected abstract void deleteResource(RunContext runContext, String renderedNamespace, T instanceResource) throws IOException;

    protected abstract T simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, FlowProcessingException;

    protected abstract T writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException, URISyntaxException, FlowProcessingException;

    protected abstract SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, T resourceBeforeUpdate, T resourceAfterUpdate) throws IOException;

    private URI createDiffFile(RunContext runContext, String renderedNamespace, Map<URI, URI> gitUriByResourceUri, Map<URI, T> beforeUpdateResourcesByUri, Map<URI, T> afterUpdateResourcesByUri, List<T> deletedResources) throws IOException, IllegalVariableEvaluationException {
        File diffFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            List<SyncResult> syncResults = new ArrayList<>();

            String renderedGitDirectory = runContext.render(this.getGitDirectory()).as(String.class).orElse(null);
            if (deletedResources != null) {
                deletedResources.stream()
                    .map(throwFunction(deletedResource -> wrapper(
                        runContext,
                        renderedGitDirectory,
                        renderedNamespace,
                        gitUriByResourceUri.get(this.toUri(renderedNamespace, deletedResource)),
                        deletedResource,
                        null
                    ))).forEach(syncResults::add);
            }

            afterUpdateResourcesByUri.entrySet().stream().flatMap(throwFunction(e -> {
                SyncResult syncResult = wrapper(
                    runContext,
                    renderedGitDirectory,
                    renderedNamespace,
                    gitUriByResourceUri.get(e.getKey()), beforeUpdateResourcesByUri.get(e.getKey()),
                    afterUpdateResourcesByUri.get(e.getKey())
                );
                return syncResult == null ? Stream.empty() : Stream.of(syncResult);
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
        // we add this method to configure ssl to allow self signed certs
        configureEnvironmentWithSsl(runContext);

        GitService gitService = new GitService(this);

        gitService.namespaceAccessGuard(runContext, this.fetchedNamespace());

        Git git = gitService.cloneBranch(runContext, runContext.render(this.getBranch()).as(String.class).orElse(null), this.cloneSubmodules);

        Path localGitDirectory = this.createGitDirectory(runContext);
        Map<URI, Supplier<InputStream>> gitContentByUri = this.gitResourcesContentByUri(localGitDirectory, runContext);

        String renderedNamespace = runContext.render(this.fetchedNamespace()).as(String.class).orElse(null);

        Map<URI, T> beforeUpdateResourcesByUri = this.fetchResources(runContext, renderedNamespace)
            .stream()
            .collect(Collectors.toMap(
            resource -> this.toUri(renderedNamespace, resource),
            Function.identity()
        ));
        Map<URI, URI> gitUriByResourceUri = new HashMap<>();
        Map<URI, T> updatedResourcesByUri = gitContentByUri.entrySet().stream()
            .sorted(Comparator.comparing(e -> StringUtils.countMatches(e.getKey().getPath(), "/")))
            .map(throwFunction(e -> {
                InputStream inputStream = e.getValue().get();
                T resource;
                if (runContext.render(this.dryRun).as(Boolean.class).orElseThrow()) {
                    resource = this.simulateResourceWrite(runContext, renderedNamespace, e.getKey(), inputStream);
                } else {
                    resource = this.writeResource(runContext, renderedNamespace, e.getKey(), inputStream);
                }

                return Pair.of(e.getKey(), resource);
            }))
            .collect(
                HashMap::new,
                (map, pair) -> {
                    URI uri = pair.getLeft();
                    T resource = pair.getRight();
                    URI resourceUri = this.toUri(renderedNamespace, resource);
                    map.put(resourceUri, resource);
                    gitUriByResourceUri.put(resourceUri, uri);
                },
                HashMap::putAll
            );

        List<T> deleted;
        if (runContext.render(this.getDelete()).as(Boolean.class).orElseThrow()) {
            deleted = new ArrayList<>();
            beforeUpdateResourcesByUri.entrySet().stream().filter(e -> !updatedResourcesByUri.containsKey(e.getKey())).forEach(throwConsumer(e -> {
                if (this.mustKeep(runContext, e.getValue())) {
                    return;
                }

                if (!runContext.render(this.dryRun).as(Boolean.class).orElseThrow()) {
                    this.deleteResource(runContext, renderedNamespace, e.getValue());
                }

                deleted.add(e.getValue());
            }));
        } else {
            deleted = null;
        }

        URI diffFileStorageUri = this.createDiffFile(runContext, renderedNamespace, gitUriByResourceUri, beforeUpdateResourcesByUri, updatedResourcesByUri, deleted);

        git.close();

        return output(diffFileStorageUri);
    }

    protected abstract List<T> fetchResources(RunContext runContext, String renderedNamespace) throws IOException, IllegalVariableEvaluationException;

    protected abstract URI toUri(String renderedNamespace, T resource);

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
        OVERWRITTEN,
        UPDATED,
        UNCHANGED
    }
}