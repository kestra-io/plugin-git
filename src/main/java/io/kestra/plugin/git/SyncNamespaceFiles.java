package io.kestra.plugin.git;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.plugin.git.shared.AbstractSyncTask;
import io.kestra.sdk.internal.ApiException;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Sync Namespace Files from Git",
    description = "Imports Namespace Files from a Git branch into a Kestra namespace (optionally child namespaces via `includeChildNamespaces`). Can delete files missing in Git, honors `.kestraignore`, and supports dry-run diff output."
)
@Plugin(
    examples = {
        @Example(
            title = "Sync Namespace Files from a Git repository. This flow can run either on a schedule (using the Schedule trigger) or anytime you push a change to a given Git branch (using the Webhook trigger).",
            full = true,
            code = """
                id: sync_from_git
                namespace: company.ops

                tasks:
                  - id: git
                    type: io.kestra.plugin.git.SyncNamespaceFiles
                    namespace: prod
                    gitDirectory: _files
                    delete: true
                    url: https://github.com/kestra-io/flows
                    branch: main
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    dryRun: true

                triggers:
                  - id: every_minute
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/1 * * * *"
                """
        ),
        @Example(
            title = "Sync only the `shared-scripts` folder of a namespace from the root of a Git repository.",
            full = true,
            code = """
                id: sync_shared_scripts
                namespace: company.ops

                tasks:
                  - id: git
                    type: io.kestra.plugin.git.SyncNamespaceFiles
                    namespace: company.ops
                    gitDirectory: "."
                    namespaceDirectory: /shared-scripts
                    url: https://github.com/kestra-io/scripts
                    branch: main
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        ),
        @Example(
            title = "Sync all flows and scripts for selected namespaces from Git to Kestra every full hour. Note that this is a [System Flow](https://kestra.io/docs/concepts/system-flows), so make sure to adjust the Scope to SYSTEM in the UI filter to see this flow or its executions.",
            full = true,
            code = """
                id: git_sync
                namespace: company.ops

                tasks:
                  - id: sync
                    type: io.kestra.plugin.core.flow.Loop
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.SyncFlows
                        targetNamespace: "{{ item.value }}"
                        gitDirectory: "{{'flows/' ~ item.value}}"
                        includeChildNamespaces: false
                        username: anna-geller
                        url: https://github.com/anna-geller/product
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: main
                        dryRun: false

                      - id: scripts
                        type: io.kestra.plugin.git.SyncNamespaceFiles
                        namespace: "{{ item.value }}"
                        gitDirectory: "{{'scripts/' ~ item.value}}"
                        username: anna-geller
                        url: https://github.com/anna-geller/product
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: main
                        dryRun: false

                triggers:
                  - id: every_full_hour
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        )
    }
)
public class SyncNamespaceFiles extends AbstractSyncTask<NamespaceFile, SyncNamespaceFiles.Output> {
    @Schema(
        title = "Branch to sync",
        description = "Defaults to `main`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Target namespace",
        description = "Namespace receiving the files; defaults to the current flow namespace."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<String> namespace = Property.ofExpression("{{ flow.namespace }}");

    @Schema(
        title = "Git directory for Namespace Files",
        description = "Relative path containing files; defaults to `_files`."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> gitDirectory = Property.ofValue("_files");

    @Schema(
        title = "Delete files missing in Git",
        description = "Default false. When true, removes Namespace Files absent from Git."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> delete = Property.ofValue(false);

    @Schema(
        title = "Namespace directory prefix",
        description = """
            Relative path within the target namespace under which every synced destination path is written. \
            Mirrors `gitDirectory`, but on the Kestra namespace side: with `/shared-scripts`, a Git file `foo.py` \
            is written to `<namespace>:/shared-scripts/foo.py` instead of `<namespace>:/foo.py`. Defaults to `/` \
            (namespace root, i.e. no prefix — existing flows are unaffected). Also scopes `delete: true` to \
            Namespace Files already under this prefix, so files elsewhere in the namespace are left untouched \
            even on the very first sync."""
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> namespaceDirectory = Property.ofValue("/");

    @Schema(
        title = "Include child namespaces",
        description = "Default false. When true, a directory under `gitDirectory` named by an existing descendant namespace (full dotted name, e.g. `company.team`) is synced into that namespace, and descendant namespaces are included when `delete` is true."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<Boolean> includeChildNamespaces = Property.ofValue(false);

    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    private transient List<String> resolvedChildNamespaces;

    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    private transient String resolvedNamespaceDirectory;

    @Override
    public Property<String> fetchedNamespace() {
        return this.namespace;
    }

    // Fetched once per run and reused by fetchResources and resolveTarget
    private List<String> childNamespaces(RunContext runContext, String renderedNamespace) throws IOException {
        if (this.resolvedChildNamespaces == null) {
            try {
                this.resolvedChildNamespaces = descendantNamespaces(runContext, runContext.flowInfo().tenantId(), renderedNamespace);
            } catch (ApiException | IllegalVariableEvaluationException e) {
                throw new IOException(e);
            }
        }
        return this.resolvedChildNamespaces;
    }

    // Rendered and normalized once per run and reused by fetchResources and resolveTarget
    private String namespaceDirectoryPrefix(RunContext runContext) throws IOException {
        if (this.resolvedNamespaceDirectory == null) {
            try {
                this.resolvedNamespaceDirectory = normalizeNamespaceDirectory(
                    runContext.render(this.namespaceDirectory).as(String.class).orElse("/")
                );
            } catch (IllegalVariableEvaluationException e) {
                throw new IOException(e);
            }
        }
        return this.resolvedNamespaceDirectory;
    }

    // "/", "" and null all normalize to "" (no prefix); a leading slash is optional on input and forced on output
    private static String normalizeNamespaceDirectory(String rendered) {
        if (rendered == null) {
            return "";
        }
        List<String> segments = Arrays.stream(rendered.trim().split("/"))
            .filter(segment -> !segment.isEmpty())
            .toList();
        if (segments.contains("..")) {
            throw new IllegalArgumentException(
                "Invalid 'namespaceDirectory' value '" + rendered + "': '..' path segments are not allowed."
            );
        }
        return segments.isEmpty() ? "" : "/" + String.join("/", segments);
    }

    private static boolean isUnderNamespaceDirectory(NamespaceFile resource, String prefix) {
        if (prefix.isEmpty()) {
            return true;
        }
        String path = "/" + resource.path();
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    @Override
    protected void deleteResource(RunContext runContext, String renderedNamespace, NamespaceFile namespaceFile) throws IOException {
        runContext.storage().namespace(namespaceFile.namespace()).delete(namespaceFile);
    }

    @Override
    protected NamespaceFile simulateResourceWrite(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        var target = resolveTarget(runContext, renderedNamespace, uri);
        var namespace = runContext.storage().namespace(target.namespace());
        var path = Path.of(target.uri().getPath());
        var existingResource = this.findExistingResource(namespace, target.namespace(), target.uri());

        if (inputStream == null) {
            return existingResource.orElseGet(() -> NamespaceFile.of(target.namespace(), target.uri()));
        }

        if (existingResource.isPresent() && !existingResource.get().isDirectory()) {
            if (this.hasSameContent(namespace, path, inputStream)) {
                return existingResource.get();
            }

            return NamespaceFile.of(target.namespace(), target.uri(), existingResource.get().revision() + 1);
        }

        return NamespaceFile.of(target.namespace(), target.uri());
    }

    @Override
    protected NamespaceFile writeResource(RunContext runContext, String renderedNamespace, URI uri, InputStream inputStream) throws IOException {
        var target = resolveTarget(runContext, renderedNamespace, uri);
        if (inputStream == null && "/".equals(target.uri().getPath())) {
            return NamespaceFile.of(target.namespace());
        }
        var namespace = runContext.storage().namespace(target.namespace());
        var path = Path.of(target.uri().getPath());
        var existingResource = this.findExistingResource(namespace, target.namespace(), target.uri());

        try {
            if (inputStream == null) {
                if (existingResource.isPresent()) {
                    return existingResource.get();
                }
                return namespace.createDirectory(path);
            }

            if (existingResource.isPresent() && !existingResource.get().isDirectory()) {
                var tempGitFile = runContext.workingDir().createTempFile(".namespace-file-sync").toFile().toPath();
                try {
                    Files.copy(inputStream, tempGitFile, StandardCopyOption.REPLACE_EXISTING);
                    if (this.hasSameContent(namespace, path, tempGitFile)) {
                        return existingResource.get();
                    }

                    try (var gitContent = Files.newInputStream(tempGitFile)) {
                        return this.putFile(namespace, path, gitContent);
                    }
                } finally {
                    Files.deleteIfExists(tempGitFile);
                }
            }

            return this.putFile(namespace, path, inputStream);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    private NamespaceFile putFile(Namespace namespace, Path path, InputStream inputStream) throws IOException, URISyntaxException {
        return namespace.putFile(path, inputStream).stream()
            .filter(nf -> !nf.isDirectory())
            .findFirst()
            .orElseThrow();
    }

    private boolean hasSameContent(Namespace namespace, Path path, Path sourceFile) throws IOException {
        try (var sourceFileContent = Files.newInputStream(sourceFile)) {
            return this.hasSameContent(namespace, path, sourceFileContent);
        }
    }

    private boolean hasSameContent(Namespace namespace, Path path, InputStream sourceContent) throws IOException {
        try (var existingContent = namespace.getFileContent(path)) {
            return this.streamsEqual(sourceContent, existingContent);
        }
    }

    private boolean streamsEqual(InputStream first, InputStream second) throws IOException {
        var firstBuffer = new byte[8192];
        var secondBuffer = new byte[8192];

        while (true) {
            // readNBytes blocks until len bytes are read or EOF, unlike read() which may return fewer bytes
            // even when more data is available. Using read() caused false mismatches when comparing streams
            // with different buffering behaviors (e.g. FileInputStream vs storage-backed stream).
            var firstRead = first.readNBytes(firstBuffer, 0, firstBuffer.length);
            var secondRead = second.readNBytes(secondBuffer, 0, secondBuffer.length);
            if (firstRead != secondRead) {
                return false;
            }
            if (firstRead == 0) {
                return true;
            }

            for (var i = 0; i < firstRead; i++) {
                if (firstBuffer[i] != secondBuffer[i]) {
                    return false;
                }
            }
        }
    }

    private Optional<NamespaceFile> findExistingResource(Namespace namespace, String renderedNamespace, URI uri) throws IOException {
        var targetUri = this.toUri(renderedNamespace, NamespaceFile.of(renderedNamespace, uri));
        return namespace.all()
            .stream()
            .filter(resource -> Objects.equals(this.toUri(renderedNamespace, resource), targetUri))
            .findFirst();
    }

    @Override
    protected SyncResult wrapper(RunContext runContext, String renderedGitDirectory, String renderedNamespace, URI resourceUri, NamespaceFile resourceBeforeUpdate,
        NamespaceFile resourceAfterUpdate) {
        SyncState syncState;
        if (resourceUri == null) {
            syncState = SyncState.DELETED;
        } else if (resourceBeforeUpdate == null) {
            syncState = SyncState.ADDED;
        } else if (Objects.equals(resourceBeforeUpdate.uri(), resourceAfterUpdate.uri())) {
            syncState = SyncState.UNCHANGED;
        } else {
            syncState = SyncState.OVERWRITTEN;
        }

        String kestraPath = Optional.ofNullable(
            this.toUri(
                renderedNamespace,
                resourceAfterUpdate == null ? resourceBeforeUpdate : resourceAfterUpdate
            )
        ).map(URI::getPath).orElse(null);
        SyncResult.SyncResultBuilder<?, ?> builder = SyncResult.builder()
            .syncState(syncState)
            .kestraPath(kestraPath);

        if (syncState != SyncState.DELETED) {
            builder.gitPath(renderedGitDirectory + resourceUri);
        }

        return builder.build();
    }

    @Override
    protected List<NamespaceFile> fetchResources(RunContext runContext, String renderedNamespace) throws IOException, IllegalVariableEvaluationException {
        String prefix = this.namespaceDirectoryPrefix(runContext);
        List<NamespaceFile> resources = new ArrayList<>(
            runContext.storage().namespace(renderedNamespace).all().stream()
                .filter(resource -> isUnderNamespaceDirectory(resource, prefix))
                .toList()
        );
        if (runContext.render(this.includeChildNamespaces).as(Boolean.class).orElse(false)) {
            for (String child : childNamespaces(runContext, renderedNamespace)) {
                runContext.storage().namespace(child).all().stream()
                    .filter(resource -> isUnderNamespaceDirectory(resource, prefix))
                    .forEach(resources::add);
            }
        }
        return resources;
    }

    @Override
    protected URI toUri(String renderedNamespace, NamespaceFile resource) {
        if (resource == null) {
            return null;
        }

        boolean hasTrailingSlash = resource.uri().toString().endsWith("/");

        String path = resource.path();
        if (hasTrailingSlash && !path.endsWith("/")) {
            path = path + "/";
        }

        return NamespaceFile.of(resource.namespace(), path, 1).uri();
    }

    private record Target(String namespace, URI uri) {
    }

    // With includeChildNamespaces, a first path segment naming an existing descendant namespace routes the file there;
    // namespaceDirectory is then applied inside whichever namespace the file was routed to.
    private Target resolveTarget(RunContext runContext, String renderedNamespace, URI uri) throws IOException {
        Target routed;
        try {
            if (!runContext.render(this.includeChildNamespaces).as(Boolean.class).orElse(false)) {
                routed = new Target(renderedNamespace, uri);
            } else {
                String raw = uri.toString().replaceFirst("^/+", "");
                int slash = raw.indexOf('/');
                String first = slash < 0 ? null : raw.substring(0, slash);
                if (first == null || !isDescendant(renderedNamespace, first) || !childNamespaces(runContext, renderedNamespace).contains(first)) {
                    routed = new Target(renderedNamespace, uri);
                } else {
                    routed = new Target(first, URI.create(raw.substring(slash)));
                }
            }
        } catch (IllegalVariableEvaluationException e) {
            throw new IOException(e);
        }

        String prefix = this.namespaceDirectoryPrefix(runContext);
        return prefix.isEmpty() ? routed : new Target(routed.namespace(), URI.create(prefix + routed.uri()));
    }

    @Override
    protected Output output(URI diffFileStorageUri) {
        return Output.builder()
            .files(diffFileStorageUri)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractSyncTask.Output {
        @Schema(
            title = "Diff of synced Namespace Files",
            description = "ION file listing per-file sync actions (added, deleted, overwritten)."
        )
        private URI files;

        @Override
        public URI diffFileUri() {
            return this.files;
        }
    }

    @SuperBuilder
    @Getter
    public static class SyncResult extends AbstractSyncTask.SyncResult {
        private String kestraPath;
    }
}
