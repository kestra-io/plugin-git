package io.kestra.plugin.git;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.core.utils.PathMatcherPredicate;
import io.kestra.plugin.git.shared.AbstractPushTask;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push Namespace Files to Git",
    description = "Exports Namespace Files from a Kestra namespace (optionally child namespaces) into `gitDirectory` (default `_files`) and pushes to Git. Branch is created if missing; use `files` globs to narrow the selection and `dryRun` to emit a diff only. Push sequentially to avoid merge conflicts."
)
@Plugin(
    examples = {
        @Example(
            title = "Push all saved Namespace Files from the dev namespace to a Git repository every 15 minutes.",
            full = true,
            code = """
                id: push_to_git
                namespace: company.ops

                tasks:
                  - id: commit_and_push
                    type: io.kestra.plugin.git.PushNamespaceFiles
                    namespace: dev
                    files: "**"
                    gitDirectory: _files
                    url: https://github.com/kestra-io/scripts
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: dev
                    commitMessage: "add namespace files"
                    dryRun: true
                triggers:
                  - id: schedule_push_to_git
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "*/15 * * * *"
                """
        ),
        @Example(
            title = "Push only the `shared-scripts` folder of a namespace to the root of a Git repository.",
            full = true,
            code = """
                id: push_shared_scripts
                namespace: company.ops

                tasks:
                  - id: commit_and_push
                    type: io.kestra.plugin.git.PushNamespaceFiles
                    namespace: company.ops
                    gitDirectory: "."
                    namespaceDirectory: /shared-scripts
                    url: https://github.com/kestra-io/scripts
                    username: git_username
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: main
                    commitMessage: "update shared scripts"
                triggers:
                  - id: schedule_push_to_git
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 * * * *"
                """
        ),
        @Example(
            title = "Release all flows and scripts from selected namespaces to a Git repository every Thursday at 11:00 AM. Adjust the `values` list to include the namespaces for which you want to push your code to Git. This [System Flow](https://kestra.io/docs/concepts/system-flows) will create two commits per namespace: one for the flows and one for the scripts.",
            full = true,
            code = """
                id: git_push
                namespace: company.ops

                tasks:
                  - id: push
                    type: io.kestra.plugin.core.flow.Loop
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.PushFlows
                        sourceNamespace: "{{ item.value }}"
                        gitDirectory: "{{'flows/' ~ item.value}}"
                        includeChildNamespaces: false
                        username: anna-geller
                        url: https://github.com/anna-geller/product
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: main
                        dryRun: false

                      - id: scripts
                        type: io.kestra.plugin.git.PushNamespaceFiles
                        namespace: "{{ item.value }}"
                        gitDirectory: "{{'scripts/' ~ item.value}}"
                        username: anna-geller
                        url: https://github.com/anna-geller/product
                        password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                        branch: main
                        dryRun: false

                triggers:
                  - id: schedule_push_to_git
                    type: io.kestra.plugin.core.trigger.Schedule
                    cron: "0 11 * * 4"
                """
        )
    }
)
public class PushNamespaceFiles extends AbstractPushTask<PushNamespaceFiles.Output> {
    @Schema(
        title = "Branch to push Namespace Files",
        description = "Defaults to `main`; created if absent."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Source namespace",
        description = "Namespace whose files are exported; defaults to the current flow namespace."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<String> namespace = Property.ofExpression("{{ flow.namespace }}");

    @Schema(
        title = "Destination directory",
        description = "Relative path inside the repo; defaults to `_files`. Paths under the namespace are preserved beneath this directory."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> gitDirectory = Property.ofValue("_files");

    @Schema(
        title = "Namespace Files to include",
        description = "Glob pattern(s); defaults to all (`**`). Matches paths relative to the namespace root.",
        defaultValue = "**"

    )
    @PluginProperty(dynamic = true, group = "deprecated")
    private Object files;

    @Schema(
        title = "Namespace directory prefix",
        description = """
            Relative path within the source namespace to export; scopes the pushed subtree to Namespace Files \
            under this path and the prefix is stripped from the destination path in Git, so a Namespace File at \
            `/shared-scripts/foo.py` is pushed as `foo.py` inside `gitDirectory` instead of \
            `shared-scripts/foo.py`. Defaults to `/` (namespace root, i.e. no prefix — existing flows are \
            unaffected). The `files` glob still matches against the full namespace-relative path, independently \
            of this prefix. Also scopes `delete` (default true) to files already under this prefix in \
            `gitDirectory`, so files previously pushed from outside this prefix are left untouched even on the \
            very first scoped push."""
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    private Property<String> namespaceDirectory = Property.ofValue("/");

    @Schema(
        title = "Include child namespaces",
        description = "Default false. When true, also pushes files from descendant namespaces, each under a directory named by its full dotted namespace inside `gitDirectory`."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<Boolean> includeChildNamespaces = Property.ofValue(false);

    @Schema(
        title = "Git commit message",
        defaultValue = "Add files from `namespace` namespace"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(Property.ofValue("Add files from " + this.namespace.toString() + " namespace"));
    }

    @Schema(
        title = "Fail when no files are matched",
        description = "If true, throws when the glob finds no files; otherwise logs and skips."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Override
    public Object globs() {
        return this.files;
    }

    @Override
    public Property<String> fetchedNamespace() {
        return this.namespace;
    }

    @Override
    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path baseDirectory, List<String> globs) throws Exception {

        String renderedNamespace = runContext.render(this.namespace).as(String.class).orElse(null);
        Predicate<Path> matcher = (globs != null) ? PathMatcherPredicate.matches(globs) : (path -> true);
        String prefix = NamespaceDirectories.normalize(runContext.render(this.namespaceDirectory).as(String.class).orElse("/"));
        boolean includeChildren = runContext.render(this.includeChildNamespaces).as(Boolean.class).orElse(false);

        List<String> namespaces = new ArrayList<>();
        namespaces.add(renderedNamespace);
        if (includeChildren) {
            namespaces.addAll(descendantNamespaces(runContext, runContext.flowInfo().tenantId(), renderedNamespace));
        }

        Map<Path, Supplier<InputStream>> filesMap = new HashMap<>();
        boolean anyGlobMatch = false;
        for (String namespaceToPush : namespaces) {
            Namespace storage = runContext.storage().namespace(namespaceToPush);
            Path directory = namespaceToPush.equals(renderedNamespace) ? baseDirectory : baseDirectory.resolve(namespaceToPush);
            for (NamespaceFile nsFile : storage.findAllFilesMatching(matcher)) {
                anyGlobMatch = true;
                String relativeToPrefix = NamespaceDirectories.stripPrefix(nsFile.path(), prefix);
                if (relativeToPrefix == null) {
                    continue;
                }
                filesMap.put(directory.resolve(relativeToPrefix), throwSupplier(() -> storage.getFileContent(Path.of(nsFile.path()))));
            }
        }

        if (runContext.render(errorOnMissing).as(Boolean.class).orElse(false) && filesMap.isEmpty()) {
            throw new KestraRuntimeException(
                anyGlobMatch
                    ? "No Namespace Files found under 'namespaceDirectory' (" + prefix + ") to commit."
                    : "No Namespace Files matched the provided 'files' parameter to commit."
            );
        }

        // AbstractPushTask#deleteOutdatedResources (plugin-git-lib, unmodifiable) walks the whole gitDirectory and
        // stages `git rm` for every already-checked-out file not returned by this method, unaware of
        // `namespaceDirectory`. Re-add files that fall outside the prefix, reading their current content back from
        // disk, so a scoped push with the default delete:true doesn't wipe out everything previously pushed outside
        // the new prefix.
        if (!prefix.isEmpty() && runContext.render(this.getDelete()).as(Boolean.class).orElse(true)) {
            Set<Path> childDirectories = new HashSet<>();
            if (includeChildren) {
                for (String namespaceToPush : namespaces) {
                    if (!namespaceToPush.equals(renderedNamespace)) {
                        childDirectories.add(baseDirectory.resolve(namespaceToPush));
                    }
                }
            }
            for (String namespaceToPush : namespaces) {
                Path directory = namespaceToPush.equals(renderedNamespace) ? baseDirectory : baseDirectory.resolve(namespaceToPush);
                Set<Path> excluded = namespaceToPush.equals(renderedNamespace) ? childDirectories : Set.of();
                preserveFilesOutsidePrefix(directory, excluded, prefix, filesMap);
            }
        }

        return filesMap;
    }

    // Re-adds already-checked-out regular files under `directory` that fall outside `prefix` (e.g. left over from a
    // previous unscoped push), skipping files under `excludedDirectories` (sibling child-namespace directories,
    // handled by their own call) and anything nested in a .git metadata directory (relevant when gitDirectory is
    // the repository root itself).
    private void preserveFilesOutsidePrefix(Path directory, Set<Path> excludedDirectories, String prefix, Map<Path, Supplier<InputStream>> filesMap) throws IOException {
        if (!Files.isDirectory(directory)) {
            return;
        }
        List<Path> existingFiles;
        try (Stream<Path> walk = Files.walk(directory)) {
            existingFiles = walk.filter(Files::isRegularFile).toList();
        }
        for (Path file : existingFiles) {
            if (filesMap.containsKey(file) || excludedDirectories.stream().anyMatch(file::startsWith)) {
                continue;
            }
            Path relative = directory.relativize(file);
            if (isInsideGitMetadataDirectory(relative)) {
                continue;
            }
            String candidatePath = "/" + relative.toString().replace('\\', '/');
            if (!NamespaceDirectories.isUnderPrefix(candidatePath, prefix)) {
                byte[] content = Files.readAllBytes(file);
                filesMap.put(file, () -> new ByteArrayInputStream(content));
            }
        }
    }

    private static boolean isInsideGitMetadataDirectory(Path relative) {
        for (Path segment : relative) {
            if (".git".equals(segment.toString())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Output output(AbstractPushTask.Output pushOutput, URI diffFileStorageUri) {
        return Output.builder()
            .commitId(pushOutput.getCommitId())
            .commitURL(pushOutput.getCommitURL())
            .files(diffFileStorageUri)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output extends AbstractPushTask.Output {
        @Schema(
            title = "A file containing all changes pushed (or not in case of dry run) to Git",
            description = """
                The output format is a ION file with one row per file, each row containing the number of added, deleted, and changed lines.
                A row looks as follows: `{changes:"3",file:"path/to/my/script.py",deletions:"-5",additions:"+10"}`"""
        )
        private URI files;

        @Override
        public URI diffFileUri() {
            return this.files;
        }
    }
}
