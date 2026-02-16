package io.kestra.plugin.git;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.Namespace;
import io.kestra.core.utils.PathMatcherPredicate;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push Namespace Files to Git",
    description = "Exports Namespace Files from a single Kestra namespace into `gitDirectory` (default `_files`) and pushes to Git. Branch is created if missing; use `files` globs to narrow the selection and `dryRun` to emit a diff only. Push sequentially to avoid merge conflicts."
)
@Plugin(
    examples = {
        @Example(
            title = "Push all saved Namespace Files from the dev namespace to a Git repository every 15 minutes.",
            full = true,
            code = """
                id: push_to_git
                namespace: system

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
            title = "Release all flows and scripts from selected namespaces to a Git repository every Thursday at 11:00 AM. Adjust the `values` list to include the namespaces for which you want to push your code to Git. This [System Flow](https://kestra.io/docs/concepts/system-flows) will create two commits per namespace: one for the flows and one for the scripts.",
            full = true,
            code = """
                id: git_push
                namespace: system

                tasks:
                  - id: push
                    type: io.kestra.plugin.core.flow.ForEach
                    values: ["company", "company.team", "company.analytics"]
                    tasks:
                      - id: flows
                        type: io.kestra.plugin.git.PushFlows
                        sourceNamespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'flows/' ~ taskrun.value}}"
                        includeChildNamespaces: false

                      - id: scripts
                        type: io.kestra.plugin.git.PushNamespaceFiles
                        namespace: "{{ taskrun.value }}"
                        gitDirectory: "{{'scripts/' ~ taskrun.value}}"

                pluginDefaults:
                  - type: io.kestra.plugin.git
                    values:
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
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Source namespace",
        description = "Namespace whose files are exported; defaults to the current flow namespace."
    )
    @Builder.Default
    private Property<String> namespace = new Property<>("{{ flow.namespace }}");

    @Schema(
        title = "Destination directory",
        description = "Relative path inside the repo; defaults to `_files`. Paths under the namespace are preserved beneath this directory."
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.ofValue("_files");

    @Schema(
        title = "Namespace Files to include",
        description = "Glob pattern(s); defaults to all (`**`). Matches paths relative to the namespace root.",
        defaultValue = "**"

    )
    @PluginProperty(dynamic = true)
    private Object files;

    @Schema(
        title = "Git commit message",
        defaultValue = "Add files from `namespace` namespace"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(new Property<>("Add files from " + this.namespace.toString() + " namespace"));
    }

    @Schema(
        title = "Fail when no files are matched",
        description = "If true, throws when the glob finds no files; otherwise logs and skips."
    )
    @Builder.Default
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

        Namespace storage = runContext.storage().namespace(runContext.render(this.namespace).as(String.class).orElse(null));
        Predicate<Path> matcher = (globs != null) ? PathMatcherPredicate.matches(globs) : (path -> true);

        Map<Path, Supplier<InputStream>> filesMap = storage
            .findAllFilesMatching(matcher)
            .stream()
            .collect(Collectors.toMap(
                nsFile -> baseDirectory.resolve(nsFile.path(false)),
                throwFunction(nsFile -> throwSupplier(() -> storage.getFileContent(Path.of(nsFile.path()))))
            ));

        if (runContext.render(errorOnMissing).as(Boolean.class).orElse(false) && filesMap.isEmpty()) {
            throw new KestraRuntimeException("No Namespace Files matched the provided 'files' parameter to commit.");
        }

        return filesMap;
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
