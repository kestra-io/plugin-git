package io.kestra.plugin.git;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.PathMatcherPredicate;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.FileInputStream;
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
@NoArgsConstructor
@Getter
@Schema(
    title = "Commit and push Execution Output Files to a Git repository.",
    description = "This task pushes one or more files produced by a task execution directly to Git."
)
@Plugin(
    examples = {
        @Example(
            title = "Push output files generated from task to Git",
            full = true,
            code = """
                id: push_exec_files
                namespace: company.team

                tasks:
                  - id: generate
                    type: io.kestra.plugin.scripts.python.Script
                    taskRunner:
                      type: io.kestra.plugin.core.runner.Process
                    outputFiles:
                      - report.txt
                    script: |
                      with open("report.txt", "w") as f:
                          f.write("Analysis done")

                  - id: push
                    type: io.kestra.plugin.git.PushExecutionFiles
                       files:
                         - "*.csv"
                         - "*.json"
                    gitDirectory: analytics
                    url: https://github.com/company/data-pipeline
                    username: git_user
                    password: "{{ secret('GITHUB_TOKEN') }}"
                    branch: data-reports
                    commitMessage: "Add CSV and JSON reports {{ now() }}"
                """
        ),
        @Example(
            title = "Push and rename execution outputs using filesMap",
            full = true,
            code = """
                id: push_with_map
                namespace: company.logs

                tasks:
                  - id: generate
                    type: io.kestra.plugin.scripts.shell.Script
                    outputFiles:
                      - "run.log"
                    script: |
                      echo "Run completed at $(date)" > run.log

                  - id: push
                    type: io.kestra.plugin.git.PushExecutionFiles
                    filesMap:
                      "run-{{ execution.id }}.log": "{{ outputs.generate.outputFiles['run.log'] }}"
                    gitDirectory: logs
                    url: https://github.com/company/log-archive
                    username: git_user
                    password: "{{ secret('GITHUB_TOKEN') }}"
                    branch: logs
                    commitMessage: "Archive log for run {{ execution.id }}"

                """
        )
    }
)
public class PushExecutionFiles extends AbstractPushTask<PushExecutionFiles.Output> {
    @Schema(
        title = "The branch to which files should be committed and pushed",
        description = "If the branch doesn’t exist yet, it will be created."
    )
    @Builder.Default
    private Property<String> branch = Property.ofValue("main");

    @Schema(
        title = "Directory in the Git repository where files should be pushed",
        description = "Defaults to `_outputs`."
    )
    @Builder.Default
    private Property<String> gitDirectory = Property.ofValue("_outputs");

    @Schema(
        title = "Glob pattern(s) to select execution output files from the working directory",
        description = "If provided, will match files relative to the execution working directory."
    )
    private Object files;

    @Schema(
        title = "Explicit file map of target filename to execution file URI",
        description = "Useful when pushing files from other tasks' outputs, which expose URIs."
    )
    private Object filesMap;

    @Builder.Default
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Schema(
        title = "Git commit message"
    )
    @Override
    public Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(new Property<>("Add execution files"));
    }

    @Override
    public Object globs() {
        return this.files;
    }

    @Override
    public Property<String> fetchedNamespace() {
        return new Property<>("{{ flow.namespace }}");
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<String> renderedGlobs = switch (this.globs()) {
            case List<?> globList -> ((List<String>) globList).stream().map(throwFunction(runContext::render)).toList();
            case String globString -> List.of(runContext.render(globString));
            case null, default -> List.of();
        };

        Map<Path, Supplier<InputStream>> contentByPath = this.instanceResourcesContentByPath(
            runContext,
            runContext.workingDir().path(),
            renderedGlobs
        );


        if (contentByPath.isEmpty()) {
            Boolean failIfMissing = runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false);
            if (failIfMissing) {
                throw new IllegalArgumentException("No files matched the provided patterns: " + this.files);
            } else {
                runContext.logger().info("No files to push, skipping Git operations.");
                return Output.builder()
                    .commitId(null)
                    .commitURL(null)
                    .files(null)
                    .build();
            }
        }

        return super.run(runContext);
    }


    @Override
    @SuppressWarnings("unchecked")
    protected Map<Path, Supplier<InputStream>> instanceResourcesContentByPath(RunContext runContext, Path baseDirectory, List<String> globs) throws Exception {
        Map<Path, Supplier<InputStream>> contentByPath;

        if (filesMap != null) {
            Map<String, Object> readFilesMap;
            if (filesMap instanceof String stringValue) {
                String rendered = runContext.render(stringValue);
                readFilesMap = JacksonMapper.ofJson().readValue(rendered, Map.class);
            } else {
                readFilesMap = (Map<String, Object>) filesMap;
            }
            Map<String, Object> renderedMap = runContext.render(readFilesMap);

            contentByPath = renderedMap.entrySet().stream().collect(Collectors.toMap(
                e -> baseDirectory.resolve(e.getKey()),
                throwFunction(e -> throwSupplier(() -> {
                    URI sourceFileURI = URI.create((String) e.getValue());
                    return runContext.storage().getFile(sourceFileURI);
                }))
            ));
        }
        else if (globs != null && !globs.isEmpty()) {
            Predicate<Path> matcher = PathMatcherPredicate.matches(globs);
            List<Path> localMatches = runContext.workingDir().findAllFilesMatching(globs)
                .stream()
                .filter(matcher)
                .toList();

            if (!localMatches.isEmpty()) {
                contentByPath = localMatches.stream().collect(Collectors.toMap(
                    path -> baseDirectory.resolve(path.toString()),
                    throwFunction(path -> throwSupplier(() -> new FileInputStream(path.toFile())))
                ));
            } else {
                Map<String, Object> outputs = (Map<String, Object>) runContext.getVariables().get("outputs");
                if (outputs != null) {
                    contentByPath = outputs.values().stream()
                        .filter(v -> v instanceof Map && ((Map<?, ?>) v).get("outputFiles") instanceof Map)
                        .flatMap(v -> ((Map<String, Object>) ((Map<?, ?>) v).get("outputFiles")).entrySet().stream())
                        .filter(e -> globs.stream().anyMatch(g -> e.getKey().matches(g.replace("*", ".*"))))
                        .collect(Collectors.toMap(
                            e -> baseDirectory.resolve(e.getKey()),
                            throwFunction(e -> throwSupplier(() -> {
                                URI sourceFileURI = URI.create((String) e.getValue());
                                return runContext.storage().getFile(sourceFileURI);
                            }))
                        ));
                } else {
                    contentByPath = Map.of();
                }
            }
        }
        else {
            Boolean failIfMissing = runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false);
            if (failIfMissing) {
                throw new IllegalArgumentException("Either files or filesMap must be provided");
            } else {
                runContext.logger().warn("No files or filesMap provided — skipping push.");
                return Map.of();
            }
        }

        if (contentByPath.isEmpty()) {
            Boolean failIfMissing = runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false);
            if (failIfMissing) {
                throw new IllegalArgumentException("No files matched the provided patterns: " + globs);
            } else {
                runContext.logger().info("No execution files matched patterns {}, skipping push.", globs);
                return Map.of();
            }
        }

        return contentByPath;
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
            title = "A file containing all changes pushed (or not in case of dry run) to Git"
        )
        private URI files;

        @Override
        public URI diffFileUri() {
            return this.files;
        }
    }
}
