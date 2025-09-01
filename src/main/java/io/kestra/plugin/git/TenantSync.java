package io.kestra.plugin.git;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.git.services.GitService;
import io.kestra.sdk.KestraClient;
import io.kestra.sdk.model.PagedResultsDashboard;
import io.kestra.sdk.model.PagedResultsNamespace;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.EmptyCommitException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteRefUpdate;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.eclipse.jgit.transport.RemoteRefUpdate.Status.*;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Unidirectional tenant sync between Kestra and Git.",
    description = "Synchronizes ALL namespaces, flows, files, and dashboards between Kestra and Git."
)
@Plugin(
    examples = {
        @Example(
            title = "Sync all objects (flows, files, dashboards) under the same tenant than this flow using Git as source of truth",
            full = true,
            code = """
                id: tenant_sync_git
                namespace: system
                tasks:
                  - id: sync
                    type: io.kestra.plugin.git.TenantSync
                    sourceOfTruth: GIT
                    whenMissingInSource: DELETE
                    protectedNamespaces:
                      - system
                    url: https://github.com/fdelbrayelle/plugin-git-qa
                    username: fdelbrayelle
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: main
                    gitDirectory: kestra
                    dryRun: true
                    kestraUrl: "http://localhost:8080"
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                """
        ),
        @Example(
            title = "Sync all objects (flows, files, dashboards) under the same tenant than this flow using Kestra as source of truth",
            full = true,
            code = """
                id: tenant_sync_kestra
                namespace: system
                tasks:
                  - id: sync
                    type: io.kestra.plugin.git.TenantSync
                    sourceOfTruth: KESTRA
                    whenMissingInSource: KEEP
                    url: https://github.com/fdelbrayelle/plugin-git-qa
                    username: fdelbrayelle
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: dev
                    kestraUrl: "http://localhost:8080"
                    auth:
                      username: "{{ secret('KESTRA_USERNAME') }}"
                      password: "{{ secret('KESTRA_PASSWORD') }}"
                """
        )
    }
)
public class TenantSync extends AbstractKestraTask implements RunnableTask<TenantSync.Output> {

    public enum SourceOfTruth {GIT, KESTRA}

    public enum WhenMissingInSource {DELETE, KEEP, FAIL}

    public enum OnInvalidSyntax {SKIP, WARN, FAIL}

    @Schema(title = "The branch to read from / write to (required).")
    @NotNull
    private Property<String> branch;

    @Schema(title = "Subdirectory inside the repo used to store Kestra code and files; if empty, repo root is used.")
    private Property<String> gitDirectory;

    @Schema(title = "Select the source of truth.")
    @Builder.Default
    private Property<SourceOfTruth> sourceOfTruth = Property.ofValue(SourceOfTruth.KESTRA);

    @Schema(title = "Behavior when an object is missing from the selected source of truth.")
    @Builder.Default
    private Property<WhenMissingInSource> whenMissingInSource = Property.ofValue(WhenMissingInSource.DELETE);

    @Schema(title = "Namespaces protected from deletion regardless of policies.")
    @Builder.Default
    private Property<List<String>> protectedNamespaces = Property.ofValue(List.of("system"));

    @Schema(title = "If true, only compute the plan and output a diff without applying changes.")
    @Builder.Default
    private Property<Boolean> dryRun = Property.ofValue(false);

    @Schema(title = "Behavior when encountering invalid syntax while syncing.")
    @Builder.Default
    private Property<OnInvalidSyntax> onInvalidSyntax = Property.ofValue(OnInvalidSyntax.FAIL);

    @Schema(title = "Git commit message when pushing back to Git.")
    private Property<String> commitMessage;

    @Schema(title = "The commit author email.")
    private Property<String> authorEmail;

    @Schema(title = "The commit author name (defaults to username if null).")
    private Property<String> authorName;

    @Schema(title = "Whether to clone submodules")
    protected Property<Boolean> cloneSubmodules;

    // Directory names
    private static final String FLOWS_DIR = "flows";
    private static final String FILES_DIR = "files";
    private static final String DASHBOARDS_DIR = "dashboards";

    @Override
    public Output run(RunContext runContext) throws Exception {
        GitService gitService = new GitService(this);
        KestraClient kestraClient = kestraClient(runContext);

        // Required runtime parameters
        String tenantId = runContext.flowInfo().tenantId();
        String rBranch = runContext.render(this.branch).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Branch must be explicitly set."));
        String rGitDirectory = runContext.render(this.gitDirectory).as(String.class).orElse(null);
        SourceOfTruth rSourceOfTruth = runContext.render(this.sourceOfTruth).as(SourceOfTruth.class)
            .orElse(SourceOfTruth.KESTRA);
        WhenMissingInSource rWhenMissingInSource = runContext.render(this.whenMissingInSource)
            .as(WhenMissingInSource.class).orElse(WhenMissingInSource.DELETE);
        boolean rDryRun = runContext.render(this.dryRun).as(Boolean.class).orElse(false);
        OnInvalidSyntax rOnInvalidSyntax = runContext.render(this.onInvalidSyntax)
            .as(OnInvalidSyntax.class).orElse(OnInvalidSyntax.FAIL);
        List<String> rProtectedNamespaces = runContext.render(this.protectedNamespaces).asList(String.class);
        String rCommitMessage = runContext.render(this.getCommitMessage())
            .as(String.class).orElse("Tenant sync from Kestra");

        // Clone the Git repository
        var git = gitService.cloneBranch(runContext, rBranch, this.cloneSubmodules);
        Path repoWorktree = git.getRepository().getWorkTree().toPath();
        Path baseDir = (rGitDirectory == null || rGitDirectory.isBlank())
            ? repoWorktree
            : repoWorktree.resolve(rGitDirectory);

        // Retrieve all namespaces with pagination
        List<String> namespaces = new ArrayList<>();
        int page = 1;
        int size = 200;
        PagedResultsNamespace result;
        do {
            result = kestraClient.namespaces()
                .searchNamespaces(page, size, false, tenantId, null, null, Map.of());
            result.getResults().forEach(ns -> namespaces.add(ns.getId()));
            page++;
        } while (result.getResults().size() == size);

        List<DiffLine> diffs = new ArrayList<>();
        List<Runnable> apply = new ArrayList<>();

        // Plan synchronization for each namespace
        for (String ns : namespaces) {
            List<FlowWithSource> kestraFlows = fetchFlowsFromKestra(kestraClient, runContext, ns);
            Map<String, byte[]> kestraFiles = listNamespaceFiles(kestraClient, runContext, ns);

            if (kestraFlows.isEmpty() && kestraFiles.isEmpty()) {
                runContext.logger().info("Skipping empty namespace: {}", ns);
                continue;
            }

            planNamespace(
                runContext, kestraClient, baseDir, ns,
                rSourceOfTruth, rWhenMissingInSource,
                rOnInvalidSyntax, rProtectedNamespaces,
                rDryRun, diffs, apply,
                kestraFlows, kestraFiles
            );
        }

        // Plan dashboard synchronization
        planDashboards(
            runContext, kestraClient, baseDir,
            rSourceOfTruth, rWhenMissingInSource,
            rOnInvalidSyntax, rDryRun, diffs, apply
        );

        String addPattern = (rGitDirectory == null || rGitDirectory.isBlank()) ? "." : rGitDirectory;

        String rCommitId = null;
        String rCommitURL = null;

        if (!rDryRun) {
            for (Runnable r : apply) r.run();

            // Stage changes
            git.add().addFilepattern(addPattern).call();
            AddCommand update = git.add();
            update.setUpdate(true).addFilepattern(addPattern).call();

            try {
                // Commit changes
                PersonIdent author = author(runContext);
                git.commit().setAllowEmpty(false).setMessage(rCommitMessage).setAuthor(author).call();

                // Push changes with proper authentication
                Iterable<PushResult> results = this.authentified(git.push(), runContext).call();
                for (PushResult pr : results) {
                    Optional<RemoteRefUpdate.Status> rejection = pr.getRemoteUpdates().stream()
                        .map(RemoteRefUpdate::getStatus)
                        .filter(Arrays.asList(
                            REJECTED_NONFASTFORWARD,
                            REJECTED_NODELETE,
                            REJECTED_REMOTE_CHANGED,
                            REJECTED_OTHER_REASON
                        )::contains)
                        .findFirst();
                    if (rejection.isPresent()) {
                        throw new KestraRuntimeException(pr.getMessages());
                    }
                }

                // Retrieve commit metadata
                ObjectId commit = git.getRepository().resolve(Constants.HEAD);
                rCommitId = commit != null ? commit.getName() : null;
                String httpUrl = gitService.getHttpUrl(runContext.render(this.url).as(String.class).orElse(null));
                rCommitURL = buildCommitUrl(httpUrl, rBranch, rCommitId);

            } catch (EmptyCommitException e) {
                runContext.logger().info("No changes to commit.");
            }
        }

        // Generate diff file for output
        URI diffFile = createIonDiff(runContext, git);
        git.close();

        return Output.builder()
            .diff(diffFile)
            .commitId(rCommitId)
            .commitURL(rCommitURL)
            .build();
    }

    // ======================================================================================
    // Plan synchronization for a specific namespace
    // ======================================================================================
    private void planNamespace(
        RunContext runContext,
        KestraClient kestraClient,
        Path baseDir,
        String namespace,
        SourceOfTruth rSourceOfTruth,
        WhenMissingInSource rWhenMissingInSource,
        OnInvalidSyntax rOnInvalidSyntax,
        List<String> rProtectedNamespaces,
        boolean rDryRun,
        List<DiffLine> diffs,
        List<Runnable> apply,
        List<FlowWithSource> kestraFlows,
        Map<String, byte[]> kestraFiles
    ) throws Exception {

        Path nsRoot = baseDir.resolve(namespace);
        Path flowsDir = nsRoot.resolve(FLOWS_DIR);
        Path filesDir = nsRoot.resolve(FILES_DIR);

        // Retrieve Git state
        var gitFlows = readGitFlows(flowsDir);
        var gitFiles = readGitFiles(filesDir);

        // Plan flows synchronization
        planFlows(runContext, flowsDir, gitFlows, kestraFlows, namespace, rSourceOfTruth,
            rWhenMissingInSource, rOnInvalidSyntax, rProtectedNamespaces, rDryRun, diffs, apply);

        // Plan namespace files synchronization
        planNamespaceFiles(runContext, kestraClient, filesDir, gitFiles, kestraFiles, rSourceOfTruth,
            rWhenMissingInSource, rDryRun, diffs, apply);
    }

    // ======================================================================================
    // List all namespace files stored in Kestra
    // ======================================================================================
    private Map<String, byte[]> listNamespaceFiles(KestraClient kestraClient, RunContext runContext, String ns) {
        try {
            var filesApi = kestraClient.files();
            String tenantId = runContext.flowInfo().tenantId();

            List<String> filePaths = filesApi.searchNamespaceFiles(ns, "", tenantId);

            Map<String, byte[]> out = new HashMap<>();
            for (String path : filePaths) {
                String normalizedPath = normalizeNsPath(path);

                var metadata = filesApi.getFileMetadatas(ns, tenantId, normalizedPath);
                if (metadata.getMetadata() == null || !ns.equals(metadata.getMetadata().get("namespace"))) {
                    continue;
                }

                File file = filesApi.getFileContent(ns, normalizedPath, tenantId);
                try (InputStream is = new FileInputStream(file)) {
                    out.put(normalizedPath, is.readAllBytes());
                }
            }

            return out;
        } catch (Exception e) {
            throw new KestraRuntimeException("Unable to list namespace files for " + ns + ": " + e.getMessage(), e);
        }
    }

    // ======================================================================================
    // Normalize namespace paths
    // ======================================================================================
    private String normalizeNsPath(String path) {
        return path.replace("\\", "/");
    }

    // ======================================================================================
    // Plan flows synchronization between Git and Kestra
    // ======================================================================================
    private void planFlows(
        RunContext runContext,
        Path flowsDir,
        Map<String, String> gitFlows,
        List<FlowWithSource> kestraFlows,
        String namespace,
        SourceOfTruth rSourceOfTruth,
        WhenMissingInSource rWhenMissingInSource,
        OnInvalidSyntax rOnInvalidSyntax,
        List<String> rProtectedNamespaces,
        boolean rDryRun,
        List<DiffLine> diffs,
        List<Runnable> apply
    ) {
        Map<String, String> kestraFlowsMap = kestraFlows.stream()
            .collect(Collectors.toMap(FlowWithSource::getId, FlowWithSource::getSource));

        Set<String> allFlowIds = new HashSet<>();
        allFlowIds.addAll(gitFlows.keySet());
        allFlowIds.addAll(kestraFlowsMap.keySet());

        for (String flowId : allFlowIds) {
            Path flowPath = flowsDir.resolve(flowId + ".yaml");
            String gitYaml = gitFlows.get(flowId);
            String kestraYaml = kestraFlowsMap.get(flowId);

            // Case 1: Flow exists only in Git
            if (gitYaml != null && kestraYaml == null) {
                if (rSourceOfTruth == SourceOfTruth.GIT) {
                    diffs.add(DiffLine.added(flowPath.toString(), flowId, "FLOW"));
                    if (!rDryRun) {
                        apply.add(() -> {
                            try {
                                kestraClient(runContext).flows().importFlows(
                                    runContext.flowInfo().tenantId(),
                                    toTempFile(gitYaml),
                                    Map.of()
                                );
                            } catch (Exception e) {
                                handleInvalid(runContext, rOnInvalidSyntax, "FLOW " + flowId, e);
                            }
                        });
                    }
                } else {
                    switch (rWhenMissingInSource) {
                        case KEEP -> diffs.add(DiffLine.unchanged(flowPath.toString(), flowId, "FLOW"));
                        case DELETE -> {
                            if (isProtected(namespace, rProtectedNamespaces)) {
                                runContext.logger().warn("Protected namespace, skipping delete in Git for FLOW {}", flowId);
                            } else {
                                diffs.add(DiffLine.deletedGit(flowPath.toString(), flowId, "FLOW"));
                                if (!rDryRun) apply.add(() -> deleteGitFile(flowPath));
                            }
                        }
                        case FAIL -> throw new KestraRuntimeException(
                            "Sync failed: FLOW " + flowId + " missing in Kestra but present in Git"
                        );
                    }
                }

                // Case 2: Flow exists only in Kestra
            } else if (gitYaml == null && kestraYaml != null) {
                if (rSourceOfTruth == SourceOfTruth.KESTRA) {
                    diffs.add(DiffLine.added(flowPath.toString(), flowId, "FLOW"));
                    if (!rDryRun) apply.add(() -> writeGitFile(flowPath, kestraYaml));
                } else {
                    switch (rWhenMissingInSource) {
                        case KEEP -> diffs.add(DiffLine.unchanged(flowPath.toString(), flowId, "FLOW"));
                        case DELETE -> {
                            if (isProtected(namespace, rProtectedNamespaces)) {
                                runContext.logger().warn("Protected namespace, skipping delete for FLOW {}", flowId);
                            } else {
                                diffs.add(DiffLine.deletedKestra(flowPath.toString(), flowId, "FLOW"));
                                if (!rDryRun) {
                                    apply.add(() -> {
                                        try {
                                            kestraClient(runContext).flows()
                                                .deleteFlow(namespace, flowId, runContext.flowInfo().tenantId());
                                        } catch (Exception e) {
                                            handleInvalid(runContext, rOnInvalidSyntax, "FLOW " + flowId, e);
                                        }
                                    });
                                }
                            }
                        }
                        case FAIL -> throw new KestraRuntimeException(
                            "Sync failed: FLOW " + flowId + " missing in Git but present in Kestra"
                        );
                    }
                }

                // Case 3: Flow exists in both Git and Kestra
            } else if (gitYaml != null) {
                boolean changed = !normalizeYaml(gitYaml).equals(normalizeYaml(kestraYaml));
                if (!changed) {
                    diffs.add(DiffLine.unchanged(flowPath.toString(), flowId, "FLOW"));
                    continue;
                }

                if (rSourceOfTruth == SourceOfTruth.GIT) {
                    diffs.add(DiffLine.updatedKestra(flowPath.toString(), flowId, "FLOW"));
                    if (!rDryRun) {
                        apply.add(() -> {
                            try {
                                kestraClient(runContext).flows().importFlows(
                                    runContext.flowInfo().tenantId(),
                                    toTempFile(gitYaml),
                                    Map.of()
                                );
                            } catch (Exception e) {
                                handleInvalid(runContext, rOnInvalidSyntax, "FLOW " + flowId, e);
                            }
                        });
                    }
                } else {
                    diffs.add(DiffLine.updatedGit(flowPath.toString(), flowId, "FLOW"));
                    if (!rDryRun) apply.add(() -> writeGitFile(flowPath, kestraYaml));
                }
            }
        }
    }

    // ======================================================================================
    // Plan namespace files synchronization between Git and Kestra
    // ======================================================================================
    private void planNamespaceFiles(
        RunContext runContext,
        KestraClient kestraClient,
        Path filesDir,
        Map<String, byte[]> gitFiles,
        Map<String, byte[]> kestraFiles,
        SourceOfTruth rSourceOfTruth,
        WhenMissingInSource rWhenMissingInSource,
        boolean rDryRun,
        List<DiffLine> diffs,
        List<Runnable> apply
    ) {
        Set<String> allFiles = new HashSet<>();
        allFiles.addAll(gitFiles.keySet());
        allFiles.addAll(kestraFiles.keySet());

        for (String rel : allFiles) {
            Path filePath = filesDir.resolve(rel);
            boolean inGit = gitFiles.containsKey(rel);
            boolean inKestra = kestraFiles.containsKey(rel);

            if (inGit && !inKestra) {
                if (rSourceOfTruth == SourceOfTruth.GIT) {
                    diffs.add(DiffLine.added(filePath.toString(), rel, "FILE"));
                    if (!rDryRun) apply.add(() -> putNamespaceFile(kestraClient, runContext, rel, gitFiles.get(rel)));
                } else {
                    switch (rWhenMissingInSource) {
                        case KEEP -> diffs.add(DiffLine.unchanged(filePath.toString(), rel, "FILE"));
                        case DELETE -> {
                            diffs.add(DiffLine.deletedGit(filePath.toString(), rel, "FILE"));
                            if (!rDryRun) apply.add(() -> deleteGitFile(filePath));
                        }
                        case FAIL -> throw new KestraRuntimeException(
                            "Sync failed: FILE missing in Kestra but present in Git: " + rel
                        );
                    }
                }
                continue;
            }

            if (!inGit && inKestra) {
                if (rSourceOfTruth == SourceOfTruth.KESTRA) {
                    diffs.add(DiffLine.added(filePath.toString(), rel, "FILE"));
                    if (!rDryRun) apply.add(() -> writeGitBinaryFile(filePath, kestraFiles.get(rel)));
                } else {
                    switch (rWhenMissingInSource) {
                        case KEEP -> diffs.add(DiffLine.unchanged(filePath.toString(), rel, "FILE"));
                        case DELETE -> {
                            diffs.add(DiffLine.deletedKestra(filePath.toString(), rel, "FILE"));
                            if (!rDryRun) apply.add(() -> deleteNamespaceFile(kestraClient, runContext, rel));
                        }
                        case FAIL -> throw new KestraRuntimeException(
                            "Sync failed: FILE missing in Git but present in Kestra: " + rel
                        );
                    }
                }
                continue;
            }

            // Present in both Git and Kestra
            if (inGit) {
                byte[] g = gitFiles.get(rel);
                byte[] k = kestraFiles.get(rel);
                if (Arrays.equals(g, k)) {
                    diffs.add(DiffLine.unchanged(filePath.toString(), rel, "FILE"));
                } else if (rSourceOfTruth == SourceOfTruth.GIT) {
                    diffs.add(DiffLine.updatedKestra(filePath.toString(), rel, "FILE"));
                    if (!rDryRun) apply.add(() -> putNamespaceFile(kestraClient, runContext, rel, g));
                } else {
                    diffs.add(DiffLine.updatedGit(filePath.toString(), rel, "FILE"));
                    if (!rDryRun) apply.add(() -> writeGitBinaryFile(filePath, k));
                }
            }
        }
    }

    // ======================================================================================
    // Plan dashboards synchronization between Git and Kestra
    // ======================================================================================
    private void planDashboards(
        RunContext runContext,
        KestraClient kestraClient,
        Path baseDir,
        SourceOfTruth rSourceOfTruth,
        WhenMissingInSource rWhenMissingInSource,
        OnInvalidSyntax rOnInvalidSyntax,
        boolean rDryRun,
        List<DiffLine> diffs,
        List<Runnable> apply
    ) throws Exception {
        Path dashboardsDir = baseDir.resolve(DASHBOARDS_DIR);

        // Get dashboards from Kestra using searchDashboards (pagination)
        Map<String, String> kestraDashboards = fetchDashboardsFromKestra(kestraClient, runContext);

        // Get dashboards from Git
        Map<String, String> gitDashboards = readGitDashboards(dashboardsDir);

        Set<String> allDashboards = new HashSet<>();
        allDashboards.addAll(kestraDashboards.keySet());
        allDashboards.addAll(gitDashboards.keySet());

        for (String dashboardId : allDashboards) {
            Path dashboardPath = dashboardsDir.resolve(dashboardId + ".yaml");
            String gitYaml = gitDashboards.get(dashboardId);
            String kestraYaml = kestraDashboards.get(dashboardId);

            // Dashboard exists only in Git
            if (gitYaml != null && kestraYaml == null) {
                if (rSourceOfTruth == SourceOfTruth.GIT) {
                    diffs.add(DiffLine.added(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                    if (!rDryRun) {
                        apply.add(() -> {
                            try {
                                kestraClient.dashboards().createDashboard(
                                    runContext.flowInfo().tenantId(),
                                    gitYaml
                                );
                            } catch (Exception e) {
                                handleInvalid(runContext, rOnInvalidSyntax, "DASHBOARD " + dashboardId, e);
                            }
                        });
                    }
                } else {
                    switch (rWhenMissingInSource) {
                        case KEEP -> diffs.add(DiffLine.unchanged(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                        case DELETE -> {
                            diffs.add(DiffLine.deletedGit(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                            if (!rDryRun) apply.add(() -> deleteGitFile(dashboardPath));
                        }
                        case FAIL -> throw new KestraRuntimeException(
                            "Sync failed: DASHBOARD missing in Kestra but present in Git: " + dashboardId
                        );
                    }
                }

                // Dashboard exists only in Kestra
            } else if (gitYaml == null && kestraYaml != null) {
                if (rSourceOfTruth == SourceOfTruth.KESTRA) {
                    diffs.add(DiffLine.added(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                    if (!rDryRun) apply.add(() -> writeGitFile(dashboardPath, kestraYaml));
                } else {
                    switch (rWhenMissingInSource) {
                        case KEEP -> diffs.add(DiffLine.unchanged(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                        case DELETE -> {
                            diffs.add(DiffLine.deletedKestra(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                            if (!rDryRun) {
                                apply.add(() -> {
                                    try {
                                        kestraClient.dashboards().deleteDashboard(
                                            dashboardId, runContext.flowInfo().tenantId()
                                        );
                                    } catch (Exception e) {
                                        handleInvalid(runContext, rOnInvalidSyntax, "DASHBOARD " + dashboardId, e);
                                    }
                                });
                            }
                        }
                        case FAIL -> throw new KestraRuntimeException(
                            "Sync failed: DASHBOARD missing in Git but present in Kestra: " + dashboardId
                        );
                    }
                }

                // Dashboard exists in both Git and Kestra
            } else if (gitYaml != null) {
                boolean changed = !normalizeYaml(gitYaml).equals(normalizeYaml(kestraYaml));
                if (!changed) {
                    diffs.add(DiffLine.unchanged(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                    continue;
                }

                if (rSourceOfTruth == SourceOfTruth.GIT) {
                    diffs.add(DiffLine.updatedKestra(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                    if (!rDryRun) {
                        apply.add(() -> {
                            try {
                                kestraClient.dashboards().createDashboard(
                                    runContext.flowInfo().tenantId(),
                                    gitYaml
                                );
                            } catch (Exception e) {
                                handleInvalid(runContext, rOnInvalidSyntax, "DASHBOARD " + dashboardId, e);
                            }
                        });
                    }
                } else {
                    diffs.add(DiffLine.updatedGit(dashboardPath.toString(), dashboardId, "DASHBOARD"));
                    if (!rDryRun) apply.add(() -> writeGitFile(dashboardPath, kestraYaml));
                }
            }
        }
    }

    // ======================================================================================
    // Fetch flows from Kestra via exportFlowsByQuery() and unzip them
    // ======================================================================================
    private List<FlowWithSource> fetchFlowsFromKestra(KestraClient kestraClient, RunContext runContext, String namespace) {
        try {
            // Export all flows from Kestra for the given namespace (including sub-namespaces)
            byte[] zippedFlows = kestraClient.flows().exportFlowsByQuery(
                runContext.flowInfo().tenantId(),
                null,      // ids
                null,      // q
                null,      // sort
                namespace, // filter by namespace (includes children)
                null       // other params
            );

            List<FlowWithSource> flows = new ArrayList<>();

            try (
                var bais = new ByteArrayInputStream(zippedFlows);
                var zis = new java.util.zip.ZipInputStream(bais)
            ) {
                java.util.zip.ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (!entry.getName().endsWith(".yml") && !entry.getName().endsWith(".yaml")) {
                        continue;
                    }

                    String yaml = new String(zis.readAllBytes(), StandardCharsets.UTF_8);

                    io.kestra.core.models.flows.Flow parsed = io.kestra.core.serializers.YamlParser.parse(yaml, io.kestra.core.models.flows.Flow.class);

                    if (!namespace.equals(parsed.getNamespace())) {
                        continue;
                    }

                    flows.add(FlowWithSource.of(parsed, yaml));
                }
            }

            return flows;

        } catch (Exception e) {
            throw new KestraRuntimeException("Failed to export flows from Kestra for namespace " + namespace, e);
        }
    }

    // ======================================================================================
    // Fetch dashboards from Kestra via DashboardsApi + pagination
    // ======================================================================================
    private Map<String, String> fetchDashboardsFromKestra(KestraClient kestraClient, RunContext runContext) {
        try {
            Map<String, String> dashboards = new HashMap<>();

            int page = 1;
            int size = 200;
            PagedResultsDashboard pagedResults;

            do {
                pagedResults = kestraClient.dashboards().searchDashboards(page, size, runContext.flowInfo().tenantId(), null, null);

                pagedResults.getResults().forEach(dash -> {
                    dashboards.put(dash.getTitle(), dash.getSourceCode());
                });

                page++;
            } while (pagedResults.getResults().size() == size);

            return dashboards;

        } catch (Exception e) {
            throw new KestraRuntimeException("Failed to fetch dashboards from Kestra", e);
        }
    }

    // ======================================================================================
    // Read dashboards from Git into memory
    // ======================================================================================
    private Map<String, String> readGitDashboards(Path dashboardsDir) throws IOException {
        Map<String, String> dashboards = new HashMap<>();
        if (!Files.exists(dashboardsDir)) {
            return dashboards;
        }

        try (var paths = Files.walk(dashboardsDir)) {
            paths.filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".yml") || p.toString().endsWith(".yaml"))
                .forEach(p -> {
                    try {
                        String id = p.getFileName().toString().replaceFirst("\\.ya?ml$", "");
                        dashboards.put(id, Files.readString(p, StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to read dashboard from Git: " + p, e);
                    }
                });
        }
        return dashboards;
    }

    // ======================================================================================
    // Read all flows from Git into memory
    // ======================================================================================
    private Map<String, String> readGitFlows(Path flowsDir) throws IOException {
        Map<String, String> flows = new HashMap<>();
        if (!Files.exists(flowsDir)) {
            return flows;
        }

        try (var paths = Files.walk(flowsDir)) {
            paths.filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".yml") || p.toString().endsWith(".yaml"))
                .forEach(p -> {
                    try {
                        String id = p.getFileName().toString().replaceFirst("\\.ya?ml$", "");
                        flows.put(id, Files.readString(p, StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to read flow from Git: " + p, e);
                    }
                });
        }
        return flows;
    }

    // ======================================================================================
    // Read all binary files from Git into memory
    // ======================================================================================
    private Map<String, byte[]> readGitFiles(Path filesDir) throws IOException {
        Map<String, byte[]> files = new HashMap<>();
        if (!Files.exists(filesDir)) {
            return files;
        }

        try (var paths = Files.walk(filesDir)) {
            paths.filter(Files::isRegularFile)
                .forEach(p -> {
                    try {
                        Path rel = filesDir.relativize(p);
                        files.put(rel.toString().replace(File.separatorChar, '/'), Files.readAllBytes(p));
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to read file from Git: " + p, e);
                    }
                });
        }
        return files;
    }

    // ======================================================================================
    // Write YAML file into Git repository
    // ======================================================================================
    private void writeGitFile(Path path, String content) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    // ======================================================================================
    // Write binary file into Git repository
    // ======================================================================================
    private void writeGitBinaryFile(Path path, byte[] content) {
        try {
            Files.createDirectories(path.getParent());
            Files.write(path, content);
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    // ======================================================================================
    // Delete a file from Git repository
    // ======================================================================================
    private void deleteGitFile(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    // ======================================================================================
    // Store a file into Kestra namespace storage
    // ======================================================================================

    private void putNamespaceFile(KestraClient kestraClient, RunContext runContext, String rel, byte[] bytes) {
        try {
            File temp = File.createTempFile("tmp", null);
            Files.write(temp.toPath(), bytes);
            String namespace = runContext.flowInfo().namespace();
            String tenant = runContext.flowInfo().tenantId();

            var filesApi = kestraClient.files();
            var path = Path.of(rel);

            String directory = path.getParent() != null ? path.getParent().toString().replace("\\", "/") : null;
            if (directory != null) {
                filesApi.createNamespaceDirectory(namespace, tenant, directory);
            }

            filesApi.createNamespaceFile(namespace, normalizeNsPath(rel), tenant, temp);
        } catch (Exception e) {
            throw new KestraRuntimeException("Failed to put namespace file: " + rel, e);
        }
    }

    // ======================================================================================
    // Delete a file from Kestra namespace storage
    // ======================================================================================
    private void deleteNamespaceFile(KestraClient kestraClient, RunContext runContext, String rel) {
        try {
            var filesApi = kestraClient.files();
            filesApi.deleteFileDirectory(
                runContext.flowInfo().namespace(),
                normalizeNsPath(rel),
                runContext.flowInfo().tenantId()
            );
        } catch (Exception e) {
            throw new KestraRuntimeException("Failed to delete namespace file: " + rel, e);
        }
    }

    // ======================================================================================
    // Utility: check if a namespace is protected
    // ======================================================================================
    private static boolean isProtected(String ns, List<String> protectedNs) {
        if (ns == null) return false;
        return protectedNs.stream().anyMatch(p -> p.equals(ns) || ns.startsWith(p + "."));
    }

    // ======================================================================================
    // Utility: normalize YAML content for diff comparison
    // ======================================================================================
    private static String normalizeYaml(String yaml) {
        return yaml == null ? null : yaml.replace("\r\n", "\n").trim();
    }

    // ======================================================================================
    // Utility: handle invalid syntax based on configured policy
    // ======================================================================================
    private void handleInvalid(RunContext rc, OnInvalidSyntax mode, String what, Exception e) {
        switch (mode) {
            case SKIP -> rc.logger().info("Skipping invalid {}", what);
            case WARN -> rc.logger().warn("{} couldn't be synced due to invalid syntax: {}", what, e.getMessage());
            case FAIL -> throw new KestraRuntimeException("Invalid syntax for " + what + ": " + e.getMessage(), e);
        }
    }

    // ======================================================================================
    // Create a temporary file from YAML string (for imports)
    // ======================================================================================
    private File toTempFile(String yaml) {
        try {
            File tmp = File.createTempFile("tmp", ".yaml");
            Files.writeString(tmp.toPath(), yaml, StandardCharsets.UTF_8);
            return tmp;
        } catch (IOException e) {
            throw new KestraRuntimeException("Failed to create temp file", e);
        }
    }

    // ======================================================================================
    // Build the commit author information
    // ======================================================================================
    private PersonIdent author(RunContext runContext) {
        try {
            String rName = runContext.render(this.authorName).as(String.class).orElse("Kestra Sync Bot");
            String rEmail = runContext.render(this.authorEmail).as(String.class).orElse("bot@kestra.io");
            return new PersonIdent(rName, rEmail);
        } catch (io.kestra.core.exceptions.IllegalVariableEvaluationException e) {
            runContext.logger().warn("Unable to evaluate authorName or authorEmail, using defaults: {}", e.getMessage());
            return new PersonIdent("Kestra Sync Bot", "bot@kestra.io");
        }
    }

    // ======================================================================================
    // Retrieve the commit ID after Git commit
    // ======================================================================================
    private String getCommitId(Git git) {
        try {
            return git.getRepository().findRef("HEAD").getObjectId().getName();
        } catch (IOException e) {
            throw new KestraRuntimeException("Unable to resolve commit ID", e);
        }
    }

    // ======================================================================================
    // Create ION diff file for audit purposes
    // ======================================================================================
    private URI createIonDiff(RunContext runContext, Git git) throws IOException, GitAPIException {
        File diffFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile));
             DiffFormatter diffFormatter = new DiffFormatter(new ByteArrayOutputStream())) {

            diffFormatter.setRepository(git.getRepository());
            var diff = git.diff().setCached(true).call();

            for (DiffEntry de : diff) {
                EditList editList = diffFormatter.toFileHeader(de).toEditList();
                int additions = 0, deletions = 0, changes = 0;

                for (Edit edit : editList) {
                    int mods = edit.getLengthB() - edit.getLengthA();
                    if (mods > 0) additions += mods;
                    else if (mods < 0) deletions += -mods;
                    else changes += edit.getLengthB();
                }

                Map<String, String> row = Map.of(
                    "file", getPath(de),
                    "additions", "+" + additions,
                    "deletions", "-" + deletions,
                    "changes", Integer.toString(changes)
                );

                diffWriter.write(row.toString());
                diffWriter.newLine();
            }
        }

        return runContext.storage().putFile(diffFile);
    }

    private static String getPath(DiffEntry diffEntry) {
        return diffEntry.getChangeType() == DiffEntry.ChangeType.DELETE
            ? diffEntry.getOldPath()
            : diffEntry.getNewPath();
    }

    // ======================================================================================
    // DiffLine helper class
    // ======================================================================================
    @Getter
    @AllArgsConstructor
    private static class DiffLine {
        private String file;
        private String key;
        private String kind;    // FLOW, FILE, DASHBOARD
        private String action;  // ADDED, UPDATED_GIT, UPDATED_KES, UNCHANGED, DELETED_GIT, DELETED_KES

        public static DiffLine added(String file, String key, String kind) {
            return new DiffLine(file, key, kind, "ADDED");
        }

        public static DiffLine updatedGit(String file, String key, String kind) {
            return new DiffLine(file, key, kind, "UPDATED_GIT");
        }

        public static DiffLine updatedKestra(String file, String key, String kind) {
            return new DiffLine(file, key, kind, "UPDATED_KES");
        }

        public static DiffLine unchanged(String file, String key, String kind) {
            return new DiffLine(file, key, kind, "UNCHANGED");
        }

        public static DiffLine deletedGit(String file, String key, String kind) {
            return new DiffLine(file, key, kind, "DELETED_GIT");
        }

        public static DiffLine deletedKestra(String file, String key, String kind) {
            return new DiffLine(file, key, kind, "DELETED_KES");
        }
    }

    private String buildCommitUrl(String httpUrl, String branch, String commitId) {
        if (commitId == null) return null;
        String commitSubroute = httpUrl.contains("bitbucket.org") ? "commits" : "commit";
        String commitUrl = httpUrl + "/" + commitSubroute + "/" + commitId;
        if (commitUrl.contains("azure.com")) {
            commitUrl = commitUrl + "?refName=refs%2Fheads%2F" + branch;
        }
        return commitUrl;
    }

    private Property<String> getCommitMessage() {
        return Optional.ofNullable(this.commitMessage).orElse(Property.ofValue("Tenant sync from Kestra"));
    }

    // ======================================================================================
    // Output definition
    // ======================================================================================
    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "A file containing all changes applied (or not in case of dry run) to/from Git.")
        private URI diff;

        @Schema(title = "ID of the commit pushed (if any).")
        @Nullable
        private String commitId;

        @Schema(title = "URL to the commit (if any).")
        @Nullable
        private String commitURL;
    }
}
