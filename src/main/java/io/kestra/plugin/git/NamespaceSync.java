package io.kestra.plugin.git;

import io.kestra.core.exceptions.FlowProcessingException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.serializers.YamlParser;
import io.kestra.core.services.FlowService;
import io.kestra.core.storages.NamespaceFile;
import io.kestra.plugin.git.services.GitService;
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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.lang.Integer.MAX_VALUE;
import static org.eclipse.jgit.transport.RemoteRefUpdate.Status.*;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Unidirectional namespace sync between Kestra and Git.",
    description = "Create/update is driven by 'sourceOfTruth'; delete/keep/fail is driven by 'whenMissingInSource'."
)
@Plugin(
    examples = {
        @Example(
            title = "Sync a namespace using Git as the source of truth (destructive).",
            full = true,
            code = """
                id: git_namespace_sync
                namespace: system
                tasks:
                  - id: sync
                    type: io.kestra.plugin.git.NamespaceSync
                    namespace: system
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
                """
        ),
        @Example(
            title = "Sync a namespace using Kestra as source of truth (additive).",
            full = true,
            code = """
                id: kestra_namespace_sync
                namespace: system
                tasks:
                  - id: sync
                    type: io.kestra.plugin.git.NamespaceSync
                    namespace: system
                    sourceOfTruth: KESTRA
                    whenMissingInSource: KEEP
                    protectedNamespaces:
                      - system
                    url: https://github.com/fdelbrayelle/plugin-git-qa
                    username: fdelbrayelle
                    password: "{{ secret('GITHUB_ACCESS_TOKEN') }}"
                    branch: dev
                    # gitDirectory omitted -> repository root
                    onInvalidSyntax: WARN
                    # dryRun omitted
                """
        )
    }
)
public class NamespaceSync extends AbstractCloningTask implements RunnableTask<NamespaceSync.Output> {

    public enum SourceOfTruth {GIT, KESTRA}

    public enum WhenMissingInSource {DELETE, KEEP, FAIL}

    public enum OnInvalidSyntax {SKIP, WARN, FAIL}

    @Schema(title = "The branch to read from / write to (required).")
    @NotNull
    private Property<String> branch;

    @Schema(
        title = "Subdirectory inside the repo used to store Kestra code and files; if empty, repo root is used.",
        description = """
            This is the base folder in your Git repository where Kestra will look for code and files.
            If you don't set it, the repo root will be used. Inside that folder, Kestra always expects
            a structure like <namespace>/flows, <namespace>/files, etc.

            | gitDirectory | namespace       | Expected Git path                        |
            | ------------ | --------------- | -----------------------------------------|
            | (not set)    | company         | company/flows/my-flow.yaml               |
            | monorepo     | system          | monorepo/system/flows/my-flow.yaml       |
            | projectA     | company.team    | projectA/company.team/flows/my-flow.yaml |"""
    )
    private Property<String> gitDirectory;

    @Schema(title = "Target namespace to sync (required).")
    @NotNull
    private Property<String> namespace;

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

    // Directory names (namespace-first structure: <ns>/<kind>/<id>.yaml)
    private static final String FLOWS_DIR = "flows";
    private static final String FILES_DIR = "files";

    private FlowService flowService(RunContext rc) {
        return ((DefaultRunContext) rc).getApplicationContext().getBean(FlowService.class);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var gitService = new GitService(this);

        var rBranch = runContext.render(this.branch).as(String.class).orElseThrow(() -> new IllegalArgumentException("Branch must be explicitly set."));
        var rNamespace = runContext.render(this.namespace).as(String.class).orElseThrow(() -> new IllegalArgumentException("Namespace must be explicitly set."));

        var flowRepository = ((DefaultRunContext) runContext).getApplicationContext().getBean(FlowRepositoryInterface.class);
        var tenantId = runContext.flowInfo().tenantId();
        var distinctNamespaces = flowRepository.findDistinctNamespace(tenantId);
        if (!distinctNamespaces.contains(rNamespace)) {
            throw new IllegalArgumentException("The namespace does not exist in the '" + tenantId + "' tenant.");
        }

        var rGitDirectory = runContext.render(this.gitDirectory).as(String.class).orElse(null);
        var rSourceOfTruth = runContext.render(this.sourceOfTruth).as(SourceOfTruth.class).orElse(SourceOfTruth.KESTRA);
        var rWhenMissingInSource = runContext.render(this.whenMissingInSource).as(WhenMissingInSource.class).orElse(WhenMissingInSource.DELETE);
        var rDryRun = runContext.render(this.dryRun).as(Boolean.class).orElse(false);
        var rOnInvalidSyntax = runContext.render(this.onInvalidSyntax).as(OnInvalidSyntax.class).orElse(OnInvalidSyntax.FAIL);
        var rProtectedNamespaces = runContext.render(this.protectedNamespaces).asList(String.class);
        var rCommitMessage = runContext.render(this.getCommitMessage()).as(String.class).orElse("Namespace sync from Kestra");

        var git = gitService.cloneBranch(runContext, rBranch, this.cloneSubmodules);

        var repoWorktree = git.getRepository().getWorkTree().toPath();
        var baseDir = (rGitDirectory == null || rGitDirectory.isBlank()) ? repoWorktree : repoWorktree.resolve(rGitDirectory);

        // Read Git content (namespace-first) then filter to target namespace (no recursion)
        GitTree gitFlowsAll = readGitTreeNamespaceFirst(baseDir, FLOWS_DIR);
        GitTree gitFlows = filterTreeByNamespace(gitFlowsAll, rNamespace);

        // Read Git namespace files (<ns>/files/**)
        Map<String, byte[]> gitFiles = readGitNamespaceFiles(baseDir, rNamespace);

        if (rSourceOfTruth == SourceOfTruth.KESTRA) {
            ensureNamespaceSkeletons(baseDir, rNamespace);
        }

        // Fetch Kestra state limited to target namespace only
        KestraState kes = loadKestraState(runContext, rNamespace);
        Map<String, byte[]> nsFiles = listNamespaceFiles(runContext, rNamespace);

        List<DiffLine> diff = new ArrayList<>();
        List<Runnable> apply = new ArrayList<>();

        planFlows(runContext, baseDir, gitFlows, kes, rSourceOfTruth, rWhenMissingInSource, rOnInvalidSyntax, rProtectedNamespaces, rDryRun, diff, apply);
        planNamespaceFiles(runContext, baseDir, rNamespace, gitFiles, nsFiles, rSourceOfTruth, rWhenMissingInSource, rProtectedNamespaces, rDryRun, diff, apply);

        if (!rDryRun) {
            for (Runnable r : apply) r.run();
        }

        String addPattern = (rGitDirectory == null || rGitDirectory.isBlank()) ? "." : rGitDirectory;
        git.add().addFilepattern(addPattern).call();
        AddCommand update = git.add();
        update.setUpdate(true).addFilepattern(addPattern).call();

        String rCommitId = null;
        String rCommitURL = null;
        try {
            if (!rDryRun) {
                PersonIdent author = author(runContext);
                git.commit().setAllowEmpty(false).setMessage(rCommitMessage).setAuthor(author).call();

                Iterable<PushResult> results = this.authentified(git.push(), runContext).call();
                for (PushResult pr : results) {
                    Optional<RemoteRefUpdate.Status> rejection = pr.getRemoteUpdates().stream()
                        .map(RemoteRefUpdate::getStatus)
                        .filter(Arrays.asList(REJECTED_NONFASTFORWARD, REJECTED_NODELETE, REJECTED_REMOTE_CHANGED, REJECTED_OTHER_REASON)::contains)
                        .findFirst();
                    if (rejection.isPresent()) {
                        throw new KestraRuntimeException(pr.getMessages());
                    }
                }

                ObjectId commit = git.getRepository().resolve(Constants.HEAD);
                rCommitId = commit != null ? commit.getName() : null;
                String httpUrl = gitService.getHttpUrl(runContext.render(this.url).as(String.class).orElse(null));
                rCommitURL = buildCommitUrl(httpUrl, rBranch, rCommitId);
            }
        } catch (EmptyCommitException e) {
            // no changes
        }

        URI rDiff = createIonDiff(runContext, git);

        git.close();
        return Output.builder().diff(rDiff).commitId(rCommitId).commitURL(rCommitURL).build();
    }

    // ================================ Planning ==========================================

    private void planFlows(RunContext rc, Path baseDir, GitTree gitTree, KestraState kes,
                           SourceOfTruth rSource, WhenMissingInSource rMissing, OnInvalidSyntax rInvalid,
                           List<String> protectedNs, boolean rDryRun,
                           List<DiffLine> diff, List<Runnable> apply) {
        FlowService fs = flowService(rc);
        Map<String, GitNode> gitByKey = gitTree.nodes;
        Map<String, FlowWithSource> kesByKey = kes.flows;
        String tenant = rc.flowInfo().tenantId();

        Set<String> keys = union(gitByKey.keySet(), kesByKey.keySet());
        for (String key : keys) {
            GitNode g = gitByKey.get(key);
            FlowWithSource k = kesByKey.get(key);
            String ns = namespaceFromKey(key);
            String fileRel = nodeToYamlPath(FLOWS_DIR, g, key);

            if (g != null && k == null) {
                if (rSource == SourceOfTruth.GIT) {
                    diff.add(DiffLine.added(baseDir, fileRel, key, "FLOW"));
                    if (!rDryRun) apply.add(() -> {
                        try {
                            fs.importFlow(tenant, g.rawYaml, false);
                        } catch (FlowProcessingException e) {
                            handleInvalid(rc, rInvalid, "FLOW " + key, e);
                        }
                    });
                } else {
                    switch (rMissing) {
                        case KEEP -> diff.add(DiffLine.unchanged(fileRel, key, "FLOW"));
                        case DELETE -> {
                            if (isProtected(ns, protectedNs)) {
                                rc.logger().warn("Protected namespace, skipping delete in Git for FLOW {}", key);
                                diff.add(DiffLine.unchanged(fileRel, key, "FLOW"));
                            } else {
                                diff.add(DiffLine.deletedGit(fileRel, key, "FLOW"));
                                if (!rDryRun) apply.add(() -> deleteGitFile(baseDir, fileRel));
                            }
                        }
                        case FAIL ->
                            throw new KestraRuntimeException("Sync failed: FLOW missing in KESTRA but present in Git for key " + key);
                    }
                }
            } else if (g == null && k != null) {
                if (rSource == SourceOfTruth.KESTRA) {
                    String rel = fileRelFromKey(FLOWS_DIR, key);
                    diff.add(DiffLine.added(baseDir, rel, key, "FLOW"));
                    if (!rDryRun) apply.add(() -> {
                        ensureNamespaceFolders(baseDir, ns);
                        writeGitFile(baseDir, rel, k.getSource());
                    });
                } else {
                    switch (rMissing) {
                        case KEEP -> diff.add(DiffLine.unchanged(fileRelFromKey(FLOWS_DIR, key), key, "FLOW"));
                        case DELETE -> {
                            if (isProtected(ns, protectedNs)) {
                                rc.logger().warn("Protected namespace, skipping delete for FLOW {}", key);
                                break;
                            }
                            diff.add(DiffLine.deletedKestra(fileRelFromKey(FLOWS_DIR, key), key, "FLOW"));
                            if (!rDryRun) apply.add(() -> deleteFlow(rc, k));
                        }
                        case FAIL ->
                            throw new KestraRuntimeException("Sync failed: FLOW missing in Git but present in KESTRA for key " + key);
                    }
                }
            } else if (g != null) {
                boolean changed = !Objects.equals(normalizeYaml(g.rawYaml), normalizeYaml(k.getSource()));
                if (!changed) {
                    diff.add(DiffLine.unchanged(fileRel, key, "FLOW"));
                    continue;
                }
                if (rSource == SourceOfTruth.GIT) {
                    diff.add(DiffLine.updatedKestra(fileRel, key, "FLOW"));
                    if (!rDryRun) apply.add(() -> {
                        try {
                            fs.importFlow(tenant, g.rawYaml, false);
                        } catch (FlowProcessingException e) {
                            handleInvalid(rc, rInvalid, "FLOW " + key, e);
                        }
                    });
                } else {
                    diff.add(DiffLine.updatedGit(fileRel, key, "FLOW"));
                    if (!rDryRun) apply.add(() -> writeGitFile(baseDir, fileRel, k.getSource()));
                }
            }
        }
    }

    private void planNamespaceFiles(RunContext rc, Path baseDir, String ns, Map<String, byte[]> gitFiles,
                                    Map<String, byte[]> kestraFiles, SourceOfTruth rSource, WhenMissingInSource rMissing,
                                    List<String> protectedNs, boolean rDryRun, List<DiffLine> diff, List<Runnable> apply) {

        Set<String> paths = union(gitFiles.keySet(), kestraFiles.keySet());
        for (String rel : paths) {
            boolean inGit = gitFiles.containsKey(rel);
            boolean inKestra = kestraFiles.containsKey(rel);
            String fileRel = ns + "/" + FILES_DIR + "/" + rel;

            if (inGit && !inKestra) {
                if (rSource == SourceOfTruth.GIT) {
                    diff.add(DiffLine.added(baseDir, fileRel, ns + ":" + rel, "FILE"));
                    if (!rDryRun) apply.add(() -> {
                        putNamespaceFile(rc, ns, rel, gitFiles.get(rel));
                    });
                } else {
                    switch (rMissing) {
                        case KEEP -> diff.add(DiffLine.unchanged(fileRel, ns + ":" + rel, "FILE"));
                        case DELETE -> {
                            if (isProtected(ns, protectedNs)) {
                                rc.logger().warn("Protected namespace, skipping delete in Git for FILE {}:{}", ns, rel);
                                diff.add(DiffLine.unchanged(fileRel, ns + ":" + rel, "FILE"));
                            } else {
                                diff.add(DiffLine.deletedGit(fileRel, ns + ":" + rel, "FILE"));
                                if (!rDryRun) apply.add(() -> deleteGitFile(baseDir, fileRel));
                            }
                        }
                        case FAIL ->
                            throw new KestraRuntimeException("Sync failed: FILE missing in KESTRA but present in Git: " + ns + ":" + rel);
                    }
                }
                continue;
            }

            if (!inGit && inKestra) {
                if (rSource == SourceOfTruth.KESTRA) {
                    diff.add(DiffLine.added(baseDir, fileRel, ns + ":" + rel, "FILE"));
                    if (!rDryRun) apply.add(() -> writeGitBinaryFile(baseDir, fileRel, kestraFiles.get(rel)));
                } else {
                    switch (rMissing) {
                        case KEEP -> diff.add(DiffLine.unchanged(fileRel, ns + ":" + rel, "FILE"));
                        case DELETE -> {
                            if (isProtected(ns, protectedNs)) {
                                rc.logger().warn("Protected namespace, skipping delete for FILE {}:{}", ns, rel);
                                break;
                            }
                            diff.add(DiffLine.deletedKestra(fileRel, ns + ":" + rel, "FILE"));
                            if (!rDryRun) apply.add(() -> deleteNamespaceFile(rc, ns, rel));
                        }
                        case FAIL ->
                            throw new KestraRuntimeException("Sync failed: FILE missing in Git but present in KESTRA: " + ns + ":" + rel);
                    }
                }
                continue;
            }

            // both present
            if (inGit) {
                byte[] g = gitFiles.get(rel);
                byte[] k = kestraFiles.get(rel);
                if (Arrays.equals(g, k)) {
                    diff.add(DiffLine.unchanged(fileRel, ns + ":" + rel, "FILE"));
                } else if (rSource == SourceOfTruth.GIT) {
                    diff.add(DiffLine.updatedKestra(fileRel, ns + ":" + rel, "FILE"));
                    if (!rDryRun) apply.add(() -> putNamespaceFile(rc, ns, rel, g));
                } else { // KESTRA
                    diff.add(DiffLine.updatedGit(fileRel, ns + ":" + rel, "FILE"));
                    if (!rDryRun) apply.add(() -> writeGitBinaryFile(baseDir, fileRel, k));
                }
            }
        }
    }

    // ================================ Helpers ===========================================

    private record GitNode(String namespace, String id, String rawYaml, String relPath) {
    }

    private static class GitTree {
        Map<String, GitNode> nodes = new HashMap<>();
    }

    private record KestraState(Map<String, FlowWithSource> flows) {
    }

    private KestraState loadKestraState(RunContext rc, String rootNs) {
        FlowService fs = flowService(rc);
        String tenant = rc.flowInfo().tenantId();

        Map<String, FlowWithSource> flowsWithSource = fs.findByNamespaceWithSource(tenant, rootNs).stream()
            .collect(Collectors.toMap(f -> key(f.getNamespace(), f.getId()), Function.identity(), (a, b) -> a));

        return new KestraState(flowsWithSource);
    }

    private GitTree readGitTreeNamespaceFirst(Path baseDir, String kind) throws IOException {
        GitTree tree = new GitTree();
        if (baseDir == null || !Files.exists(baseDir)) return tree;

        Pattern p = Pattern.compile("(.+?)/" + Pattern.quote(kind) + "/([^/]+)\\.(ya?ml|yml)$");

        try (Stream<Path> paths = Files.walk(baseDir, MAX_VALUE)) {
            paths.filter(Files::isRegularFile)
                .filter(YamlParser::isValidExtension)
                .forEach(throwConsumer(abs -> {
                    Path relPath = baseDir.relativize(abs);
                    String unix = relPath.toString().replace(File.separatorChar, '/');
                    Matcher m = p.matcher(unix);
                    if (!m.matches()) return;
                    String nsPath = m.group(1);
                    String id = stripExt(new File(m.group(2)).getName());
                    String ns = nsPath.replace('/', '.');
                    String yaml = Files.readString(abs, StandardCharsets.UTF_8);
                    String compositeKey = key(ns, id);
                    tree.nodes.put(compositeKey, new GitNode(ns, id, yaml, unix));
                }));
        }
        return tree;
    }

    private Map<String, byte[]> readGitNamespaceFiles(Path baseDir, String ns) throws IOException {
        Map<String, byte[]> out = new HashMap<>();
        if (baseDir == null || !Files.exists(baseDir)) return out;
        Path nsFilesRoot = baseDir.resolve(ns).resolve(FILES_DIR);
        if (!Files.exists(nsFilesRoot)) return out;

        try (Stream<Path> paths = Files.walk(nsFilesRoot, MAX_VALUE)) {
            paths.filter(Files::isRegularFile)
                .forEach(throwConsumer(p -> {
                    Path rel = nsFilesRoot.relativize(p);
                    byte[] content = Files.readAllBytes(p);
                    String relUnix = rel.toString().replace(File.separatorChar, '/');
                    out.put(relUnix, content);
                }));
        }
        return out;
    }

    private Map<String, byte[]> listNamespaceFiles(RunContext runContext, String ns) {
        try {
            var entries = runContext.storage().namespace(ns).all(true);

            List<String> normalized = entries.stream()
                .map(NamespaceFile::path)
                .map(this::normalizeNsPath)
                .toList();

            Set<String> directories = new HashSet<>();
            for (String p : normalized) {
                String prefix = p + "/";
                for (String q : normalized) {
                    if (!p.equals(q) && q.startsWith(prefix)) {
                        directories.add(p);
                        break;
                    }
                }
            }

            Map<String, byte[]> out = new HashMap<>();
            for (int i = 0; i < entries.size(); i++) {
                String key = normalized.get(i);
                if (directories.contains(key)) continue;
                byte[] bytes = readNamespaceFileBytes(runContext, entries.get(i).uri());
                out.put(key, bytes);
            }

            return out;
        } catch (Exception e) {
            throw new KestraRuntimeException("Unable to list namespace files for " + ns + ": " + e.getMessage(), e);
        }
    }

    private String normalizeNsPath(String path) {
        String p = path.replace("\\", "/");
        if (p.startsWith(FILES_DIR + "/")) {
            return p.substring(FILES_DIR.length() + 1);
        }
        return p;
    }

    private byte[] readNamespaceFileBytes(RunContext runContext, URI uri) {
        try (InputStream in = runContext.storage().getFile(uri)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    private void putNamespaceFile(RunContext runContext, String ns, String rel, byte[] bytes) {
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            runContext.storage().namespace(ns).putFile(Path.of(rel), in);
        } catch (IOException | URISyntaxException e) {
            throw new KestraRuntimeException(e);
        }
    }

    private void deleteNamespaceFile(RunContext runContext, String namespace, String file) {
        try {
            var nsStore = runContext.storage().namespace(namespace);

            try {
                nsStore.delete(Path.of(file));
            } catch (IOException e1) {
                try {
                    nsStore.delete(Path.of("files").resolve(file));
                } catch (IOException e2) {
                    e2.addSuppressed(e1);
                    throw e2;
                }
            }
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    private GitTree filterTreeByNamespace(GitTree src, String rootNs) {
        GitTree out = new GitTree();
        src.nodes.forEach((k, v) -> {
            if (v.namespace.equals(rootNs)) {
                out.nodes.put(k, v);
            }
        });
        return out;
    }

    private void ensureNamespaceSkeletons(Path baseDir, String ns) {
        ensureNamespaceFolders(baseDir, ns);
    }

    private void ensureNamespaceFolders(Path baseDir, String ns) {
        if (baseDir == null) return;
        Path nsRoot = baseDir.resolve(ns);
        for (String d : List.of(FLOWS_DIR, FILES_DIR)) {
            try {
                Files.createDirectories(nsRoot.resolve(d));
            } catch (IOException ignored) {
            }
        }
    }

    private void deleteFlow(RunContext rc, FlowWithSource flow) {
        flowService(rc).delete(flow);
    }

    private static boolean isProtected(String ns, List<String> protectedNs) {
        if (ns == null) return false;
        return protectedNs.stream().anyMatch(p -> p.equals(ns) || ns.startsWith(p + "."));
    }

    private static String normalizeYaml(String yaml) {
        return yaml == null ? null : yaml.replace("\r\n", "\n").trim();
    }

    private static String key(String namespace, String id) {
        return (namespace == null || namespace.isEmpty()) ? id : namespace + ":" + id;
    }

    private static String namespaceFromKey(String key) {
        int idx = key.indexOf(':');
        return idx < 0 ? "" : key.substring(0, idx);
    }

    private static String idFromKey(String key) {
        int idx = key.indexOf(':');
        return idx < 0 ? key : key.substring(idx + 1);
    }

    private static String stripExt(String name) {
        int i = name.lastIndexOf('.');
        return i > 0 ? name.substring(0, i) : name;
    }

    private static <T> Set<T> union(Set<T> a, Set<T> b) {
        Set<T> s = new HashSet<>(a);
        s.addAll(b);
        return s;
    }

    private static String fileRelFromKey(String kind, String key) {
        String ns = namespaceFromKey(key);
        String id = idFromKey(key);
        Path rel = Path.of(ns).resolve(kind).resolve(id + ".yaml");
        return rel.toString().replace(File.separatorChar, '/');
    }

    private static String nodeToYamlPath(String kind, GitNode g, String key) {
        if (g != null && g.relPath != null) return g.relPath;
        return fileRelFromKey(kind, key);
    }

    private void writeGitFile(Path base, String rel, String content) {
        try {
            Path target = base.resolve(rel);
            Files.createDirectories(target.getParent());
            Files.writeString(target, content == null ? "" : content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    private void writeGitBinaryFile(Path base, String rel, byte[] content) {
        try {
            Path target = base.resolve(rel);
            Files.createDirectories(target.getParent());
            Files.write(target, content == null ? new byte[0] : content);
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    private void deleteGitFile(Path base, String rel) {
        try {
            Path target = base.resolve(rel);
            Files.deleteIfExists(target);
        } catch (IOException e) {
            throw new KestraRuntimeException(e);
        }
    }

    private void handleInvalid(RunContext rc, OnInvalidSyntax mode, String what, Exception e) {
        switch (mode) {
            case SKIP -> rc.logger().info("Skipping invalid {}", what);
            case WARN -> rc.logger().warn("{} couldn't be synced due to invalid syntax: {}", what, e.getMessage());
            case FAIL -> throw new KestraRuntimeException("Invalid syntax for " + what + ": " + e.getMessage(), e);
        }
    }

    // ================================ Diff output ========================================

    @Getter
    @AllArgsConstructor
    private static class DiffLine {
        private String file;
        private String key;        // namespace:id or namespace:relative/path for FILE
        private String kind;       // FLOW/FILE
        private String action;     // ADDED/UPDATED/UNCHANGED/UPDATED_GIT/UPDATED_KES/DELETED_GIT/DELETED_KES

        public static DiffLine added(Path base, String file, String key, String kind) {
            return new DiffLine((base == null ? file : base.resolve(file).toString()), key, kind, "ADDED");
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

    private URI createIonDiff(RunContext runContext, Git git) throws IOException, GitAPIException {
        File diffFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (BufferedWriter diffWriter = new BufferedWriter(new FileWriter(diffFile))) {
            try (DiffFormatter diffFormatter = new DiffFormatter(null)) {
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
                    diffWriter.write(JacksonMapper.ofIon().writeValueAsString(row));
                    diffWriter.write("\n");
                }
            }
        }
        return runContext.storage().putFile(diffFile);
    }

    private static String getPath(DiffEntry diffEntry) {
        return diffEntry.getChangeType() == DiffEntry.ChangeType.DELETE ? diffEntry.getOldPath() : diffEntry.getNewPath();
    }

    private PersonIdent author(RunContext runContext) throws IllegalVariableEvaluationException {
        String rName = runContext.render(this.authorName).as(String.class).orElse(runContext.render(this.username).as(String.class).orElse(null));
        String rEmail = runContext.render(this.authorEmail).as(String.class).orElse(null);
        if (rEmail == null || rName == null) return null;
        return new PersonIdent(rName, rEmail);
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
        return Optional.ofNullable(this.commitMessage).orElse(Property.ofValue("Namespace sync from Kestra"));
    }

    // ================================ Output =============================================

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
