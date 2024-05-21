package io.kestra.plugin.git.services;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.services.FlowService;
import io.kestra.plugin.git.AbstractGitTask;
import io.kestra.plugin.git.Clone;
import lombok.AllArgsConstructor;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Constants;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.eclipse.jgit.lib.Constants.R_HEADS;

@AllArgsConstructor
public class GitService {
    private static final Pattern SSH_URL_PATTERN = Pattern.compile("git@(?:ssh\\.)?([^:]+):(?:v\\d*/)?(.*)");

    private AbstractGitTask gitTask;

    public Git cloneBranch(RunContext runContext, String branch, Boolean withSubmodules) throws Exception {
        Clone cloneHead = Clone.builder()
            .url(gitTask.getUrl())
            .username(gitTask.getUsername())
            .password(gitTask.getPassword())
            .privateKey(gitTask.getPrivateKey())
            .passphrase(gitTask.getPassphrase())
            .cloneSubmodules(withSubmodules)
            .build();

        boolean branchExists = this.branchExists(runContext, branch);
        if (branchExists) {
            cloneHead.toBuilder()
                .branch(branch)
                .build()
                .run(runContext);
        } else {
            runContext.logger().info("Branch {} does not exist, creating it", branch);

            cloneHead.run(runContext);
        }


        Git git = Git.open(runContext.tempDir().toFile());
        if (!branchExists && git.getRepository().resolve(Constants.HEAD) != null) {
            git.checkout()
                .setName(branch)
                .setCreateBranch(true)
                .call();
        }

        return git;
    }

    public boolean branchExists(RunContext runContext, String branch) throws Exception {
        return gitTask.authentified(Git.lsRemoteRepository().setRemote(runContext.render(gitTask.getUrl())), runContext)
            .callAsMap()
            .containsKey(R_HEADS + branch);
    }

    public String getHttpUrl(String gitUrl) {
        String httpUrl = gitUrl;
        // SSH URL
        Matcher sshUrlMatcher = SSH_URL_PATTERN.matcher(httpUrl);
        if (sshUrlMatcher.matches()) {
            httpUrl = sshUrlMatcher.group(1) + "/" + sshUrlMatcher.group(2);

            if (httpUrl.contains("azure.com")) {
                int orgFromProjectSeparatorIndex = httpUrl.lastIndexOf("/");
                httpUrl = httpUrl.substring(0, orgFromProjectSeparatorIndex) + "/_git/" + httpUrl.substring(orgFromProjectSeparatorIndex + 1);
            }

            httpUrl = "https://" + httpUrl;
        } else if (httpUrl.contains("@")) {
            httpUrl = httpUrl.replaceFirst("//.*@", "//");
        }

        return httpUrl;
    }

    public void namespaceAccessGuard(RunContext runContext, String namespaceToAccess) throws IllegalVariableEvaluationException {
        FlowService flowService = runContext.getApplicationContext().getBean(FlowService.class);
        RunContext.FlowInfo flowInfo = runContext.flowInfo();
        flowService.checkAllowedNamespace(
            runContext.tenantId(),
            runContext.render(namespaceToAccess),
            flowInfo.tenantId(),
            flowInfo.namespace()
        );
    }
}
