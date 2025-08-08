package io.kestra.plugin.git;

import io.kestra.core.junit.annotations.KestraTest;
import io.micronaut.context.annotation.Value;

@KestraTest
public abstract class AbstractGitTest {

    @Value("${kestra.git.pat}")
    protected String pat;

    @Value("${kestra.git.repository-url}")
    protected String repositoryUrl;

    @Value("${kestra.git.user.email}")
    protected String gitUserEmail;

    @Value("${kestra.git.user.name}")
    protected String gitUserName;

    @Value("${kestra.gitea.pat}")
    protected String giteaPat;

    @Value("${kestra.gitea.repository-url}")
    protected String giteaRepoUrl;

    @Value("${kestra.gitea.user.name}")
    protected String giteaUserName;

    @Value("${kestra.gitea.ca-pem-path:}")
    protected String giteaCaPemPath;
}
