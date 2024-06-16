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
}
