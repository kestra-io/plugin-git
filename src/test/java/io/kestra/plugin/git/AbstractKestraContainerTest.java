package io.kestra.plugin.git;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

/**
 * Base class for integration tests that run against a real Kestra Docker container.
 * Complements (does not replace) the existing {@link MockKestraApiServer} tests.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public abstract class AbstractKestraContainerTest {

    protected static final String USERNAME = "admin@admin.com";
    protected static final String PASSWORD = "Root!1234";

    // kestra/kestra:v1.3-no-plugins is pinned for stability — develop has known regressions
    @Container
    protected static final GenericContainer<?> KESTRA_CONTAINER = new GenericContainer<>(
        DockerImageName.parse("kestra/kestra:v1.3-no-plugins")
    )
        .withExposedPorts(8080)
        .withEnv("KESTRA_SECURITY_SUPER_ADMIN_USERNAME", USERNAME)
        .withEnv("KESTRA_SECURITY_SUPER_ADMIN_PASSWORD", PASSWORD)
        .withEnv("KESTRA_CONFIGURATION", """
            kestra:
              repository:
                type: memory
              queue:
                type: memory
              storage:
                type: local
                local:
                  base-path: /tmp/kestra-storage
              server:
                basic-auth:
                  username: admin@admin.com
                  password: Root!1234
              security:
                super-admin:
                  username: admin@admin.com
                  password: Root!1234
            """)
        .withCommand("server local")
        .waitingFor(Wait.forHttp("/ui/login").forStatusCode(200))
        .withLogConsumer(new Slf4jLogConsumer(log))
        .withStartupTimeout(Duration.ofMinutes(2));

    protected String kestraUrl;
    protected KestraContainerTestDataUtils kestraTestDataUtils;

    @BeforeAll
    void setupKestra() {
        kestraUrl = "http://" + KESTRA_CONTAINER.getHost() + ":" + KESTRA_CONTAINER.getMappedPort(8080);
        log.info("Kestra container started at URL: {}", kestraUrl);
        kestraTestDataUtils = new KestraContainerTestDataUtils(kestraUrl, USERNAME, PASSWORD);
    }
}
