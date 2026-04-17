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

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public abstract class AbstractKestraContainerTest {

    @Container
    protected static final GenericContainer<?> KESTRA_CONTAINER = new GenericContainer<>(
        DockerImageName.parse("kestra/kestra:develop-no-plugins")
    )
        .withExposedPorts(8080)
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
        kestraTestDataUtils = new KestraContainerTestDataUtils(kestraUrl);
    }
}
