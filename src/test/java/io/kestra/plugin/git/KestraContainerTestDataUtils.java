package io.kestra.plugin.git;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.Flow;

public class KestraContainerTestDataUtils {

    private final KestraClient kestraClient;

    public KestraContainerTestDataUtils(String kestraUrl, String username, String password) {
        this.kestraClient = KestraClient.builder()
            .url(kestraUrl)
            .basicAuth(username, password)
            .build();
    }

    public List<Flow> listFlowsByNamespace(String namespace, String tenantId) throws ApiException {
        return kestraClient.flows().listFlowsByNamespace(namespace, tenantId);
    }

    /**
     * Imports a flow into the Kestra container using the SDK's importFlows API.
     * The YAML is written to a named temp file so the API can infer the flow id.
     * Passing {@code false} as the first argument performs an additive (non-destructive) import.
     */
    public void createFlow(String tenantId, String flowYaml) throws ApiException, IOException {
        var tmp = Files.createTempFile("kestra-flow-", ".yaml");
        try {
            Files.writeString(tmp, flowYaml, StandardCharsets.UTF_8);
            kestraClient.flows().importFlows(false, tenantId, tmp.toFile());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
