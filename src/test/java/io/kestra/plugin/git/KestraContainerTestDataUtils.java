package io.kestra.plugin.git;

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
}
