package io.kestra.plugin.git;

import java.util.List;

import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.Flow;

/**
 * Minimal helper for Kestra API interactions in container-based integration tests.
 * Wraps {@link KestraClient} with basic auth to create and query test data.
 */
public class KestraContainerTestDataUtils {

    private final KestraClient kestraClient;

    public KestraContainerTestDataUtils(String kestraUrl, String username, String password) {
        this.kestraClient = KestraClient.builder()
            .url(kestraUrl)
            .basicAuth(username, password)
            .build();
    }

    /**
     * Lists all flows belonging to the given namespace for the given tenant.
     *
     * @param namespace namespace to query (exact match)
     * @param tenantId  tenant identifier, or {@code null} for the default tenant
     * @return list of flows found in that namespace
     */
    public List<Flow> listFlowsByNamespace(String namespace, String tenantId) throws ApiException {
        return kestraClient.flows().listFlowsByNamespace(namespace, tenantId);
    }
}
