package io.kestra.plugin.git;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.dashboards.Dashboard;

/**
 * In-memory dashboard store backing {@link MockKestraApiServer}.
 *
 * Replaces DashboardRepositoryInterface, which moved from OSS core to EE in Kestra 2.0.0 and is no
 * longer on an OSS plugin's classpath. Insertion order is preserved so search results stay stable.
 */
public class MockDashboardStore {
    private final Map<String, Dashboard> dashboards = Collections.synchronizedMap(new LinkedHashMap<>());

    /** Stores the dashboard, attaching {@code source} as its source code when it carries none. */
    public Dashboard save(Dashboard dashboard, String source) {
        Dashboard stored = dashboard.getSourceCode() != null
            ? dashboard
            : dashboard.toBuilder().sourceCode(source).build();
        dashboards.put(key(stored.getTenantId(), stored.getId()), stored);
        return stored;
    }

    public List<Dashboard> findAll(String tenantId) {
        synchronized (dashboards) {
            return dashboards.values().stream()
                .filter(d -> tenantId == null || tenantId.equals(d.getTenantId()))
                .toList();
        }
    }

    /** Removes and returns the dashboard, mirroring the repository contract the tests relied on. */
    public Dashboard delete(String tenantId, String dashboardId) {
        return dashboards.remove(key(tenantId, dashboardId));
    }

    private static String key(String tenantId, String dashboardId) {
        return tenantId + "/" + dashboardId;
    }
}
