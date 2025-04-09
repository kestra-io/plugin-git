package io.kestra.plugin.git;

import io.kestra.core.models.dashboards.Dashboard;
import io.kestra.core.repositories.DashboardRepositoryInterface;
import io.kestra.core.serializers.YamlParser;

public class DashboardUtils {
    public static Dashboard createDashboard(DashboardRepositoryInterface dashboardRepositoryInterface ,
                                            String tenantId, String title, String id) {
        String dashboardSource = """
            id:\s""" + id + """

            title:\s""" + title + """

            description: TEST DASHBOARD
            timeWindow:
              default: P30D # P30DT30H
              max: P365D

            charts:
              - id: executions_timeseries
                type: io.kestra.plugin.core.dashboard.chart.TimeSeries
                chartOptions:
                  displayName: Executions
                  description: Executions duration and count per date
                  legend:
                    enabled: true
                  column: date
                  colorByColumn: state
                data:
                  type: io.kestra.plugin.core.dashboard.data.Executions
                  columns:
                    date:
                      field: START_DATE
                      displayName: Date
                    state:
                      field: STATE
                    total:
                      displayName: Executions
                      agg: COUNT
                      graphStyle: BARS
                    duration:
                      displayName: Duration
                      field: DURATION
                      agg: SUM
                      graphStyle: LINES
            """;
        Dashboard dashboard = YamlParser.parse(dashboardSource, Dashboard.class).toBuilder()
            .tenantId(tenantId)
            .build();

        return dashboardRepositoryInterface.save(dashboard, dashboardSource);
    }
}
