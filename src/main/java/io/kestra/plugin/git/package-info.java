@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using Git.\n" +
        "Git is a distributed version control system that tracks changes in any set of computer files, usually used for coordinating work among programmers collaboratively developing source code during software development. Its goals include speed, data integrity, and support for distributed, non-linear workflows.\n"
        +
        "Dashboard tasks moved to the Git EE plugin in Kestra 2.0.0, because dashboards are an Enterprise Edition feature.",
    categories = {
        PluginSubGroup.PluginCategory.INFRASTRUCTURE,
        PluginSubGroup.PluginCategory.BUSINESS
    }
)
package io.kestra.plugin.git;

import io.kestra.core.models.annotations.PluginSubGroup;
