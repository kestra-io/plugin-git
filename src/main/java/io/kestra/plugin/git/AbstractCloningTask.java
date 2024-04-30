package io.kestra.plugin.git;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public abstract class AbstractCloningTask extends AbstractGitTask {
    @Schema(
        title = "Whether to clone submodules."
    )
    @PluginProperty
    protected Boolean cloneSubmodules;
}
