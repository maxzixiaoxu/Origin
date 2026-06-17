package com.origin.backend.compute;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Identifies which backend instance this process is.
 *
 *
 * Resolution order:
 *   1. INSTANCE_ID env var / instance.id property (set per container)
 *   2. HOSTNAME env var (Docker sets this to the container id by default)
 *   3. "local"
 */
@Component
public class InstanceInfo {

    private final String id;

    public InstanceInfo(@Value("${instance.id:${HOSTNAME:local}}") String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }
}
