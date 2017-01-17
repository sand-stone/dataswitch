package kdb.presto;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class KdbConfig
{
    private URI metadata;

    @NotNull
    public URI getMetadata()
    {
        return metadata;
    }

    @Config("metadata-uri")
    public KdbConfig setMetadata(URI metadata)
    {
        this.metadata = metadata;
        return this;
    }
}
