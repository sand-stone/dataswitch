package kdb.presto;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import kdb.Client;

public class KdbClient {
  private static final Logger log = Logger.get(KdbClient.class);

  /**
   * SchemaName -> (TableName -> TableMetadata)
   */
  private final Supplier<Map<String, Map<String, KdbTable>>> schemas;

  @Inject
  public KdbClient(KdbConfig config, JsonCodec<Map<String, List<KdbTable>>> catalogCodec)
    throws IOException
  {
    requireNonNull(config, "config is null");
    requireNonNull(catalogCodec, "catalogCodec is null");

    schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
  }

  public Set<String> getSchemaNames()
  {
    return schemas.get().keySet();
  }

  public Set<String> getTableNames(String schema)
  {
    requireNonNull(schema, "schema is null");
    Map<String, KdbTable> tables = schemas.get().get(schema);
    if (tables == null) {
      return ImmutableSet.of();
    }
    return tables.keySet();
  }

  public KdbTable getTable(String schema, String tableName)
  {
    requireNonNull(schema, "schema is null");
    requireNonNull(tableName, "tableName is null");
    Map<String, KdbTable> tables = schemas.get().get(schema);
    if (tables == null) {
      return null;
    }
    return tables.get(tableName);
  }

  private static Supplier<Map<String, Map<String, KdbTable>>> schemasSupplier(final JsonCodec<Map<String, List<KdbTable>>> catalogCodec, final URI metadataUri)
  {
    return () -> {
      try {
        return lookupSchemas(metadataUri, catalogCodec);
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    };
  }

  private static Map<String, Map<String, KdbTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<KdbTable>>> catalogCodec)
    throws IOException
  {
    URL result = metadataUri.toURL();
    String json = Resources.toString(result, UTF_8);
    Map<String, List<KdbTable>> catalog = catalogCodec.fromJson(json);

    return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
  }

  private static Function<List<KdbTable>, Map<String, KdbTable>> resolveAndIndexTables(final URI metadataUri)
  {
    return tables -> {
      Iterable<KdbTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
      return ImmutableMap.copyOf(uniqueIndex(resolvedTables, KdbTable::getName));
    };
  }

  private static Function<KdbTable, KdbTable> tableUriResolver(final URI baseUri)
  {
    return table -> {
      List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
      return new KdbTable(table.getName(), table.getColumns(), sources);
    };
  }

}
