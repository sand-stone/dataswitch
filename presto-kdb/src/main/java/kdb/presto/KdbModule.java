package kdb.presto;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class KdbModule
  implements Module
{
  private final String connectorId;
  private final TypeManager typeManager;

  public KdbModule(String connectorId, TypeManager typeManager)
  {
    this.connectorId = requireNonNull(connectorId, "connector id is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(TypeManager.class).toInstance(typeManager);

    binder.bind(KdbConnector.class).in(Scopes.SINGLETON);
    binder.bind(KdbConnectorId.class).toInstance(new KdbConnectorId(connectorId));
    binder.bind(KdbMetadata.class).in(Scopes.SINGLETON);
    binder.bind(KdbClient.class).in(Scopes.SINGLETON);
    binder.bind(KdbSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(KdbRecordSetProvider.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(KdbConfig.class);

    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(KdbTable.class));
  }

  public static final class TypeDeserializer
    extends FromStringDeserializer<Type>
  {
    private final TypeManager typeManager;

    @Inject
    public TypeDeserializer(TypeManager typeManager)
    {
      super(Type.class);
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context)
    {
      Type type = typeManager.getType(parseTypeSignature(value));
      checkArgument(type != null, "Unknown type %s", value);
      return type;
    }
  }
}
