package dstream;

import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.DynamicRecordTypeImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class SdbCatalogReader implements Prepare.CatalogReader {
  private static Logger log = LogManager.getLogger(SdbCatalogReader.class);

  protected static final String DEFAULT_CATALOG = "CATALOG";
  protected static final String DEFAULT_SCHEMA = "dstream";

  public static final Ordering<Iterable<String>>
    CASE_INSENSITIVE_LIST_COMPARATOR =
    Ordering.from(String.CASE_INSENSITIVE_ORDER).lexicographical();

  protected final RelDataTypeFactory typeFactory;
  private final boolean caseSensitive;
  private final Map<List<String>, SdbTable> tables;
  protected final Map<String, SdbSchema> schemas;
  private RelDataType addressType;

  public SdbCatalogReader(RelDataTypeFactory typeFactory, boolean caseSensitive) {
    this.typeFactory = typeFactory;
    this.caseSensitive = caseSensitive;
    if (caseSensitive) {
      tables = Maps.newHashMap();
      schemas = Maps.newHashMap();
    } else {
      tables = Maps.newTreeMap(CASE_INSENSITIVE_LIST_COMPARATOR);
      schemas = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    }
  }

  @Override
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public SdbCatalogReader init() {
    final RelDataType intType =
      typeFactory.createSqlType(SqlTypeName.INTEGER);
    final RelDataType intTypeNull =
      typeFactory.createTypeWithNullability(intType, true);
    final RelDataType varchar10Type =
      typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    final RelDataType varchar20Type =
      typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
    final RelDataType timestampType =
      typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType dateType =
      typeFactory.createSqlType(SqlTypeName.DATE);
    final RelDataType booleanType =
      typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    final RelDataType rectilinearCoordType =
      typeFactory.builder()
      .add("X", intType)
      .add("Y", intType)
      .build();
    final RelDataType rectilinearPeekCoordType =
      typeFactory.builder()
      .add("X", intType)
      .add("Y", intType)
      .kind(StructKind.PEEK_FIELDS)
      .build();

    SdbSchema acmeSchema = new SdbSchema("ACME");
    registerSchema(acmeSchema);

    SdbTable acmeTable =
      SdbTable.create(this, acmeSchema, "ACME", false, 4);
    acmeTable.addColumn("id", intType);
    acmeTable.addColumn("name", varchar10Type);
    registerTable(acmeTable);

    addressType =
      new ObjectSqlType(
                        SqlTypeName.STRUCTURED,
                        new SqlIdentifier("ADDRESS", SqlParserPos.ZERO),
                        false,
                        Arrays.asList(
                                      new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
                                      new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
                                      new RelDataTypeFieldImpl("ZIP", 2, intType),
                                      new RelDataTypeFieldImpl("STATE", 3, varchar20Type)),
                        RelDataTypeComparability.NONE);

    SdbSchema structTypeSchema = new SdbSchema("STRUCT");
    registerSchema(structTypeSchema);
    SdbTable structTypeTable = SdbTable.create(this, structTypeSchema, "T",
                                               false, 100);
    structTypeTable.addColumn("K0", varchar20Type);
    structTypeTable.addColumn("C1", varchar20Type);
    final RelDataType f0Type = typeFactory.builder()
      .add("C0", intType)
      .add("C1", intType)
      .kind(StructKind.PEEK_FIELDS_DEFAULT)
      .build();
    structTypeTable.addColumn("F0", f0Type);
    final RelDataType f1Type = typeFactory.builder()
      .add("C0", intTypeNull)
      .add("C2", intType)
      .add("A0", intType)
      .kind(StructKind.PEEK_FIELDS)
      .build();
    structTypeTable.addColumn("F1", f1Type);
    final RelDataType f2Type = typeFactory.builder()
      .add("C3", intType)
      .add("A0", booleanType)
      .kind(StructKind.PEEK_FIELDS)
      .build();
    structTypeTable.addColumn("F2", f2Type);
    registerTable(structTypeTable);
    return this;
  }

  public void lookupOperatorOverloads(SqlIdentifier opName,
                                      SqlFunctionCategory category, SqlSyntax syntax,
                                      List<SqlOperator> operatorList) {
  }

  public List<SqlOperator> getOperatorList() {
    return ImmutableList.of();
  }

  public Prepare.CatalogReader withSchemaPath(List<String> schemaPath) {
    return this;
  }

  public Prepare.PreparingTable getTableForMember(List<String> names) {
    return getTable(names);
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public void registerRules(RelOptPlanner planner) {
  }

  protected void registerTable(SdbTable table) {
    table.onRegister(typeFactory);
    tables.put(table.getQualifiedName(), table);
  }

  protected void registerSchema(SdbSchema schema) {
    schemas.put(schema.name, schema);
  }

  public Prepare.PreparingTable getTable(final List<String> names) {
    log.info("names:{}", names);
    switch (names.size()) {
    case 1:
      return tables.get(
                        ImmutableList.of(DEFAULT_CATALOG, DEFAULT_SCHEMA, names.get(0)));
    case 2:
      return tables.get(
                        ImmutableList.of(DEFAULT_CATALOG, names.get(0), names.get(1)));
    case 3:
      return tables.get(names);
    default:
      return null;
    }
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    if (typeName.equalsDeep(addressType.getSqlIdentifier(), Litmus.IGNORE)) {
      return addressType;
    } else {
      return null;
    }
  }

  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    //log.info("getAllSchemaObjectNames:{}", names);
    List<SqlMoniker> result;
    switch (names.size()) {
    case 0:
      // looking for catalog and schema names
      return ImmutableList.<SqlMoniker>builder()
        .add(new SqlMonikerImpl(DEFAULT_CATALOG, SqlMonikerType.CATALOG))
        .addAll(getAllSchemaObjectNames(ImmutableList.of(DEFAULT_CATALOG)))
        .build();
    case 1:
      // looking for schema names
      result = Lists.newArrayList();
      for (SdbSchema schema : schemas.values()) {
        final String catalogName = names.get(0);
        if (schema.getCatalogName().equals(catalogName)) {
          final ImmutableList<String> names1 =
            ImmutableList.of(catalogName, schema.name);
          result.add(new SqlMonikerImpl(names1, SqlMonikerType.SCHEMA));
        }
      }
      return result;
    case 2:
      // looking for table names in the given schema
      SdbSchema schema = schemas.get(names.get(1));
      if (schema == null) {
        return Collections.emptyList();
      }
      result = Lists.newArrayList();
      for (String tableName : schema.tableNames) {
        result.add(
                   new SqlMonikerImpl(
                                      ImmutableList.of(schema.getCatalogName(), schema.name,
                                                       tableName),
                                      SqlMonikerType.TABLE));
      }
      return result;
    default:
      return Collections.emptyList();
    }
  }

  public List<String> getSchemaName() {
    return ImmutableList.of(DEFAULT_CATALOG, DEFAULT_SCHEMA);
  }

  public RelDataTypeField field(RelDataType rowType, String alias) {
    //log.info("field:{}", alias);
    return SqlValidatorUtil.lookupField(caseSensitive, rowType, alias);
  }

  public boolean matches(String string, String name) {
    return Util.matches(caseSensitive, string, name);
  }

  public RelDataType createTypeFromProjection(final RelDataType type,
                                              final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
                                                     typeFactory, caseSensitive);
  }

  private static List<RelCollation> deduceMonotonicity(
                                                       Prepare.PreparingTable table) {
    final List<RelCollation> collationList = Lists.newArrayList();

    // Deduce which fields the table is sorted on.
    int i = -1;
    for (RelDataTypeField field : table.getRowType().getFieldList()) {
      ++i;
      final SqlMonotonicity monotonicity =
        table.getMonotonicity(field.getName());
      if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
        final RelFieldCollation.Direction direction =
          monotonicity.isDecreasing()
          ? RelFieldCollation.Direction.DESCENDING
          : RelFieldCollation.Direction.ASCENDING;
        collationList.add(
                          RelCollations.of(
                                           new RelFieldCollation(i, direction)));
      }
    }
    return collationList;
  }

  public static class SdbSchema {
    private final List<String> tableNames = Lists.newArrayList();
    private String name;

    public SdbSchema(String name) {
      this.name = name;
    }

    public void addTable(String name) {
      tableNames.add(name);
    }

    public String getCatalogName() {
      return DEFAULT_CATALOG;
    }

    public String getName() {
      return name;
    }
  }

  public abstract static class AbstractModifiableTable
    extends AbstractTable implements ModifiableTable {
    protected AbstractModifiableTable(String tableName) {
      super();
    }

    public TableModify toModificationRel(
                                         RelOptCluster cluster,
                                         RelOptTable table,
                                         Prepare.CatalogReader catalogReader,
                                         RelNode child,
                                         TableModify.Operation operation,
                                         List<String> updateColumnList,
                                         boolean flattened) {
      return LogicalTableModify.create(table, catalogReader, child, operation,
                                       updateColumnList, flattened);
    }
  }

  public static class SdbTable implements Prepare.PreparingTable {
    protected final SdbCatalogReader catalogReader;
    private final boolean stream;
    private final double rowCount;
    protected final List<Map.Entry<String, RelDataType>> columnList =
      new ArrayList<>();
    protected RelDataType rowType;
    private List<RelCollation> collationList;
    protected final List<String> names;
    private final Set<String> monotonicColumnSet = Sets.newHashSet();
    private StructKind kind = StructKind.FULLY_QUALIFIED;

    public SdbTable(SdbCatalogReader catalogReader, String catalogName,
                    String schemaName, String name, boolean stream, double rowCount) {
      this.catalogReader = catalogReader;
      this.stream = stream;
      this.rowCount = rowCount;
      this.names = ImmutableList.of(catalogName, schemaName, name);
    }

    public static SdbTable create(SdbCatalogReader catalogReader,
                                  SdbSchema schema, String name, boolean stream, double rowCount) {
      SdbTable table =
        new SdbTable(catalogReader, schema.getCatalogName(), schema.name,
                     name, stream, rowCount);
      schema.addTable(name);
      return table;
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isAssignableFrom(Table.class)) {
        return clazz.cast(
                          new AbstractModifiableTable(Util.last(names)) {
                            @Override public RelDataType
                              getRowType(RelDataTypeFactory typeFactory) {
                              return typeFactory.createStructType(rowType.getFieldList());
                            }

                            @Override public Collection getModifiableCollection() {
                              return null;
                            }

                            @Override public <E> Queryable<E>
                              asQueryable(QueryProvider queryProvider, SchemaPlus schema,
                                          String tableName) {
                              return null;
                            }

                            @Override public Type getElementType() {
                              return null;
                            }

                            @Override public Expression getExpression(SchemaPlus schema,
                                                                      String tableName, Class clazz) {
                              return null;
                            }
                          });
      }
      return null;
    }

    public double getRowCount() {
      return rowCount;
    }

    public RelOptSchema getRelOptSchema() {
      return catalogReader;
    }

    public RelNode toRel(ToRelContext context) {
      return LogicalTableScan.create(context.getCluster(), this);
    }

    public List<RelCollation> getCollationList() {
      return collationList;
    }

    public RelDistribution getDistribution() {
      return RelDistributions.BROADCAST_DISTRIBUTED;
    }

    public boolean isKey(ImmutableBitSet columns) {
      return false;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public boolean supportsModality(SqlModality modality) {
      return modality == (stream ? SqlModality.STREAM : SqlModality.RELATION);
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = typeFactory.createStructType(kind, Pair.right(columnList),
                                             Pair.left(columnList));
      collationList = deduceMonotonicity(this);
    }

    public List<String> getQualifiedName() {
      return names;
    }

    public SqlMonotonicity getMonotonicity(String columnName) {
      return monotonicColumnSet.contains(columnName)
        ? SqlMonotonicity.INCREASING
        : SqlMonotonicity.NOT_MONOTONIC;
    }

    public SqlAccessType getAllowedAccess() {
      return SqlAccessType.ALL;
    }

    public Expression getExpression(Class clazz) {
      throw new UnsupportedOperationException();
    }

    public void addColumn(String name, RelDataType type) {
      columnList.add(Pair.of(name, type));
    }

    public void addMonotonic(String name) {
      monotonicColumnSet.add(name);
      assert Pair.left(columnList).contains(name);
    }

    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      final SdbTable table = new SdbTable(catalogReader, names.get(0),
                                          names.get(1), names.get(2), stream, rowCount);
      table.columnList.addAll(columnList);
      table.columnList.addAll(extendedFields);
      table.onRegister(catalogReader.typeFactory);
      return table;
    }

    public void setKind(StructKind kind) {
      this.kind = kind;
    }

    public StructKind getKind() {
      return kind;
    }
  }

  public static class SdbDynamicTable extends SdbTable {
    SdbDynamicTable(SdbCatalogReader catalogReader, String catalogName,
                    String schemaName, String name, boolean stream, double rowCount) {
      super(catalogReader, catalogName, schemaName, name, stream, rowCount);
    }

    public void onRegister(RelDataTypeFactory typeFactory) {
      rowType = new DynamicRecordTypeImpl(typeFactory);
    }

    /**
     * Recreates an immutable rowType, if the table has Dynamic Record Type,
     * when converts table to Rel.
     */
    public RelNode toRel(ToRelContext context) {
      if (rowType.isDynamicStruct()) {
        rowType = new RelRecordType(rowType.getFieldList());
      }
      return super.toRel(context);
    }
  }

  private static class DelegateStructType implements RelDataType {
    private RelDataType delegate;
    private StructKind structKind;

    DelegateStructType(RelDataType delegate, StructKind structKind) {
      assert delegate.isStruct();
      this.delegate = delegate;
      this.structKind = structKind;
    }

    public boolean isStruct() {
      return delegate.isStruct();
    }

    public boolean isDynamicStruct() {
      return delegate.isDynamicStruct();
    }

    public List<RelDataTypeField> getFieldList() {
      return delegate.getFieldList();
    }

    public List<String> getFieldNames() {
      return delegate.getFieldNames();
    }

    public int getFieldCount() {
      return delegate.getFieldCount();
    }

    public StructKind getStructKind() {
      return structKind;
    }

    public RelDataTypeField getField(String fieldName, boolean caseSensitive,
                                     boolean elideRecord) {
      return delegate.getField(fieldName, caseSensitive, elideRecord);
    }

    public boolean isNullable() {
      return delegate.isNullable();
    }

    public RelDataType getComponentType() {
      return delegate.getComponentType();
    }

    public RelDataType getKeyType() {
      return delegate.getKeyType();
    }

    public RelDataType getValueType() {
      return delegate.getValueType();
    }

    public Charset getCharset() {
      return delegate.getCharset();
    }

    public SqlCollation getCollation() {
      return delegate.getCollation();
    }

    public SqlIntervalQualifier getIntervalQualifier() {
      return delegate.getIntervalQualifier();
    }

    public int getPrecision() {
      return delegate.getPrecision();
    }

    public int getScale() {
      return delegate.getScale();
    }

    public SqlTypeName getSqlTypeName() {
      return delegate.getSqlTypeName();
    }

    public SqlIdentifier getSqlIdentifier() {
      return delegate.getSqlIdentifier();
    }

    public String getFullTypeString() {
      return delegate.getFullTypeString();
    }

    public RelDataTypeFamily getFamily() {
      return delegate.getFamily();
    }

    public RelDataTypePrecedenceList getPrecedenceList() {
      return delegate.getPrecedenceList();
    }

    public RelDataTypeComparability getComparability() {
      return delegate.getComparability();
    }
  }
}
