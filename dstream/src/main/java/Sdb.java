package dstream;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Sdb {
  private static Logger log = LogManager.getLogger(Sdb.class);

  private SqlOperatorTable opTab;
  private RelDataTypeFactory typeFactory;
  private RelOptPlanner planner;
  private Function<RelOptCluster, RelOptCluster> clusterFactory;

  public RelDataTypeFactory createTypeFactory() {
    return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  public Prepare.CatalogReader createCatalogReader(RelDataTypeFactory typeFactory) {
    return new SdbCatalogReader(typeFactory, false).init();
  }

  public SqlConformance getConformance() {
    return SqlConformance.DEFAULT;
  }

  public final SqlOperatorTable getOperatorTable() {
    if (opTab == null) {
      opTab = createOperatorTable();
    }
    return opTab;
  }

  public RelDataTypeFactory getTypeFactory() {
    if(typeFactory == null)
      typeFactory = createTypeFactory();
    return typeFactory;
  }

  public RelOptPlanner createPlanner() {
    return new SdbRelOptPlanner();
  }

  public RelOptPlanner getPlanner() {
    if (planner == null) {
      planner = createPlanner();
    }
    return planner;
  }

  private SqlOperatorTable createOperatorTable() {
    final SdbSqlOperatorTable opTab =
      new SdbSqlOperatorTable(SqlStdOperatorTable.instance());
    SdbSqlOperatorTable.addRamp(opTab);
    return opTab;
  }

  private static class SdbValidator extends SqlValidatorImpl {
    public SdbValidator(
                        SqlOperatorTable opTab,
                        SqlValidatorCatalogReader catalogReader,
                        RelDataTypeFactory typeFactory,
                        SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }

    // override SqlValidator
    public boolean shouldExpandIdentifiers() {
      return true;
    }
  }

  public SqlToRelConverter createSqlToRelConverter(
                                                   final SqlValidator validator,
                                                   final Prepare.CatalogReader catalogReader,
                                                   final RelDataTypeFactory typeFactory,
                                                   final SqlToRelConverter.Config config) {
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster =
      RelOptCluster.create(getPlanner(), rexBuilder);
    if (clusterFactory != null) {
      cluster = clusterFactory.apply(cluster);
    }
    return new SqlToRelConverter(null, validator, catalogReader, cluster,
                                 StandardConvertletTable.INSTANCE, config);
  }

  public SqlValidator createValidator(
                                      SqlValidatorCatalogReader catalogReader,
                                      RelDataTypeFactory typeFactory) {
    return new SdbValidator(
                            getOperatorTable(),
                            catalogReader,
                            typeFactory,
                            getConformance());
  }

  public SqlNode parseQuery(String sql) throws Exception {
    SqlParser parser = SqlParser.create(sql);
    return parser.parseQuery();
  }

  private static void print(RelNode node) {
    log.info("children {}={}", node, node.getClass());
    if(node instanceof org.apache.calcite.rel.logical.LogicalTableScan) {
      log.info("table {}", ((org.apache.calcite.rel.logical.LogicalTableScan)node).getTable());
    } else if (node instanceof org.apache.calcite.rel.logical.LogicalFilter) {
      log.info("filter {}", ((org.apache.calcite.rel.logical.LogicalFilter)node).getCondition());
    } else if (node instanceof org.apache.calcite.rel.logical.LogicalProject) {
      log.info("project {}", ((org.apache.calcite.rel.logical.LogicalProject)node).getProjects());
    } else if (node instanceof org.apache.calcite.rel.logical.LogicalSort) {
      log.info("sort {}", ((org.apache.calcite.rel.logical.LogicalSort)node).getChildExps());
      log.info("sort {}", ((org.apache.calcite.rel.logical.LogicalSort)node).getCollationList());
    }

    for(RelNode c : node.getInputs()) {
      print(c);
    }
  }

  public static void main(String[] args) throws Exception {
    Sdb sdb = new Sdb();
    SqlNode sqlQuery = sdb.parseQuery("select id+1, name from acme.acme where id>3 order by id desc, name desc");
    final RelDataTypeFactory typeFactory = sdb.getTypeFactory();
    final Prepare.CatalogReader catalogReader =
      sdb.createCatalogReader(typeFactory);
    final SqlValidator validator = sdb.createValidator(catalogReader, typeFactory);
    final SqlToRelConverter.Config localConfig;
    boolean enableExpand = true;
    localConfig = SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(true).withExpand(enableExpand).build();
    final SqlNode validatedQuery = validator.validate(sqlQuery);
    //final SqlNode validatedQuery = sqlQuery;
    //log.info("***sqlQuery {}", validatedQuery);
    final SqlToRelConverter converter =
      sdb.createSqlToRelConverter(
                                  validator,
                                  catalogReader,
                                  typeFactory,
                                  localConfig);
    RelRoot root =
      converter.convertQuery(validatedQuery, false, true);
    assert root != null;
    log.info("root={}", root);
    print(root.rel);
  }
}
