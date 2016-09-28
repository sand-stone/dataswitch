package dstream;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;

public class StreamTableScan extends TableScan implements EnumerableRel {
  private static Logger log = LogManager.getLogger(StreamTableScan.class);

  final TranslatableTable sTable;
  final int[] fields;

  protected StreamTableScan(RelOptCluster cluster, RelOptTable table,
                         TranslatableTable sTable, int[] fields) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
    this.sTable = sTable;
    this.fields = fields;

    assert sTable != null;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    log.info("copy");
    return new StreamTableScan(getCluster(), table, sTable, fields);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    log.info("explainTerms");
    return super.explainTerms(pw)
      .item("fields", Primitive.asList(fields));
  }

  @Override public RelDataType deriveRowType() {
    log.info("deriveRowType");
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.FieldInfoBuilder builder =
      getCluster().getTypeFactory().builder();
    for (int field : fields) {
      builder.add(fieldList.get(field));
    }
    return builder.build();
  }

  @Override public void register(RelOptPlanner planner) {
    log.info("register");
    planner.addRule(StreamProjectTableScanRule.INSTANCE);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    log.info("implement");
    PhysType physType =
      PhysTypeImpl.of(
                      implementor.getTypeFactory(),
                      getRowType(),
                      pref.preferArray());

    return implementor.result(
                              physType,
                              Blocks.toBlock(
                                             Expressions.call(table.getExpression(TranslatableTable.class),
                                                              "project", implementor.getRootExpression(),
                                                              Expressions.constant(fields))));
  }
}
