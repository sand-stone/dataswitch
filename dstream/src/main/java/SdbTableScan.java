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

import java.util.List;

/**
 * Relational expression representing a scan of a CSV file.
 *
 * <p>Like any table scan, it serves as a leaf node of a query tree.</p>
 */
public class SdbTableScan extends TableScan implements EnumerableRel {
  final SdbTranslatableTable csvTable;
  final int[] fields;

  protected SdbTableScan(RelOptCluster cluster, RelOptTable table,
                         SdbTranslatableTable csvTable, int[] fields) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
    this.csvTable = csvTable;
    this.fields = fields;

    assert csvTable != null;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new SdbTableScan(getCluster(), table, csvTable, fields);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .item("fields", Primitive.asList(fields));
  }

  @Override public RelDataType deriveRowType() {
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.FieldInfoBuilder builder =
      getCluster().getTypeFactory().builder();
    for (int field : fields) {
      builder.add(fieldList.get(field));
    }
    return builder.build();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(SdbProjectTableScanRule.INSTANCE);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
      PhysTypeImpl.of(
                      implementor.getTypeFactory(),
                      getRowType(),
                      pref.preferArray());

    if (table instanceof JsonTable) {
      return implementor.result(
                                physType,
                                Blocks.toBlock(
                                               Expressions.call(table.getExpression(JsonTable.class),
                                                                "enumerable")));
    }
    return implementor.result(
                              physType,
                              Blocks.toBlock(
                                             Expressions.call(table.getExpression(SdbTranslatableTable.class),
                                                              "project", implementor.getRootExpression(),
                                                              Expressions.constant(fields))));
  }
}
