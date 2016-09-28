package dstream;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;

public class StreamProjectTableScanRule extends RelOptRule {
  private static Logger log = LogManager.getLogger(StreamProjectTableScanRule.class);

  public static final StreamProjectTableScanRule INSTANCE =
    new StreamProjectTableScanRule();

  private StreamProjectTableScanRule() {
    super(
          operand(LogicalProject.class,
                  operand(StreamTableScan.class, none())),
          "StreamProjectTableScanRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    log.info("onMatch");
    final LogicalProject project = call.rel(0);
    final StreamTableScan scan = call.rel(1);
    int[] fields = getProjectFields(project.getProjects());
    if (fields == null) {
      // Project contains expressions more complex than just field references.
      return;
    }
    call.transformTo(
                     new StreamTableScan(
                                         scan.getCluster(),
                                         scan.getTable(),
                                         scan.sTable,
                                         fields));
  }

  private int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }
}
