package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

public class VersionedExecutionPushDown implements PlanOptimizer {
    @Override
    public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
        if (analysis.getTreeStatement().getType() != StatementType.QUERY) {
            return plan;
        }
        QueryStatement queryStatement = analysis.getQueryStatement();

        return plan;
    }

    private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

        @Override
        public PlanNode visitPlan(PlanNode node, RewriterContext context) {
            return null;
        }

    }

    private static class RewriterContext {

    }
}
