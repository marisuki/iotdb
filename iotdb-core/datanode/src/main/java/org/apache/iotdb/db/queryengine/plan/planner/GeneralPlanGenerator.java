package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralPlanGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext>  {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralPlanGenerator.class);
    private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
            MPPDataExchangeService.getInstance().getMPPDataExchangeManager();


    @Override
    public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
        return null;
    }
}
