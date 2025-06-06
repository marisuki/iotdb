/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.db.protocol.session.IClientSession.SqlDialect.TREE;

/**
 * Used to plan a fragment instance. One fragment instance could be split into multiple pipelines so
 * that a fragment instance could be run in parallel, and thus we can take full advantages of
 * multi-cores.
 */
public class LocalExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);
  private static final IMemoryBlock OPERATORS_MEMORY_BLOCK;
  private static final long MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;

  public final Metadata metadata = new TableMetadataImpl();

  static {
    IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    DataNodeMemoryConfig MEMORY_CONFIG = IoTDBDescriptor.getInstance().getMemoryConfig();

    OPERATORS_MEMORY_BLOCK =
        MEMORY_CONFIG
            .getOperatorsMemoryManager()
            .exactAllocate("Operators", MemoryBlockType.DYNAMIC);
    MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD =
        (long)
            ((OPERATORS_MEMORY_BLOCK.getTotalMemorySizeInBytes())
                * (1.0 - CONFIG.getMaxAllocateMemoryRatioForLoad()));
  }

  public long getFreeMemoryForOperators() {
    return OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes();
  }

  public long getFreeMemoryForLoadTsFile() {
    return OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes() - MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
  }

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      DataNodeQueryContext dataNodeQueryContext)
      throws MemoryNotEnoughException {
    if (Objects.isNull(plan)) {
      throw new IoTDBRuntimeException(
          "The planNode is null during local execution, maybe caused by closing of the current dataNode",
          TSStatusCode.CLOSE_OPERATION_ERROR.getStatusCode());
    }
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(types, instanceContext, dataNodeQueryContext);

    Operator root = generateOperator(instanceContext, context, plan);

    PipelineMemoryEstimator memoryEstimator =
        context.constructPipelineMemoryEstimator(root, null, plan, -1);
    // set the map to null for gc
    context.invalidateParentPlanNodeIdToMemoryEstimator();

    // check whether current free memory is enough to execute current query
    long estimatedMemorySize = checkMemory(memoryEstimator, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), estimatedMemorySize);

    instanceContext.setSourcePaths(collectSourcePaths(context));
    instanceContext.setDevicePathsToContext(collectDevicePathsToContext(context));
    instanceContext.setQueryDataSourceType(
        getQueryDataSourceType((DataDriverContext) context.getDriverContext()));

    context.getTimePartitions().ifPresent(instanceContext::setTimePartitions);

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      ISchemaRegion schemaRegion)
      throws MemoryNotEnoughException {
    if (Objects.isNull(plan)) {
      throw new IoTDBRuntimeException(
          "The planNode is null during local execution, maybe caused by closing of the current dataNode",
          TSStatusCode.CLOSE_OPERATION_ERROR.getStatusCode());
    }
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(types, instanceContext, schemaRegion);

    Operator root = generateOperator(instanceContext, context, plan);

    PipelineMemoryEstimator memoryEstimator =
        context.constructPipelineMemoryEstimator(root, null, plan, -1);
    // set the map to null for gc
    context.invalidateParentPlanNodeIdToMemoryEstimator();

    // check whether current free memory is enough to execute current query
    checkMemory(memoryEstimator, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), 0);

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  private Operator generateOperator(
      FragmentInstanceContext instanceContext, LocalExecutionPlanContext context, PlanNode node) {
    // Generate pipelines, return the last pipeline data structure
    // TODO Replace operator with operatorFactory to build multiple driver for one pipeline
    Operator root;
    IClientSession.SqlDialect sqlDialect =
        instanceContext.getSessionInfo() == null
            ? TREE
            : instanceContext.getSessionInfo().getSqlDialect();
    switch (sqlDialect) {
      case TREE:
        instanceContext.setIgnoreAllNullRows(true);
        root = node.accept(new OperatorTreeGenerator(), context);
        break;
      case TABLE:
        instanceContext.setIgnoreAllNullRows(false);
        root = node.accept(new TableOperatorGenerator(metadata), context);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown sql dialect: %s", sqlDialect));
    }
    return root;
  }

  private long checkMemory(
      final PipelineMemoryEstimator memoryEstimator, FragmentInstanceStateMachine stateMachine)
      throws MemoryNotEnoughException {

    // if it is disabled, just return
    if (!IoTDBDescriptor.getInstance().getMemoryConfig().isEnableQueryMemoryEstimation()
        && !IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return 0;
    }

    long estimatedMemorySize = memoryEstimator.getEstimatedMemoryUsageInBytes();

    QueryRelatedResourceMetricSet.getInstance().updateEstimatedMemory(estimatedMemorySize);

    if (OPERATORS_MEMORY_BLOCK.allocate(estimatedMemorySize)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "[ConsumeMemory] consume: {}, current remaining memory: {}",
            estimatedMemorySize,
            OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes());
      }
    } else {
      throw new MemoryNotEnoughException(
          String.format(
              "There is not enough memory to execute current fragment instance, "
                  + "current remaining free memory is %dB, "
                  + "estimated memory usage for current fragment instance is %dB",
              OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes(), estimatedMemorySize));
    }
    stateMachine.addStateChangeListener(
        newState -> {
          if (newState.isDone()) {
            try (SetThreadName fragmentInstanceName =
                new SetThreadName(stateMachine.getFragmentInstanceId().getFullId())) {
              OPERATORS_MEMORY_BLOCK.release(estimatedMemorySize);
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "[ReleaseMemory] release: {}, current remaining memory: {}",
                    estimatedMemorySize,
                    OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes());
              }
            }
          }
        });
    return estimatedMemorySize;
  }

  private QueryDataSourceType getQueryDataSourceType(DataDriverContext dataDriverContext) {
    return dataDriverContext.getQueryDataSourceType().orElse(QueryDataSourceType.SERIES_SCAN);
  }

  private Map<IDeviceID, DeviceContext> collectDevicePathsToContext(
      LocalExecutionPlanContext context) {
    DataDriverContext dataDriverContext = (DataDriverContext) context.getDriverContext();
    Map<IDeviceID, DeviceContext> deviceContextMap = dataDriverContext.getDeviceIDToContext();
    dataDriverContext.clearDeviceIDToContext();
    return deviceContextMap;
  }

  private List<IFullPath> collectSourcePaths(LocalExecutionPlanContext context) {
    List<IFullPath> sourcePaths = new ArrayList<>();
    context
        .getPipelineDriverFactories()
        .forEach(
            pipeline -> {
              DataDriverContext dataDriverContext = (DataDriverContext) pipeline.getDriverContext();
              sourcePaths.addAll(dataDriverContext.getPaths());
              dataDriverContext.clearPaths();
            });
    return sourcePaths;
  }

  public synchronized boolean forceAllocateFreeMemoryForOperators(long memoryInBytes) {
    // TODO @spricoder: consider a better way
    if (OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes() - memoryInBytes
        <= MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD) {
      return false;
    } else {
      OPERATORS_MEMORY_BLOCK.forceAllocateWithoutLimitation(memoryInBytes);
      return true;
    }
  }

  public synchronized long tryAllocateFreeMemoryForOperators(long memoryInBytes) {
    if (OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes() - memoryInBytes
        <= MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD) {
      long result =
          OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes() - MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
      OPERATORS_MEMORY_BLOCK.forceAllocateWithoutLimitation(result);
      return result;
    } else {
      OPERATORS_MEMORY_BLOCK.forceAllocateWithoutLimitation(memoryInBytes);
      return memoryInBytes;
    }
  }

  public void reserveFromFreeMemoryForOperators(
      final long memoryInBytes,
      final long reservedBytes,
      final String queryId,
      final String contextHolder) {
    if (OPERATORS_MEMORY_BLOCK.allocate(memoryInBytes)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "[ConsumeMemory] consume: {}, current remaining memory: {}",
            memoryInBytes,
            OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes());
      }
    } else {
      throw new MemoryNotEnoughException(
          String.format(
              "There is not enough memory for Query %s, the contextHolder is %s,"
                  + "current remaining free memory is %dB, "
                  + "already reserved memory for this context in total is %dB, "
                  + "the memory requested this time is %dB",
              queryId,
              contextHolder,
              OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes(),
              reservedBytes,
              memoryInBytes));
    }
  }

  public void releaseToFreeMemoryForOperators(final long memoryInBytes) {
    OPERATORS_MEMORY_BLOCK.release(memoryInBytes);
  }

  public long getAllocateMemoryForOperators() {
    return OPERATORS_MEMORY_BLOCK.getTotalMemorySizeInBytes();
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final LocalExecutionPlanner INSTANCE = new LocalExecutionPlanner();
  }
}
