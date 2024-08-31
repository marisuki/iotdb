package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

public class VersionScanUtil extends SeriesScanUtil {
    public VersionScanUtil(IFullPath seriesPath, Ordering scanOrder, SeriesScanOptions scanOptions, FragmentInstanceContext context) {
        super(seriesPath, scanOrder, scanOptions, context);
    }
}
