package org.apache.iotdb.warehouse.server.query.simd;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.warehouse.common.Dataset;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class Cartesian extends BinaryOperator {
    String operatorName = "Cartesian(T)";
    @Override
    public Dataset transform(Dataset data1, Dataset data2, int refer_id) {

    }

    @Override
    public String getOperatorName() {
        return operatorName;
    }
}
