package org.apache.iotdb.warehouse.server.query.simd;

import org.apache.iotdb.warehouse.common.Dataset;

public abstract class BinaryOperator {
    //String operatorName;

    public abstract Dataset transform(Dataset data1, Dataset data2, int refer_id);

    public abstract String getOperatorName();

    public String toString() {
        return String.format("%s: binary", this.getOperatorName());
    }

}
