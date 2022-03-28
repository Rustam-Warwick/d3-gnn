package helpers;

import elements.GraphOp;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import storage.BaseStorage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class MyKeyedProcessOperator extends KeyedProcessOperator<String, GraphOp, GraphOp> {
    public MyKeyedProcessOperator(BaseStorage function) {
        super(function);
    }

    public Short getOperatorMainKey() {
        int index = getRuntimeContext().getIndexOfThisSubtask();
        int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        boolean[] seen = new boolean[parallelism];
        List<Short> thisKeysList = new ArrayList<>();
        List<Short> replicaKeysList = new ArrayList<>();


        short masterKey = 0;
        for (short i = 0; i < maxParallelism; i++) {
            int operatorIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(String.valueOf(i), maxParallelism, parallelism);
            if (seen[operatorIndex]) {
                if (operatorIndex == index) {
                    thisKeysList.add(i);
                }
            } else {
                if (operatorIndex == index) {
                    masterKey = i;
                    thisKeysList.add(i);
                } else {
                    replicaKeysList.add(i);
                }

            }

            seen[operatorIndex] = true;
        }

        ((BaseStorage) userFunction).thisKeys = thisKeysList;
        ((BaseStorage) userFunction).otherKeys = replicaKeysList;

        return masterKey;
    }

    @Override
    public void open() throws Exception {
        super.open();
        Field collectorField = KeyedProcessOperator.class.getDeclaredField("collector");
        Field contextField = KeyedProcessOperator.class.getDeclaredField("context");
        collectorField.setAccessible(true);
        contextField.setAccessible(true);
        TimestampedCollector<GraphOp> collector = (TimestampedCollector<GraphOp>) collectorField.get(this);
        KeyedProcessFunction<String, GraphOp, GraphOp>.Context context = (KeyedProcessFunction<String, GraphOp, GraphOp>.Context) contextField.get(this);
        Short firstKey = getOperatorMainKey();
        ((BaseStorage) userFunction).out = collector;
        ((BaseStorage) userFunction).ctx = context;
        ((BaseStorage) userFunction).currentKey = firstKey;
        ((BaseStorage) userFunction).thisMaster = firstKey;
        setCurrentKey(String.valueOf(firstKey));
        FunctionUtils.openFunction(userFunction, new Configuration());

    }


}
