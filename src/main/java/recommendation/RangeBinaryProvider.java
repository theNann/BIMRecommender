package recommendation;

import compression.Range;
import org.apache.flink.api.java.ExecutionEnvironment;
import java.util.List;

public class RangeBinaryProvider extends ResultProvider{
    private Range range;
    public RangeBinaryProvider(ExecutionEnvironment env) {
        super(env);
        range = new Range(trainFilePath[0]);
    }
    @Override
    public List<Integer> getTrainResultById(int dataId) {
//        return trainResult.get(dataId).getVisibleObj();
        return range.getLine(dataId);
    }
}
