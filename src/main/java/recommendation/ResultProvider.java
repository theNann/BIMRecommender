package recommendation;

import compression.Huffman;
import dao.Result;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import tools.Configuration;

import java.util.*;

public class ResultProvider {
    protected ExecutionEnvironment env;
    protected HashMap<Integer,Result> testResult;
    protected String[] testFilePath;
    protected String[] trainFilePath;

    public ResultProvider()
    {

    }

    public ResultProvider(ExecutionEnvironment env) {
        this.env = env;
        this.testFilePath = Configuration.getInstance().getTestTargetPath();
        this.trainFilePath = Configuration.getInstance().getTrainTargetPath();
        testResult = new HashMap<Integer, Result>();
        readTestResult();
    }

    public void readTestResult() {
        this.testResult = readResult(testFilePath);
    }

    public HashMap<Integer,Result> readResult(String[] filePath) {
        HashMap<Integer, Result> hashMap = new HashMap<Integer, Result>();
        hashMap.clear();
        if(filePath == null || filePath.length == 0) {
            return hashMap;
        }
        for(int idx = 0; idx < filePath.length; idx += 1) {
            System.out.println(filePath[idx]);
            DataSet<String> lines = env.readTextFile(filePath[idx]);
            DataSet<Result> ds = lines.flatMap(new PrepareResult.LinesMap());
//            DataSet<Result> ds = lines.flatMap(new FlatMapFunction<String, Result>() {
//                @Override
//                public void flatMap(String s, Collector<Result> collector) throws Exception {
//                    String[] split = s.split(", ");
//                    int dataId = Integer.valueOf(split[0]);
//                    List<Integer> visibleObj = new ArrayList<Integer>();
//                    for (int i = 1; i < split.length; i++) {
//                        visibleObj.add(Integer.valueOf(split[i]));
//                    }
//                    Collections.sort(visibleObj);
//                    Result result = new Result(dataId, visibleObj);
//                    collector.collect(result);
//                }
//            });
            try {
                List<Result> rs = ds.collect();
                for (int i = 0; i < rs.size(); i++) {
                    hashMap.put(rs.get(i).getDataId(), rs.get(i));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return hashMap;
    }

    public static final class LinesMap implements FlatMapFunction<String, Result> {
        public void flatMap(String s, Collector<Result> collector) throws Exception {
            String[] split = s.split(", ");
            int dataId = Integer.valueOf(split[0]);
            List<Integer> visibleObj = new ArrayList<Integer>();
            for (int i = 1; i < split.length; i++) {
                visibleObj.add(Integer.valueOf(split[i]));
            }
            Collections.sort(visibleObj);
            Result result = new Result(dataId, visibleObj);
            collector.collect(result);
        }
    }

    public HashMap<Integer,Result> getTestResult() {
        return testResult;
    }

    public List<Integer> getTrainResultById(int dataId)
    {
        return new ArrayList<Integer>();
    }
}
