package recommendation;

import dao.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import tools.Configuration;
import tools.Tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Recommender {
    //    private static HashMap<Integer, Position> trainPosition;
//    private static HashMap<Integer, Direction> trainDirection;
    private DataSet<PrimitiveData> testDataDS;
    private HashMap<Integer, PrimitiveData> testData;
    private static HashMap<Integer, PrimitiveData> trainData;

    private static HashMap<Integer,Result> trainResult;
    private static HashMap<Integer,Result> testResult;

    private ExecutionEnvironment env;
    private ParameterTool params;
    private String writoTofile = "/home/pyn/Desktop/DataSet/RecomScore.csv";

    public Recommender(ParameterTool params, ExecutionEnvironment env, PrepareData prepareData,
                       PrepareResult prepareResult) {
        this.params = params;
        this.env = env;

//        trainPosition = prepareData.getTrainPosition();
//        trainDirection = prepareData.getTrainDirection();
        testDataDS = prepareData.getTestDataDS();
        testData = prepareData.getTestData();
        trainData = prepareData.getTrainMapData();
        trainResult = prepareResult.getTrainResult();
        testResult = prepareResult.getTestResult();
    }

    public void solveRecommender() {
        DataSet<Result> ans = testDataDS.flatMap(new recommenderMap());
        DataSet<Tuple3<Integer, Double, Double>> scores = ans.flatMap(new scoreMap());
        scores.writeAsCsv(writoTofile,"\n",",", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        try {
            env.execute("FlinkScores");
        } catch (Exception e) {}
    }

    public static final class recommenderMap implements FlatMapFunction<PrimitiveData, Result> {
        public void flatMap(PrimitiveData primitiveData, Collector<Result> collector) throws Exception {
            int k = Configuration.getInstance().getReck();
            int howMany = Configuration.getInstance().getRecHowMany();
            int maxK = 15;
            int dataId = primitiveData.getDataId();
            Position position = primitiveData.getPosition();
            Direction direction = primitiveData.getDirection();
            List<Integer> visibleObjList = new ArrayList<Integer>();
            visibleObjList.clear();
            //KNN
            List<SimilarityTuple> kNearestNeighbors = Tools.getNearestNeighbors(trainData, position, direction,
                    0, k, 15);
            //CollaborativeFiltering
            for(int i = 0; i < kNearestNeighbors.size(); i++) {
                int simId = kNearestNeighbors.get(i).dataId;
                List<Integer> visibleObj = trainResult.get(simId).getVisibleObj();
                List<SimilarityTuple> recommendNearestNeighbors = Tools.userBasedRecommend(trainResult, visibleObj, howMany);
                for(int j = 0; j < recommendNearestNeighbors.size(); j++) {
                    int _simId = recommendNearestNeighbors.get(j).dataId;
                    List<Integer> _visibleObj = trainResult.get(_simId).getVisibleObj();
                    visibleObjList.addAll(_visibleObj);
                }
            }
            collector.collect(new Result(dataId, Tools.removeDuplicateFromList(visibleObjList)));
        }
    }

    public static final class scoreMap implements FlatMapFunction<Result, Tuple3<Integer, Double, Double>> {
        public void flatMap(Result result, Collector<Tuple3<Integer, Double, Double>> collector) throws Exception {
            List<Integer> preditcVisibleObj = result.getVisibleObj();
            int dataId = result.getDataId();
            System.out.println("testDataId : " + dataId);
            List<Integer> targetVisibleObj = testResult.get(dataId).getVisibleObj();
            int jiaoSize = Tools.intersection(preditcVisibleObj, targetVisibleObj);
            double acc = jiaoSize*1.0 / preditcVisibleObj.size();
            double recall = jiaoSize*1.0 / targetVisibleObj.size();
            collector.collect(new Tuple3<Integer, Double, Double>(dataId, acc, recall));
        }
    }
}
