package recommendation;

import dao.PrimitiveData;
import dao.Result;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import tools.Tools;

import java.util.HashMap;
import java.util.List;

public class CollaborativeFiltering {
    //    private static HashMap<Integer, Position> trainPosition;
//    private static HashMap<Integer, Direction> trainDirection;
    private DataSet<PrimitiveData> testDataDS;
    private HashMap<Integer, PrimitiveData> testData;

    private static HashMap<Integer,Result> trainResult;
    private static HashMap<Integer,Result> testResult;


    private ExecutionEnvironment env;
    private ParameterTool params;
    private String writoTofile = "/home/pyn/Desktop/DataSet/CFScore.csv";

    public CollaborativeFiltering(ParameterTool params, ExecutionEnvironment env, PrepareData prepareData,
                                  PrepareResult prepareResult) {
        this.params = params;
        this.env = env;

//        trainPosition = prepareData.getTrainPosition();
//        trainDirection = prepareData.getTrainDirection();
        testDataDS = prepareData.getTestDataDS();
        testData = prepareData.getTestData();

        trainResult = prepareResult.getTrainResult();
        testResult = prepareResult.getTestResult();
    }

    public DataSet<Tuple3<Integer, Double, Double>> solveCollaborativeFiltering() {
        DataSet<Result> ans = testDataDS.flatMap(new recommenderMap());
        DataSet<Tuple3<Integer, Double, Double>> scores = ans.flatMap(new scoreMap());
        scores.writeAsCsv(writoTofile,"\n",",", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        try {
            env.execute("FlinkScores");
        } catch (Exception e) {

        } finally {
            return scores;
        }
    }

    public static final class scoreMap implements FlatMapFunction<Result, Tuple3<Integer,Double, Double>> {
        public void flatMap(Result result, Collector<Tuple3<Integer, Double, Double>> collector) throws Exception {
            int dataId = result.getDataId();
            List<Integer> preditcVisibleObj = result.getVisibleObj();
            System.out.println("testDataId : " + dataId);
            List<Integer> targetVisibleObj = testResult.get(dataId).getVisibleObj();
            int jiaoSize = Tools.intersection(preditcVisibleObj, targetVisibleObj);
            double acc = jiaoSize*1.0 / preditcVisibleObj.size();
            double recall = jiaoSize*1.0 / targetVisibleObj.size();
            collector.collect(new Tuple3<Integer, Double, Double>(dataId, acc, recall));
        }
    }

    public static final class recommenderMap implements FlatMapFunction<PrimitiveData, Result> {
        public void flatMap(PrimitiveData primitiveData, Collector<Result> collector) throws Exception {
//            int dataId = primitiveData.getDataId();
//            Position position = primitiveData.getPosition();
//            Direction direction = primitiveData.getDirection();
//            List<SimilarityTuple> coldStartNearestNeighbor = Tools.getNearestNeighbors(trainPosition, position, 1,
//                    1, 15, trainDirection, direction);
//            List<Integer> visibleObjList = new ArrayList<Integer>();
//            visibleObjList.clear();
//            int coldStartDataId = coldStartNearestNeighbor.get(0).dataId;
//            for(int i = 0; i < coldStartNearestNeighbor.size(); i++) {
//                int simId = coldStartNearestNeighbor.get(i).dataId;
//                Result rs = trainResult.get(simId);
//                visibleObjList.addAll(rs.getVisibleObj());
//            }
//            Collections.sort(visibleObjList);
//            int howMany = Configuration.getInstance().getCFHowMany();
//            List<SimilarityTuple> recommendNearestNeighbors = Tools.userBasedRecommend(trainResult, visibleObjList, howMany);
//            for(int i = 0; i < recommendNearestNeighbors.size(); i++) {
//                int simId = recommendNearestNeighbors.get(i).dataId;
//                if(simId == coldStartDataId) {
//                    continue;
//                }
//                Result rs = trainResult.get(simId);
//                visibleObjList.addAll(rs.getVisibleObj());
//            }
//            //去重
//            List<Integer> visibleObjListNoDuplicate = Tools.removeDuplicateFromList(visibleObjList);
//            collector.collect(new Result(dataId, new ArrayList<Integer>(visibleObjListNoDuplicate)));
        }
    }
}
