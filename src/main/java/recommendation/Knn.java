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

import java.util.*;

@SuppressWarnings("serial")
// 目前使用Knn-8的方法
public class Knn {
//    private static HashMap<Integer, Position> trainPosition;
//    private static HashMap<Integer, Direction> trainDirection;

    private DataSet<PrimitiveData> testDataDS;

    private static HashMap<Integer, PrimitiveData> testData;
    private static HashMap<Integer, PrimitiveData> trainData;

    private static HashMap<Integer,Result> testResult;
    private static ResultProvider resultProvider;

    private ExecutionEnvironment env;
    private ParameterTool params;

    public Knn(ParameterTool params, ExecutionEnvironment env, PrepareData prepareData,
               ResultProvider _resultProvider) {
        this.params = params;
        this.env = env;
        trainData = prepareData.getTrainMapData();
        testDataDS = prepareData.getTestDataDS();
        testData = prepareData.getTestData();

        resultProvider = _resultProvider;
        testResult = resultProvider.getTestResult();
    }

    public DataSet<Tuple3<Integer, Double, Double>> solveKnn() {
        DataSet<Result> ans = testDataDS.flatMap(new knnMap());
//        if (params.has("output")) {
//            System.out.println("output : " + params.get("output"));
//            ans.writeAsText(params.get("output"),FileSystem.WriteMode.OVERWRITE);
//            try {
//                env.execute("Knn");
//            } catch (Exception e) {}
//        } else {
//            System.out.println("Printing result to stdout. Use --output to specify output path.");
//            try {
//                ans.print();
//            } catch (Exception e) {}
//        }

        DataSet<Tuple3<Integer, Double, Double>> scores = ans.flatMap(new scoreMap());


//        HashMap<Integer, Result> testTarget = prepareResult.getTestResult();
//        System.out.println("testTarget size : " + testTarget.size());
//        HashMap<Integer, PrimitiveData> testData = prepareData.getTestData();
//        System.out.println("testData size : " + testData.size());
//        www.pyn.tools.Tools.expandTrainSet(scores, testData, testResult, 13674);

        scores.writeAsCsv(Configuration.getInstance().getKnnWriteToFile(),"\n",",", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        try {
            env.execute("FlinkScores");
        } catch (Exception e) {

        }finally {
            return scores;
        }
    }

    public static final class scoreMap implements FlatMapFunction<Result, Tuple3<Integer,Double, Double>> {
        public void flatMap(Result result, Collector<Tuple3<Integer, Double, Double>> collector) throws Exception {
            int dataId = result.getDataId();
            List<Integer> preditcVisibleObj = result.getVisibleObj();
//            System.out.println("testDataId : " + dataId);
            List<Integer> targetVisibleObj = testResult.get(dataId).getVisibleObj();
            int jiaoSize = Tools.intersection(preditcVisibleObj, targetVisibleObj);
            double acc = 1;
            if(preditcVisibleObj.size() != 0) {
                acc = jiaoSize * 1.0 / preditcVisibleObj.size();
            }
            double recall = 1;
            if(targetVisibleObj.size() != 0 ) {
                recall = jiaoSize * 1.0 / targetVisibleObj.size();
            }
            collector.collect(new Tuple3<Integer, Double, Double>(dataId, acc, recall));
        }
    }

    public static final class knnMap implements FlatMapFunction<PrimitiveData, Result> {
        public void flatMap(PrimitiveData primitiveData, Collector<Result> collector) throws Exception {
            int dataId = primitiveData.getDataId();
            Position position = primitiveData.getPosition();
            Direction direction = primitiveData.getDirection();
            // System.out.println("testDataId : " + dataId);
            List<Integer> visibleObjList = new ArrayList<Integer>();
            Set<Integer> visibleObjectSet = new HashSet<Integer>();
            visibleObjList.clear();

            //优化前
//            int positionK = Configuration.getInstance().getKnnPositionk();
//            int directionK = Configuration.getInstance().getKnnDirectionk();
//            List<SimilarityTuple> kNearestNeighbors = Tools.getNearestNeighbors(trainData, position, direction,
//                     1, directionK, 15);
//
//            for(int i = 0; i < kNearestNeighbors.size(); i++) {
//                int simId = kNearestNeighbors.get(i).dataId;
////                System.out.println(dataId + " " + simId + " " + kNearestNeighbors[i].simlarity);
//                Result rs = trainResult.get(simId);
//                //并集
//                visibleObjList.addAll(rs.getVisibleObj());
//                //交集
////                if(visibleObjectSet.size() == 0) {
////                    visibleObjectSet.addAll(rs.getVisibleObj());
////                } else {
////                    visibleObjectSet.retainAll(rs.getVisibleObj());
////                }
//            }

            //优化后
            int gridX = SceneInfo.ToGridX(position.getPx());
            int gridY = SceneInfo.ToGridY(position.getPy());
            int gridZ = SceneInfo.ToGridZ(position.getPz());
//            List<GridData> neighbors7 = SceneInfo.nearestNeighbors7(gridX, gridY, gridZ, trainData);
//            List<GridData> neighbors2 = SceneInfo.nearestNeighbors2(gridX, gridY, gridZ, position, trainData);
            List<Integer> trainDataId = SceneInfo.nearestNeighbors1(gridX, gridY, gridZ, position, direction, 2);
//            List<Integer> trainDataId = SceneInfo.nearestNeighbors2(gridX, gridY, gridZ, position, direction, 2);
            for(int i = 0; i < trainDataId.size(); i++) {
                int id = trainDataId.get(i);
                visibleObjList.addAll(resultProvider.getTrainResultById(id));
            }
            //优化后

            collector.collect(new Result(dataId, Tools.removeDuplicateFromList(visibleObjList)));
//            collector.collect(new Result(dataId, new ArrayList<>(visibleObjectSet)));
//            System.out.println("dataId : " + dataId);
        }
    }

//    public static HashMap<Integer, Position> getTrainPosition() {
//        return trainPosition;
//    }
//
//    public static void setTrainPosition(HashMap<Integer, Position> trainPosition) {
//        Knn.trainPosition = trainPosition;
//    }
//
//    public static HashMap<Integer, Direction> getTrainDirection() {
//        return trainDirection;
//    }
//
//    public static void setTrainDirection(HashMap<Integer, Direction> trainDirection) {
//        Knn.trainDirection = trainDirection;
//    }
}
