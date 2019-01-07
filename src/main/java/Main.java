import dao.PrimitiveData;
import dao.Result;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import recommendation.*;
import tools.Configuration;
import java.util.ArrayList;
import java.util.List;

import static tools.Tools.calScoresAvg;

//TODO:用新的压缩代码重新生成压缩文件，并对比论文中的压缩数据，看差异(差异很小，可不修改实验数据)
//TODO:用解压缩的方式重新连接OcclusionCulling进行测试，并考虑为什么OutOfMemory(原因是每次调用getline都重新遍历huffman树，可事先存储)
//TODO:考虑矩阵分解
//TODO:整体Review代码，为答辩做准备

public class Main {
    public static void generaterBuffer(byte[] buffer, int idx, int data) {
        buffer[idx*4] = (byte) (data & 0xff);
        buffer[idx*4+1] = (byte) ((data >> 8 ) & 0xff);
        buffer[idx*4+2] = (byte) ((data >> 16 ) & 0xff);
        buffer[idx*4+3] = (byte) ((data >> 24 ) & 0xff);
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration.getInstance().setIp("10.222.178.213");
        Configuration.getInstance().setReadPort(6001);
        Configuration.getInstance().setWritePort(6002);
//        int trainLen = 38;
//        String[] trainDataPath = new String[trainLen];
//        for(int i = 0; i < trainLen; i++) {
//            trainDataPath[i] = "E:\\DataSet\\GridData_5\\" + "gridData" + i + ".csv";
//        }
        String[] trainDataPath = {
//                "/home/pyn/Desktop/DataSet/train_random/random.csv",
//                "/home/pyn/Desktop/DataSet/train_random/data_train_all/data_train_0.csv",
//                "/home/pyn/Desktop/DataSet/train_random/data_train_all/data_train_1.csv",
//                "/home/pyn/Desktop/DataSet/train_random/data_train_1.csv"
        };
        String[] testDataPath = {
//                "/home/pyn/Desktop/DataSet/NewData/test_data_last.csv",
//                "/home/pyn/Desktop/DataSet/TestData/test_data.csv",
//                "/home/pyn/Desktop/DataSet/NewData/data1.csv",//该csv的dataid需要接着上一个
                //"E:\\DataSet\\TestData\\test_data.csv",
//                "E:\\DataSet\\TestData\\test_data_1.csv"
        };

        String[] trainTargetPath = {
//                "/home/pyn/Desktop/DataSet/GridData_10/binRaw.dat",
                "/home/pyn/Desktop/DataSet/GridData_10/huff.dat",
//                "/home/pyn/Desktop/DataSet/GridData_10/binRange.dat",
//                "/home/pyn/Desktop/DataSet/train_random/random.txt",
//                "/home/pyn/Desktop/DataSet/train_random/target_train_1.txt"
        };
        String[] testTargetPath = {
                // "E:\\DataSet\\TestData\\test_target.txt",
//                "E:\\DataSet\\TestData\\test_target_1.txt"
//                "/home/pyn/Desktop/DataSet/NewData/test_target_last.txt",
//                "/home/pyn/Desktop/DataSet/TestData/test_target.txt",
//                "/home/pyn/Desktop/DataSet/NewData/target1.txt",
//                "/home/pyn/Desktop/DataSet/NewData/target1.txt",
        };
        Configuration.getInstance().setTrainDataPath(trainDataPath);
        Configuration.getInstance().setTestDataPath(testDataPath);
        Configuration.getInstance().setTrainTargetPath(trainTargetPath);
        Configuration.getInstance().setTestTargetPath(testTargetPath);
        Configuration.getInstance().setKnnWriteToFile("/home/pyn/Desktop/DataSet/knnScore.csv");

        ExecutionEnvironment env;
        ParameterTool params;
        params = ParameterTool.fromArgs(args);
        env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        PrepareData prepareData = PrepareData.getInstance(env);
        ResultProvider resultProvider = new CompressedBinaryProvider(env);
        long prepareTime = System.currentTimeMillis();
        //将所有训练数据的结果存入到二进制文件中
//        int objCnt = Tools.writeToBinary(prepareResult.getTrainResult(), "E:\\DataSet\\GridData_5\\train_target.dat");
//        System.out.println("ObjCnt : " + objCnt);

        //        Configuration.getInstance().setKnnPositionk(48);
//        Configuration.getInstance().setKnnDirectionk(2);
//        www.pyn.tools.Tools.Configuration.getInstance().setReck(2);
//        www.pyn.tools.Tools.Configuration.getInstance().setRecHowMany(3);
//        www.pyn.tools.Tools.Configuration.getInstance().setCFHowMany(4);

        Knn knn = new Knn(params, env, prepareData, resultProvider);
//        Recommender recommender = new Recommender(params, env, prepareData, prepareResult);
//        CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering(params, env, prepareData, prepareResult);

        List<Tuple3<Integer, Double, Double>> avgs = new ArrayList<Tuple3<Integer, Double, Double>>();

//        DataSet<Tuple3<Integer, Double, Double>> scores = collaborativeFiltering.solveCollaborativeFiltering();
//        recommender.solveRecommender();
        for(int k = 5; k <= 5; k += 2)
        {
//            Configuration.getInstance().setReck(1);
//            Configuration.getInstance().setRecHowMany(k);
//            recommender.solveRecommender();
            Configuration.getInstance().setKnnPositionk(k);
            Configuration.getInstance().setKnnDirectionk(k);
//            DataSet<Tuple3<Integer, Double, Double>> scores = knn.solveKnn();
//            Tools.expandTrainSet(scores, prepareData.getTestData(), prepareResult.getTestResult(),5243);
//            Tools.removeTestData(scores, prepareData.getTestData(), prepareResult.getTestResult());
//            Tuple3<Integer, Double, Double> avg = calScoresAvg(scores);
//            avgs.add(avg);
        }

        for(int i = 0; i < avgs.size(); i++) {
            System.out.println(avgs.get(i).f0 + " " + avgs.get(i).f1 + " " + avgs.get(i).f2);
        }
//        List<Integer> raw;
//        for(int i = 0; i < 1000; i++) {
//            System.out.print(i);
//            for(int j = 0; j < 70000; j++) {
//                raw = resultProvider.getTrainResultById(i);
////                System.out.print(raw.size() + " ");
//                //            for(int j = 0; j < raw.size(); j++) {
//                //                System.out.print(raw.get(j) + " ");
//                //            }
//                //            System.out.println();
//            }
//        }

        long endTime = System.currentTimeMillis();
        System.out.println("Prepare time: " + (prepareTime-startTime)*1.0/1000+"s");
        System.out.println("Cal time: " + (endTime-prepareTime)*1.0/1000 + "s ,Data size: " + prepareData.getTestData().size());
//        System.out.println("train: data, result " + prepareData.getTrainMapData().size()+ " " +prepareResult.getTrainResult().size()); //72000
//        System.out.println("test: data, result " + prepareData.getTestData().size()+ " " + prepareResult.getTestResult().size()); //5809, time:3.359s
        // System.out.println("grid size : " + prepareData.getTrainData().length + " " +prepareData.getTrainData()[0].length + " " + prepareData.getTrainData()[0][0].length);


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //与OcclusionCulling交互
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        String ip = Configuration.getInstance().getIp();
        int readPort = Configuration.getInstance().getReadPort();
        int writePort = Configuration.getInstance().getWritePort();
        DataStream<byte[]> bytes = senv.addSource(new SocketByteStreamFunction(ip, readPort, 18*4,0L));

        DataStream<PrimitiveData> primitiveDataDataStream = bytes.flatMap(new FlatMapFunction<byte[], PrimitiveData>() {
            public void flatMap(byte[] bytes, Collector<PrimitiveData> collector) throws Exception {
                PrimitiveData primitiveData = PrimitiveData.primitiveDataFromBytes(bytes);
                collector.collect(primitiveData);
            }
        });

        DataStream<Result> result = primitiveDataDataStream.flatMap(new Knn.knnMap());
        //本文实现了一个基于Flink流处理引擎对观察视点的可见物体进行推荐的服务器，开源项目OcclusionCulling提供可视化平台，相当于客户端。本系统基于Flink获取在OcclusionCulling的场景中随机游走时观察视点的相关流式数据，将推荐算法的思想应用到可见性问题中，基于这些数据对观察视点的可见物体对象集合进行实时推荐，将推荐结果以流数据的形式发送到OcclusionCulling进行渲染实现可视化。
        result.writeToSocket(ip, writePort, new SerializationSchema<Result>() {
            public byte[] serialize(Result re) {
                int len = re.getVisibleObj().size()+1;
//                System.out.println("len : " + len);
                byte[] buffer = new byte[(len+1)*4];
                generaterBuffer(buffer, 0, len);
                generaterBuffer(buffer, 1, re.getDataId());
                for(int i = 0; i < re.getVisibleObj().size(); i++) {
                    int tmp = re.getVisibleObj().get(i);
                    generaterBuffer(buffer, i+2, tmp);
                }
//                File file = new File("/home/pyn/Desktop/out.txt");
//                FileOutputStream in;
//                try {
//                    in = new FileOutputStream(file);
//                    String start = "start ";
//                    in.write(start.getBytes());
//                    for(int i = 0; i < re.getVisibleObj().size(); i++) {
//                        int tmp = re.getVisibleObj().get(i);
//                        byte[] bt = String.valueOf(tmp).concat(", ").getBytes();
//                        in.write(bt, 0 ,bt.length);
//                        generaterBuffer(buffer, i+2, tmp);
//                    }
//                    in.close();
//                } catch (FileNotFoundException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }

//                byte[] buffer = new byte[4*3];
//                generaterBuffer(buffer, 0, 2);
//                generaterBuffer(buffer, 1, 0);
//                generaterBuffer(buffer, 2, 1);

                return buffer;
            }
        });
        senv.execute("test");
//////////////////////////////////////////////////////////////////////////////////////////////////////////







//        final ParameterTool params = ParameterTool.fromArgs(args);
//        // set up the execution environment
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        // make parameters available in the web interface
//        env.getConfig().setGlobalJobParameters(params);
//
//        List<Position> trainPosition = null;
//        System.out.println("Input!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//
//        DataSet<Position> dataTrainPositionDataSet = env.readCsvFile("E:\\pyn_playground\\flink_data.csv")
//                .pojoType(Position.class, "dataId", "px", "py", "pz");
//        trainPosition = dataTrainPositionDataSet.collect();
        // 也可以通过如下方式得到dataTrainPosition，即先读为Tuple4，然后通过map转化为Dataset<position>
//        final DataSet<Tuple4<Integer,Integer,Integer,Integer>> dataTrainPositionDataSet =
//                env.readCsvFile("E:\\pyn_playground\\flink_data.csv").
//                types(Integer.class, Integer.class, Integer.class, Integer.class);

//        trainPosition = dataTrainPositionDataSet.flatMap(new FlatMapFunction<Tuple4<Integer,Integer,Integer,Integer>, Position>() {
//            public void flatMap(Tuple4<Integer,Integer,Integer,Integer> tuple4, Collector<Position> out) {
//                out.collect(new Position(tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3));
//                System.out.println(tuple4.f0 +" " + tuple4.f1 + " " + tuple4.f2 + " " + tuple4.f3 + "MAPPPPPPPPPPPPPP");
//            }
//        }).collect();





//        DataSet<String> text = env.fromElements("hello world");
//        DataSet<Tuple2<String, Integer>> counts =
//                // split up the lines in pairs (2-tuples) containing: (word,1)
//                text.flatMap(new SocketWordCount.Tokenizer())
//                        // group by the tuple field "0" and sum up tuple field "1"
//                        .groupBy(0)
//                        .sum(1);
//
//        if (params.has("output")) {
//            counts.writeAsCsv(params.get("output"), "\n", " ");
//            // execute program
//            env.execute("Knn");
//        } else {
//            System.out.println("Printing result to stdout. Use --output to specify output path.");
//            counts.print();
//        }
    }
}

