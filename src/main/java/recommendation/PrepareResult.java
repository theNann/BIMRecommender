package recommendation;

import compression.Huffman;
import dao.Result;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import tools.Configuration;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class PrepareResult {
    private ExecutionEnvironment env;
    private HashMap<Integer,Result> trainResult;
    private HashMap<Integer,Result> testResult;
    private String[] trainFilePath;
    private String[] testFilePath;
    private static PrepareResult prepareResult = null;
    private Huffman huffman;

    private PrepareResult(ExecutionEnvironment env) {
        this.env = env;
        this.trainFilePath = Configuration.getInstance().getTrainTargetPath();
        this.testFilePath = Configuration.getInstance().getTestTargetPath();
        trainResult = new HashMap<Integer, Result>();
        testResult = new HashMap<Integer, Result>();
//        readTrainResult();
        readTrainResultByCompress();
        readTestResult();
    }

    public static PrepareResult getInstance(ExecutionEnvironment env) {
        if(prepareResult == null) {
            prepareResult = new PrepareResult(env);
        }
        return prepareResult;
    }

    //PrepareResult
    public void readTrainResultByCompress() {
        huffman = new Huffman(trainFilePath[0]);
        huffman.decode();
    }

    public List<Integer> getResultByCompress(int dataId) {
       return huffman.getline(dataId);
//        List<Integer> raw = new ArrayList<>();
//        return raw;
    }

    public void readTrainResult() {
        if(trainFilePath == null || trainFilePath.length == 0) {
            return ;
        }
        // 读二进制数据
        DataInputStream dis = null;
        for(int fileIdx = 0; fileIdx < trainFilePath.length; fileIdx += 1) {
            File file = new File(trainFilePath[fileIdx]);
            //        int cnt = -1;
            try {
                dis = new DataInputStream(new FileInputStream(file));
                int dataId = 0;
                byte[] byteSize = new byte[2];
                while (true) {
                    //                if(++cnt == 5) {
                    //                    break;
                    //                }
                    //dataId不存在二进制文件中，由逻辑处理
                    int res = dis.read(byteSize, 0, 2);
                    if (res == -1) {
                        break;
                    }
                    int size = ((byteSize[0] & 0xff) << 8) | (byteSize[1] & 0xff);
//                    int size = dis.readShort();
                    //                System.out.println("dataId , size : " + dataId + " , " + size) ;
                    //                System.out.print("dataId : " + dataId);
                    byte[] nums = new byte[size * 2];
                    dis.read(nums, 0, size * 2);
                    List<Integer> visibleObj = new ArrayList<Integer>();
                    //                System.out.print("tmp : ");
                    for (int i = 0; i < size * 2 - 1; i += 2) {
                        int tmp = ((nums[i] & 0xff) << 8) | (nums[i + 1] & 0xff);
                        visibleObj.add(tmp);
                        //                    System.out.print(tmp + " ");
                    }
                    //                System.out.println();
                    trainResult.put(dataId, new Result(dataId++, visibleObj));
                }
                dis.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
            }
        }

        //读取txt文件
//        this.trainResult = readResult(trainFilePath);
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
            DataSet<Result> ds = lines.flatMap(new LinesMap());
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
    public HashMap<Integer,Result> getTrainResult() {
        return trainResult;
    }

    public HashMap<Integer,Result> getTestResult() {
        return testResult;
    }
}
