package tools;

import dao.*;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.util.*;

public class Tools {
    public static double vectorSimlarity(double[] v1, double[] v2) {
        ArrayRealVector vec1 = new ArrayRealVector(v1);
        ArrayRealVector vec2 = new ArrayRealVector(v2);
        double dotRes = vec1.dotProduct(vec2);
        double denorm = vec1.getNorm() * vec2.getNorm();
        double cos = dotRes / denorm;
        return 0.5 + 0.5 * cos;
    }

    public static double euclideanDistanceSim(double[] v1, double[] v2) {
        ArrayRealVector vec1 = new ArrayRealVector(v1);
        ArrayRealVector vec2 = new ArrayRealVector(v2);
        ArrayRealVector tmp = vec1.subtract(vec2);
        double dist = tmp.getNorm();
        double sim = 1.0 / (1.0+dist);
        return sim;
    }

    public static int intersection(List<Integer> sortList1, List<Integer> sortList2) {
//        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
//        for(int i = 0; i < list1.size(); i++) {
//            map.put(list1.get(i), 1);
//        }
//        int jiaoSize = 0;
//        for(int i = 0; i < list2.size(); i++) {
//            if(map.containsKey(list2.get(i))) {
//                jiaoSize += 1;
//            }
//        }
        int size1 = sortList1.size();
        int size2 = sortList2.size();
        if(size1 == 0 && size2 == 0) {
            return 0;
        }
        int jiaoSize = 0;
        int i = 0;
        int j = 0;
        while(i < size1 && j < size2) {
            if(sortList1.get(i).equals(sortList2.get(j))) {
                jiaoSize += 1;
                i += 1;
                j += 1;
            } else {
                if(sortList1.get(i) < sortList2.get(j)) {
                    i += 1;
                } else {
                    j += 1;
                }
            }
        }
        return jiaoSize;
    }

    public static double setSimilarity(List<Integer> sortList1, List<Integer> sortList2) {
        int size1 = sortList1.size();
        int size2 = sortList2.size();
        if(size1 == 0 && size2 == 0) {
            return 0;
        }
        int jiaoSize = 0;
        int i = 0;
        int j = 0;
        while(i < size1 && j < size2) {
            if(sortList1.get(i).equals(sortList2.get(j))) {
                jiaoSize += 1;
                i += 1;
                j += 1;
            } else {
                if(sortList1.get(i) < sortList2.get(j)) {
                    i += 1;
                } else {
                    j += 1;
                }
            }
        }
        int bingSize = size1 + size2 - jiaoSize;
        return jiaoSize*1.0 / bingSize;
    }

    public static List<SimilarityTuple> listSort(List<SimilarityTuple> list) {
        Collections.sort(list, new Comparator<SimilarityTuple>() {
            public int compare(SimilarityTuple o1, SimilarityTuple o2) {
                if(o2.similarityP > o1.similarityP) {
                    return 1;
                } else if(o2.similarityP == o1.similarityP){
                    return 0;
                } else {
                    return -1;
                }
            }
        });
        return list;
    }

    public static List<SimilarityTuple> getNearestNeighbors(HashMap<Integer, PrimitiveData> trainData, Position position
            , Direction direction, int considerDirectiton, int minK, int maxK) {
        int kk;
        if(considerDirectiton == 1) {
            kk = maxK;
        } else {
            kk = minK;
        }
        MinHeap minHeap = new MinHeap(kk);
        for(Map.Entry<Integer,PrimitiveData> entry : trainData.entrySet()) {
            int dataId = entry.getKey();
            Position trainPosition = entry.getValue().getPosition();
            double sim = Tools.euclideanDistanceSim(trainPosition.getPosition(), position.getPosition());
            if(minHeap.getCount() < kk) {
                minHeap.add(new SimilarityTuple(dataId, sim));
                if(minHeap.getCount() == kk) {
                    minHeap.buildHeap();
                }
            } else{
                if(sim > minHeap.arr[0].similarityP) {
                    minHeap.arr[0] = new SimilarityTuple(dataId, sim);
                    minHeap.adjustHeap(0);
                }
            }
        }

        List<SimilarityTuple> res = new ArrayList<SimilarityTuple>();
        if(considerDirectiton == 0) {
            for(int i = 0; i < minHeap.arr.length; i++) {
                res.add(minHeap.arr[i]);
            }
            return res;
        } else {
            //若考虑Direction，就对前maxK个数据进行simD和simP二维排序，选出simD前minK大的数据作为最后结果，
            // 同时也要去除那些direction夹角大于fov的数据（这个比较重要，能够提升recall），所以最后的结果个数小于等于minK。
            List<SimilarityTuple> similarityTuplesNew = new ArrayList<SimilarityTuple>();
            int maxSimPIndex = 0;
            double maxSimp = minHeap.arr[0].similarityP;

            for(int i = 0; i < minHeap.arr.length; i++) {
                int dataId = minHeap.arr[i].dataId;
                double simP = minHeap.arr[i].similarityP;
                double simD = Tools.vectorSimlarity(trainData.get(dataId).getDirection().getDirection(), direction.getDirection());
                similarityTuplesNew.add(new SimilarityTuple(dataId, simP, simD));
                if(maxSimp < simP) {
                    maxSimp = simP;
                    maxSimPIndex = i;
                }
            }

            Collections.sort(similarityTuplesNew, new Comparator<SimilarityTuple>() {
                public int compare(SimilarityTuple o1, SimilarityTuple o2) {
                    if(o2.similarityD > o1.similarityD) {
                        return 1;
                    } else if(o2.similarityD == o1.similarityD) {
                        if(o2.similarityP > o1.similarityP) {
                            return 1;
                        } else {
                            return -1;
                        }
                    } else {
                        return -1;
                    }
                }
            });

            res.clear();
            for(int i = 0; i < similarityTuplesNew.size(); i++) {
                if(similarityTuplesNew.get(i).similarityD < Math.cos(1.0472)) {
                    break;
                } else {
                    res.add(similarityTuplesNew.get(i));
                }
                if(res.size() == minK) {
                    break;
                }
            }
            if(res.size() == 0) {
                res.add(minHeap.arr[maxSimPIndex]);
            }
            return res;
        }
    }

    public static List<SimilarityTuple> userBasedRecommend(HashMap<Integer, Result> trainResult, List<Integer> predictVisibleObj,
                                                           int howMany) {
        MinHeap minHeap = new MinHeap(howMany);
        for(Map.Entry<Integer, Result> entry : trainResult.entrySet()) {
            int dataId = entry.getKey();
            List<Integer> visibleObj = entry.getValue().getVisibleObj();
            // 耗时的重点所在：计算两个较大集合的相似度,使用两个排序的list求交集能够提高效率
            double sim = Tools.setSimilarity(predictVisibleObj, visibleObj);
            //当只有一个sim时调用第一个构造函数，虽然是similarityP，这里也可认为是结果集合的相似性
            if(minHeap.getCount() < howMany) {
                minHeap.add(new SimilarityTuple(dataId, sim));
                if(minHeap.getCount() == howMany) {
                    minHeap.buildHeap();
                }
            } else{
                if(sim > minHeap.arr[0].similarityP) {
                    minHeap.arr[0] = new SimilarityTuple(dataId, sim);
                    minHeap.adjustHeap(0);
                }
            }
        }

        List<SimilarityTuple> userSimilarity = new ArrayList<SimilarityTuple>();
        for(int i = 0; i < minHeap.arr.length; i++) {
            userSimilarity.add(minHeap.arr[i]);
        }
        return userSimilarity;
    }

    public static double calF1(double acc, double recall) {
        return 2*acc*recall / (acc + recall);
    }

    public static void expandTrainSet(DataSet<Tuple3<Integer, Double, Double>> scores, HashMap<Integer,PrimitiveData> testData,
                                      HashMap<Integer, Result> testResult, int startDataId) {
        List<PrimitiveData> expandData = new ArrayList<PrimitiveData>();
        List<Result> expandResult = new ArrayList<Result>();
        try {
            List<Tuple3<Integer, Double, Double>> list = scores.collect();
            for(int i = 0; i < list.size(); i++) {
                if(list.get(i).f1 > 0.7 && list.get(i).f1 <= 0.8 && list.get(i).f2 > 0.7) {
                    int dataId = list.get(i).f0;
                    expandData.add(testData.get(dataId));
                    expandResult.add(testResult.get(dataId));
//                    if(expandData.size() >= 1000) {
//                        break;
//                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        String expandDataFile = "E:\\DataSet\\TestData\\test_data.csv";//"/home/pyn/Desktop/DataSet/tt.csv"
        String expandTargetFile = "E:\\DataSet\\TestData\\test_target.txt";//"/home/pyn/Desktop/DataSet/tt_result.txt"
        writeCSV2(expandData, expandDataFile, startDataId);
        writeTxt2(expandResult, expandTargetFile, startDataId);
    }


    public static void writeCSV2(List<PrimitiveData> dataList, String finalPath, int startDataId) {
        FileOutputStream out = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            File finalCSVFile = new File(finalPath);
            out = new FileOutputStream(finalCSVFile, true);
            osw = new OutputStreamWriter(out, "UTF-8");
            // 手动加上BOM标识
//            osw.write(new String(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }));
            bw = new BufferedWriter(osw);
            /**
             * 往CSV中写新数据
             */
//            String title = "";
//            title = "dataId,px,py,pz,dx,dy,dz";
//            bw.append(title).append("\r");

            if (dataList != null && !dataList.isEmpty()) {
                for (PrimitiveData data : dataList) {
                    bw.append((startDataId++) + ",");
                    bw.append(data.getPx() + ",");
                    bw.append(data.getPy() + ",");
                    bw.append(data.getPz() + ",");
                    bw.append(data.getDx() + ",");
                    bw.append(data.getDy() + ",");
                    bw.append(data.getDz() + ",");
                    bw.append(0 + ",");
                    bw.append(1 + ",");
                    bw.append(0 + ",");
                    bw.append("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                    bw = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (osw != null) {
                try {
                    osw.close();
                    osw = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out != null) {
                try {
                    out.close();
                    out = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        System.out.println(finalPath + "数据导出成功");
    }

    public static void writeTxt2(List<Result> dataList, String finalPath, int startDataId) {
        File file = new File(finalPath);
        BufferedWriter writer = null;
        try {
            if(file.isFile()&&!file.exists()){
                System.out.println("找不到指定的文件");
                file.createNewFile();// 不存在则创建
            }
            else{
                writer = new BufferedWriter(new FileWriter(file,true)); //这里加入true 可以不覆盖原有TXT文件内容 续写
                for(int i = 0; i < dataList.size(); i++) {
                    StringBuffer content = new StringBuffer("");
                    content.append(startDataId++);
                    for(int j = 0; j < dataList.get(i).getVisibleObj().size(); j++) {
                        content.append(", ");
                        content.append(dataList.get(i).getVisibleObj().get(j));
                    }
                    writer.write(new String(content));
                    writer.write("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void removeTestData(DataSet<Tuple3<Integer, Double, Double>> scores, HashMap<Integer, PrimitiveData> testData,
                                      HashMap<Integer, Result> testResult) {
        List<Tuple3<Integer, Double, Double>> scoresList;
        List<Integer> removeDataId = new ArrayList<Integer>();
        try {
            scoresList = scores.collect();
            for(int i = 0; i < scoresList.size(); i++) {
                if(scoresList.get(i).f1 < 0.5 || scoresList.get(i).f2 < 0.5) {
                    removeDataId.add(scoresList.get(i).f0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for(int i = 0; i < removeDataId.size(); i++) {
            int dataId = removeDataId.get(i);
            testData.remove(dataId);
            testResult.remove(dataId);
        }
        List<PrimitiveData> testDataList = new ArrayList<PrimitiveData>();
        List<Result> testResultList = new ArrayList<Result>();
        for(Map.Entry<Integer, PrimitiveData> entry : testData.entrySet()) {
            testDataList.add(entry.getValue());
        }
        for(Map.Entry<Integer, Result> entry : testResult.entrySet()) {
            testResultList.add(entry.getValue());
        }
        writeCSV2(testDataList, "/home/pyn/Desktop/DataSet/NewData/test_data_hhhh_2.csv", 0);
        writeTxt2(testResultList, "/home/pyn/Desktop/DataSet/NewData/test_target_hhhh_2.txt", 0);
    }

    public static List<Integer> removeDuplicateFromList(List<Integer> list) {
        Collections.sort(list);
        List<Integer> listNoDuplicate = new ArrayList<Integer>();
        if(list.size() == 0) {
            return listNoDuplicate;
        }
        listNoDuplicate.add(list.get(0));
        int idx = 0;
        for(int i = 0; i < list.size(); i++) {
            int tmp = list.get(i);
            if(tmp != listNoDuplicate.get(idx)) {
                listNoDuplicate.add(tmp);
                idx += 1;
            }
        }
        return listNoDuplicate;
    }

    public static Tuple3<Integer, Double, Double> calScoresAvg(DataSet<Tuple3<Integer, Double, Double>> scores) {
        double accSum = 0;
        double recallSum = 0;
        int len = 1;
        try {
            List<Tuple3<Integer, Double, Double>> scoresList = scores.collect();
            len = scoresList.size();
            for (int i = 0; i < len; i++) {
                accSum += scoresList.get(i).f1;
                recallSum += scoresList.get(i).f2;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Tuple3<Integer, Double, Double>(Configuration.getInstance().getKnnDirectionk(), accSum / len,
                recallSum / len);
    }

    public static int writeToBinary(HashMap<Integer,Result> result, String filePath) {
        int objCnt = 0;
        int cnt = 0;
        DataOutputStream dos = null;
        try{
            if(!new File(filePath).exists()){
                new File(filePath).createNewFile();
            }
            dos=new DataOutputStream(new FileOutputStream(new File(filePath)));
        }catch(Exception e){
            e.printStackTrace();
        }
        try {
            for (Map.Entry<Integer, Result> entry : result.entrySet()) {
                List<Integer> visibleObj = entry.getValue().getVisibleObj();
                dos.writeInt(entry.getKey());
                dos.writeShort(entry.getValue().getVisibleObj().size());

                for (int i = 0; i < visibleObj.size(); i++) {
                    if (visibleObj.get(i) > Short.MAX_VALUE) {
                        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@");
                        System.out.println(visibleObj.get(i));
                    }
                    dos.writeShort(visibleObj.get(i));
                    objCnt += 1;
                }
                System.out.println("cnt : " + cnt);
                cnt += 1;
            }
            dos.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return objCnt;
    }
}
