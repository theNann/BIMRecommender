package compression;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Range {
    private HashMap<Integer,List<Integer>> trainResult;
    private String filePath;

    public Range(String path) {
        trainResult = new HashMap<Integer,List<Integer>>();
        filePath = path;
        readTrainResult();
    }

    public void readTrainResult(){
//        for(int i = 0; i < SceneInfo.getTrainSize(); i++) {
//            trainResult.put(i, new Result(i, huffman.getLine(i)));
//        }
        File file = new File(filePath);
        DataInputStream dis = null;
        try {
            dis = new DataInputStream(new FileInputStream(file));
            int dataId = 0;
            byte[] byteSize = new byte[2];
            while (true) {
                //dataId不存在二进制文件中，由逻辑处理
                int res = dis.read(byteSize, 0, 2);
                if (res == -1) {
                    break;
                }
                int size = ((byteSize[0] & 0xff) << 8) | (byteSize[1] & 0xff);
                byte[] nums = new byte[size * 2];
                dis.read(nums, 0, size * 2);
                List<Integer> visibleObj = new ArrayList<Integer>();
                for (int i = 0; i < size * 2 - 1; i += 2) {
                    int tmp = ((nums[i] & 0xff) << 8) | (nums[i + 1] & 0xff);
                    visibleObj.add(tmp);
                }
                trainResult.put(dataId++, visibleObj);
            }
            dis.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }

//        for(int i = 0; i < 10; i++) {
//            System.out.println(trainResult.get(i).size());
//        }
    }

    public List<Integer> getLine(int dataId) {
        List<Integer> line = new ArrayList<>();
        List<Integer> rangeResult = trainResult.get(dataId);
        for(int i = 0; i < rangeResult.size(); i++) {
            if(i == 0) {
                line.add(rangeResult.get(0));
                continue;
            }
            int len = line.size();
            int left = line.get(len - 1);
            int right = rangeResult.get(i);
            if (right > left) {
                line.add(right);
            } else {
                line.remove(len - 1);
                for (int j = right; j <= left; j++) {
                    line.add(j);
                }
            }
        }
        return line;
    }
}
