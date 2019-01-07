package recommendation;

import dao.Result;
import org.apache.flink.api.java.ExecutionEnvironment;
import tools.Configuration;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RawBinaryProvider extends ResultProvider{
    private HashMap<Integer,Result> trainResult;

    public RawBinaryProvider(ExecutionEnvironment env)
    {
        super(env);
        trainResult = new HashMap<Integer, Result>();
        readTrainResult();
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

    @Override
    public List<Integer> getTrainResultById(int dataId) {
       return trainResult.get(dataId).getVisibleObj();
    }
}
