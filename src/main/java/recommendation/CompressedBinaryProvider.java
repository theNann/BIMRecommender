package recommendation;

import compression.Huffman;
import dao.Result;
import dao.SceneInfo;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.HashMap;
import java.util.List;

public class CompressedBinaryProvider extends ResultProvider{
    private Huffman huffman;
//    private HashMap<Integer,Result> trainResult;

    public CompressedBinaryProvider(ExecutionEnvironment env) {
        super(env);
//        trainResult = new HashMap<Integer, Result>();
        huffman = new Huffman(trainFilePath[0]);
//        readTrainResult();
    }

//    public void readTrainResult(){
//        for(int i = 0; i < SceneInfo.getTrainSize(); i++) {
//            trainResult.put(i, new Result(i, huffman.getLine(i)));
//        }
//    }
    @Override
    public List<Integer> getTrainResultById(int dataId) {
        return huffman.getline(dataId);
    }
}
