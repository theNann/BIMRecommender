package compression;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Huffman {
    private int[] _weight;
    public List<Integer> offsets = new ArrayList<Integer>();
    private Bitset body = new Bitset();
    private TreeNode root;
    private String filePath;
    private HashMap<Integer,List<Integer>> trainResult;

    public Huffman(String path) {
        _weight = new int[32768];
        for (int i = 0; i < 32768; i++) {
            _weight[i] = 0;
        }
        filePath = path;
        trainResult = new HashMap<Integer,List<Integer>>();
        decode();
        readTrainResult();
    }

    public TreeNode buildTree() {
        TreeNodeHeap queue = new TreeNodeHeap();
        for (int i = 0; i < 32768; i++) {
            if (_weight[i] > 0) {
                queue.push(new TreeNode((short)i, _weight[i]));
            }
        }
        while (queue.size() > 1) {
            TreeNode node1 = queue.pop();
            TreeNode node2 = queue.pop();
            queue.push(new TreeNode((short)0, node1.w + node2.w, node1, node2));
        }
        TreeNode root = queue.pop();
        return root;
    }

    public void decode() {
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + filePath + "!!!!!!!!!!!!!!!!");
        FileInputStream file = null;
        try {
            file = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        for (int i = 0; i < 32768; i++) {
            _weight[i] = FileHelper.readInt32(file);
        }
        root = buildTree();

        int offsetSize = FileHelper.readInt32(file);
        for (int i = 0; i < offsetSize; i++) {
            offsets.add(FileHelper.readInt32(file));
        }

        int bodySize = FileHelper.readInt32(file);
        body._data = new byte[bodySize];

        try {
            file.read(body._data, 0, bodySize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readTrainResult() {
        int gridNums = offsets.size() - 1;
        for(int i = 0; i < gridNums; i++) {
            trainResult.put(i, getLineFromHuffman(i));
        }

        //clear memory
        root = null;
        offsets.clear();
        _weight = null;
        body.clear();
    }

    public List<Integer> getline(int dataId) {
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

    //Huffman
    public List<Integer> getLineFromHuffman(int id) {
        TreeNode p = root;
        List<Integer> line = new ArrayList<Integer>();
        body._rpos = offsets.get(id);
        while (body._rpos < offsets.get(id + 1)) {
            if (body.read() == 0) {
                p = p.left;
            }
            else {
                p = p.right;
            }
            if (p.left == null && p.right == null) {
                int right = (int)p.n;
                line.add(right);
//                -Xmx5g
//                if(line.size() == 0) {
//                    line.add(right);
//                } else {
//                    int len = line.size();
//                    int left = line.get(len-1);
//                    if(right > left) {
//                        line.add(right);
//                    } else {
//                        line.remove(len-1);
//                        for(int j = right; j <= left; j++) {
//                            line.add(j);
//                        }
//                    }
//                }
                p = root;
            }
        }
        return line;
    }
}
