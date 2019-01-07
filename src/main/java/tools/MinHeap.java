package tools;

import dao.SimilarityTuple;

public class MinHeap {
    public SimilarityTuple[] arr;
    public int count;
    public int k;

    public MinHeap(int k) {
        arr = new SimilarityTuple[k];
        k = k;
        count = 0;
    }

    public void add(SimilarityTuple similarityTuple) {
        this.arr[count] = similarityTuple;
        count += 1;
    }
    public int getCount() {
        return this.count;
    }

    public void buildHeap() {
        int len = arr.length;
        for(int i = len/2-1; i >= 0; i--) {
            adjustHeap(i);
        }
    }
    public void adjustHeap(int i) {
        int len = arr.length;
        SimilarityTuple tmp = arr[i];
        int j = 2*i + 1;
        while(j < len) {
            if(j+1 < len && arr[j+1].similarityP < arr[j].similarityP) {
                j += 1;
            }
            if(tmp.similarityP <= arr[j].similarityP) {
                break;
            }
            arr[i] = arr[j];
            i = j;
            j = 2*i + 1;
        }
        arr[i] = tmp;
    }


    public SimilarityTuple[] getArr() {
        return arr;
    }

    public void setArr(SimilarityTuple[] arr) {
        this.arr = arr;
    }

}
