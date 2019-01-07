package dao;

public class SimilarityTuple {
    public int dataId;
    public double similarityP;
    public double similarityD;
    public SimilarityTuple(int dataId, double similarityP) {
        this.dataId = dataId;
        this.similarityP = similarityP;
    }
    public SimilarityTuple(int dataId, double similarityP, double similarityD) {
        this.dataId = dataId;
        this.similarityP = similarityP;
        this.similarityD = similarityD;
    }

}

