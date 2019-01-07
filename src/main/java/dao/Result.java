package dao;

import java.util.List;

public class Result {
    private int dataId;
    private List<Integer> visibleObj;
    public Result(int dataId, List<Integer> visibleObj) {
        this.dataId = dataId;
        this.visibleObj = visibleObj;
    }

    public int getDataId() {
        return dataId;
    }

    public List<Integer> getVisibleObj() {
        return visibleObj;
    }

    public void setDataId(int dataId) {
        this.dataId = dataId;
    }

    public void setVisibleObj(List<Integer> visibleObj) {
        this.visibleObj = visibleObj;
    }

    @Override
    public String toString() {
        return "Result{" +
                "dataId=" + dataId +
                ", visibleObj=" + visibleObj +
                '}';
    }
}
