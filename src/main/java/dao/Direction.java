package dao;

import java.io.Serializable;

public class Direction implements Serializable {
    int dataId;
    double dx;
    double dy;
    double dz;
    public Direction(){}
    public Direction(int data_id, double dx, double dy, double dz) {
        this.dataId = data_id;
        this.dx = dx;
        this.dy = dy;
        this.dz = dz;
    }

    public double[] getDirection() {
        double[] d = {dx, dy, dz};
        return d;
    }

    public int getDataId() {
        return dataId;
    }

    public double getDx() {
        return dx;
    }

    public double getDy() {
        return dy;
    }

    public double getDz() {
        return dz;
    }

    public void setDataId(int dataId) {
        this.dataId = dataId;
    }

    public void setDx(double dx) {
        this.dx = dx;
    }

    public void setDy(double dy) {
        this.dy = dy;
    }

    public void setDz(double dz) {
        this.dz = dz;
    }

    @Override
    public String toString() {
        return "Direction{" +
                "dataId=" + dataId +
                ", dx=" + dx +
                ", dy=" + dy +
                ", dz=" + dz +
                '}';
    }
}
