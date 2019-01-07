package dao;

import java.io.Serializable;

public class Position implements Serializable {
    int dataId;
    double px;
    double py;
    double pz;
    public Position(){}
    public Position(int data_id, double px, double py, double pz) {
        this.dataId = data_id;
        this.px = px;
        this.py = py;
        this.pz = pz;
    }
    public Position(double px, double py, double pz) {
        this.px = px;
        this.py = py;
        this.pz = pz;
    }

    public double[] getPosition() {
        double[] p = {px, py, pz};
        return p;
    }

    public int getDataId() {
        return dataId;
    }

    public double getPx() {
        return px;
    }

    public double getPy() {
        return py;
    }

    public double getPz() {
        return pz;
    }

    public void setDataId(int dataId) {
        this.dataId = dataId;
    }

    public void setPx(double px) {
        this.px = px;
    }

    public void setPy(double py) {
        this.py = py;
    }

    public void setPz(double pz) {
        this.pz = pz;
    }

    @Override
    public String toString() {
        return "Position{" +
                "dataId=" + dataId +
                ", px=" + px +
                ", py=" + py +
                ", pz=" + pz +
                '}';
    }
}


