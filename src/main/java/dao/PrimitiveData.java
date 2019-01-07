package dao;

import org.ujmp.core.DenseMatrix;
import org.ujmp.core.Matrix;

import java.io.Serializable;

public class PrimitiveData implements Serializable {
    public double px;
    public double py;
    public double pz;
    public double dx;
    public double dy;
    public double dz;
    public int dataId;

    public PrimitiveData() {
    }

    public PrimitiveData(int dataId, double px, double py, double pz, double dx, double dy, double dz) {
        this.dataId = dataId;
        this.px = px;
        this.py = py;
        this.pz = pz;
        this.dx = dx;
        this.dy = dy;
        this.dz = dz;
    }

    public int getDataId() {
        return this.dataId;
    }

    public Position getPosition() {
        return new Position(dataId, px, py, pz);
    }

    public Direction getDirection() {
        return new Direction(dataId, dx, dy, dz);
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

    public void setPx(double px) {
        this.px = px;
    }

    public void setPy(double py) {
        this.py = py;
    }

    public void setPz(double pz) {
        this.pz = pz;
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

    public static PrimitiveData primitiveDataFromBytes(byte[] bytes) {
        int idx = 0;
        double[] data = new double[18];
        int i = 0;
        while (i < bytes.length) {
            int num = ((bytes[i + 3] & 0xff) << 24) | ((bytes[i + 2] & 0xff) << 16) | ((bytes[i + 1] & 0xff) << 8) | (bytes[i] & 0xff);
            double num1 = num * 1.0 / 1000000;
            i += 4;
            data[idx++] = num1;
//          System.out.println("num1 : " + num1);
        }

        Matrix viewMatrix = DenseMatrix.Factory.zeros(4, 4);
        idx = 2;
        for (i = 0; i < viewMatrix.getRowCount(); i++) {
            for (int j = 0; j < viewMatrix.getColumnCount(); j++) {
                viewMatrix.setAsDouble(data[idx++], i, j);
            }
        }
        Matrix bMatrix = DenseMatrix.Factory.zeros(4, 4);
        idx = 2;
        for (i = 0; i < bMatrix.getRowCount(); i++) {
            for (int j = 0; j < bMatrix.getColumnCount(); j++) {
                bMatrix.setAsDouble(data[idx++], i, j);
            }
        }
        for (i = 0; i < 3; i++) {
            bMatrix.setAsDouble(0, 3, i);
        }
        Matrix tMatrix = viewMatrix.mtimes(bMatrix.inv());
        PrimitiveData primitiveData = new PrimitiveData((int) data[0],
                -tMatrix.getAsDouble(3, 0), -tMatrix.getAsDouble(3, 1), -tMatrix.getAsDouble(3, 2),
                viewMatrix.getAsDouble(0, 2), viewMatrix.getAsDouble(1, 2), viewMatrix.getAsDouble(2, 2));
        return primitiveData;
    }

    @Override
    public String toString() {
        return "PrimitiveData{" +
                "dataId=" + dataId +
                ", px=" + px +
                ", py=" + py +
                ", pz=" + pz +
                ", dx=" + dx +
                ", dy=" + dy +
                ", dz=" + dz +
                '}';
    }
}
