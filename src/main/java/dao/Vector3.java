package dao;

public class Vector3 {
    public double x;
    public double y;
    public double z;
    public Vector3(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public double[] getArray() {
        double[] d = {x, y, z};
        return d;
    }

    public Vector3 normalize() {
        double len = Math.sqrt(x*x + y*y + z*z);
        return new Vector3(x/len, y/len, z/len);
    }

    public Vector3 add(Vector3 vec) {
        return new Vector3(this.x+vec.x, this.y+vec.y, this.z+vec.z);
    }
}

