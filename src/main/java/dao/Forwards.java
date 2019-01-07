package dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Forwards {
    public static List<Vector3> forwards = new ArrayList<Vector3>(Arrays.asList(
            new Vector3(1, 0, 0),
            new Vector3(1, 1, 0).normalize(),
            new Vector3(1, 0, -1).normalize(),
            new Vector3(1, 1, 0).normalize().add(new Vector3(0, 0, -1)).normalize(),
            new Vector3(0, 0, -1),
            new Vector3(0, 1, -1).normalize(),
            new Vector3(-1, 0, -1).normalize(),
            new Vector3(-1, 1, 0).normalize().add(new Vector3(0, 0, -1)).normalize(),
            new Vector3(-1, 0, 0),
            new Vector3(-1, 1, 0).normalize(),
            new Vector3(-1, 0, 1).normalize(),
            new Vector3(-1, 1, 0).normalize().add(new Vector3(0, 0, 1)).normalize(),
            new Vector3(0, 0, 1),
            new Vector3(0, 1, 1).normalize(),
            new Vector3(1, 0, 1).normalize(),
            new Vector3(1, 1, 0).normalize().add(new Vector3(0, 0, 1)).normalize(),
            new Vector3(1, -1, 0).normalize(),
            new Vector3(1, -1, 0).normalize().add(new Vector3(0, 0, -1)).normalize(),
            new Vector3(0, -1, -1).normalize(),
            new Vector3(-1, -1, 0).normalize().add(new Vector3(0, 0, -1)).normalize(),
            new Vector3(-1, -1, 0).normalize(),
            new Vector3(-1, -1, 0).normalize().add(new Vector3(0, 0, 1)).normalize(),
            new Vector3(0, -1, 1).normalize(),
            new Vector3(1, -1, 0).normalize().add(new Vector3(0, 0, 1)).normalize()
    ));
}
