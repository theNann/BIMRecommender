package dao;

import scala.Tuple2;
import scala.Tuple3;
import tools.Tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SceneInfo {
    public static double positionXmin = -40;
    public static double positionXmax = 160;
    public static double positionYmin = 0;
    public static double positionYmax = 30;
    public static double positionZmin = -70;
    public static double positionZmax = 130;

    public static double gridLength = 10;
    public static int forwardsNum = 60;
    public static Vector3[] forwards = {
            new Vector3(0, 0, 1),
            new Vector3(0.5, 0, 0.866025),
            new Vector3(0.866025, 0, 0.5),
            new Vector3(1, 0, 0),
            new Vector3(0.866025, 0, -0.5),
            new Vector3(0.5, 0, -0.866025),
            new Vector3(0, 0, -1),
            new Vector3(-0.5, 0, -0.866025),
            new Vector3(-0.866025, 0, -0.5),
            new Vector3(-1, 0, 0),
            new Vector3(-0.866026, 0, 0.5),
            new Vector3(-0.5, 0, 0.866025),
            new Vector3(0, -0.5, 0.866025),
            new Vector3(0.433013, -0.5, 0.75),
            new Vector3(0.75, -0.5, 0.433013),
            new Vector3(0.866025, -0.5, 0),
            new Vector3(0.75, -0.5, -0.433013),
            new Vector3(0.433013, -0.5, -0.75),
            new Vector3(0, -0.5, -0.866025),
            new Vector3(-0.433013, -0.5, -0.75),
            new Vector3(-0.75, -0.5, -0.433013),
            new Vector3(-0.866025, -0.5, 0),
            new Vector3(-0.75, -0.5, 0.433013),
            new Vector3(-0.433013, -0.5, 0.75),
            new Vector3(0, -0.866025, 0.5),
            new Vector3(0.25, -0.866026, 0.433013),
            new Vector3(0.433013, -0.866025, 0.25),
            new Vector3(0.5, -0.866025, 0),
            new Vector3(0.433013, -0.866025, -0.25),
            new Vector3(0.25, -0.866025, -0.433013),
            new Vector3(0, -0.866025, -0.5),
            new Vector3(-0.25, -0.866025, -0.433013),
            new Vector3(-0.433013, -0.866025, -0.25),
            new Vector3(-0.5, -0.866025, 0),
            new Vector3(-0.433013, -0.866025, 0.25),
            new Vector3(-0.25, -0.866025, 0.433013),
            new Vector3(0, 0.5, -0.866025),
            new Vector3(-0.433013, 0.5, -0.75),
            new Vector3(-0.75, 0.5, -0.433013),
            new Vector3(-0.866025, 0.5, 0),
            new Vector3(-0.75, 0.5, 0.433013),
            new Vector3(-0.433013, 0.5, 0.75),
            new Vector3(0, 0.5, 0.866025),
            new Vector3(0.433013, 0.5, 0.75),
            new Vector3(0.75, 0.5, 0.433013),
            new Vector3(0.866025, 0.5, 0),
            new Vector3(0.75, 0.5, -0.433013),
            new Vector3(0.433013, 0.5, -0.75),
            new Vector3(0, 0.866025, -0.5),
            new Vector3(-0.25, 0.866025, -0.433013),
            new Vector3(-0.433013, 0.866026, -0.25),
            new Vector3(-0.5, 0.866025, 0),
            new Vector3(-0.433013, 0.866026, 0.25),
            new Vector3(-0.25, 0.866026, 0.433013),
            new Vector3(0, 0.866026, 0.5),
            new Vector3(0.25, 0.866026, 0.433013),
            new Vector3(0.433013, 0.866025, 0.25),
            new Vector3(0.5, 0.866025, 0),
            new Vector3(0.433013, 0.866025, -0.25),
            new Vector3(0.25, 0.866025, -0.433013)
    };
    public static int xGridNumber = (int) Math.ceil((positionXmax - positionXmin) / gridLength);
    public static int yGridNumber = (int) Math.ceil((positionYmax - positionYmin) / gridLength);
    public static int zGridNumber = (int) Math.ceil((positionZmax - positionZmin) / gridLength);
    public static int Clamp(int v, int min, int max) {
        return Math.min(max, Math.max(v, min));
    }
    public static int ToGridX(double x) {
        return Clamp((int)Math.floor((x + 40) / gridLength), 0, xGridNumber - 1);
    }
    public static int ToGridY(double y) {
        return Clamp((int)Math.floor(y / gridLength), 0, yGridNumber - 1);
    }
    public static int ToGridZ(double z) {
        return Clamp((int)Math.floor((z + 70) / gridLength), 0, zGridNumber - 1);
    }

    public static int getTrainSize() {
        return xGridNumber*yGridNumber*zGridNumber*forwardsNum;
    }

    public static int getDataId(int gridX, int gridY, int gridZ, int forwardsIdx) {
        int gridIdx = gridX * yGridNumber * zGridNumber + gridY * zGridNumber + gridZ;
        return gridIdx * forwardsNum + forwardsIdx;
    }

    public static List<Integer> nearestForwards(Direction direction, int k) {
        List<Integer> res = new ArrayList<Integer>();
        List<Tuple2<Integer, Double>> sim = new ArrayList<Tuple2<Integer, Double>>();
        for(int i = 0; i < forwards.length; i++) {
            Vector3 direction1 = forwards[i];
            sim.add(new Tuple2<Integer, Double>(i, Tools.vectorSimlarity(direction.getDirection(), direction1.getArray())));
        }
        Collections.sort(sim, new Comparator<Tuple2<Integer, Double>>() {
            public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
                if(o2._2 > o1._2) {
                    return 1;
                } else if(o2._2.equals(o1._2)){
                    return 0;
                } else {
                    return -1;
                }
            }
        });
        for(int i = 0; i < k; i++) {
            res.add(sim.get(i)._1());
        }
        return res;
    }

    public static List<Integer> nearestNeighbors1(int gridX, int gridY, int gridZ, Position position, Direction direction, int k) {
        List<Integer> dataIds = new ArrayList<Integer>();
        List<Integer> nearestForwardsList = nearestForwards(direction, k);
        for(int i = 0; i < nearestForwardsList.size(); i++) {
            int forwardsIdx = nearestForwardsList.get(i);
            int dataId = getDataId(gridX, gridY, gridZ, forwardsIdx);
            dataIds.add(dataId);
        }
        return dataIds;
    }

    public static List<GridData> nearestNeighbors7(int gridX, int gridY, int gridZ, GridData[][][] trainData) {
        List<GridData> gridDataList = new ArrayList<GridData>();
        gridDataList.add(trainData[gridX][gridY][gridZ]);
        if(gridY+1 < SceneInfo.yGridNumber) {
            gridDataList.add(trainData[gridX][gridY+1][gridZ]);
        }
        if(gridY-1 >= 0) {
            gridDataList.add(trainData[gridX][gridY-1][gridZ]);
        }
        if(gridX+1 < SceneInfo.xGridNumber) {
            gridDataList.add(trainData[gridX+1][gridY][gridZ]);
        }
        if(gridX-1 >= 0) {
            gridDataList.add(trainData[gridX-1][gridY][gridZ]);
        }
        if(gridZ+1 < SceneInfo.zGridNumber) {
            gridDataList.add(trainData[gridX][gridY][gridZ+1]);
        }
        if(gridZ-1 >= 0) {
            gridDataList.add(trainData[gridX][gridY][gridZ-1]);
        }
        return gridDataList;
    }

    public static List<Integer> nearestNeighbors2(int gridX, int gridY, int gridZ, Position position, Direction direction, int k) {
        List<Integer> dataIds  = new ArrayList<Integer>();
        List<Tuple3<Integer, Integer, Integer>> nearestGrids = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        List<Integer> nearestForwards = nearestForwards(direction, k);
        nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX, gridY, gridZ));
        Position gridPosition = new Position(positionXmin+gridX*gridLength+gridLength/2,
                positionYmin+gridY*gridLength+gridLength/2,
                positionZmin+gridZ*gridLength+gridLength/2);
        double xx = position.getPx() - gridPosition.getPx();
        double yy = position.getPy() - gridPosition.getPy();
        double zz = position.getPz() - gridPosition.getPz();
        //xyz分别取一个, 共3个
//        if(xx > 0 && gridX+1 < xGridNumber) {
//            list.add(trainData[gridX+1][gridY][gridZ]);
//        } else if(xx < 0 && gridX-1 >= 0) {
//            list.add(trainData[gridX-1][gridY][gridZ]);
//        }
//        if(yy > 0 && gridY+1 < yGridNumber) {
//            list.add(trainData[gridX][gridY+1][gridZ]);
//        } else if(yy < 0 && gridY-1 >= 0) {
//            list.add(trainData[gridX][gridY-1][gridZ]);
//        }
//        if(zz > 0 && gridZ+1 < zGridNumber) {
//            list.add(trainData[gridX][gridY][gridZ+1]);
//        } else if(zz < 0 && gridZ-1 >= 0) {
//            list.add(trainData[gridX][gridY][gridZ-1]);
//        }

        //xyz中取一个，共1个
        if(Math.abs(xx) >= Math.abs(yy) && Math.abs(xx) >= Math.abs(zz)) {
            if(xx > 0 && gridX+1 < xGridNumber) {
                nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX+1, gridY, gridZ));
            } else if(xx < 0 && gridX-1 >= 0) {
                nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX-1, gridY, gridZ));

            }
        } else if(Math.abs(yy) >= Math.abs(xx) && Math.abs(yy) >= Math.abs(xx)) {
            if(yy > 0 && gridY+1 < yGridNumber) {
                nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX, gridY+1, gridZ));
            } else if(yy < 0 && gridY-1 >= 0){
                nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX, gridY-1, gridZ));
            }
        } else if(Math.abs(zz) >= Math.abs(xx) && Math.abs(zz) >= Math.abs(yy)) {
            if(zz > 0 && gridZ+1 < yGridNumber) {
                nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX, gridY, gridZ+1));
            } else if(zz < 0 && gridZ-1 >= 0){
                nearestGrids.add(new Tuple3<Integer, Integer, Integer>(gridX, gridY, gridZ-1));
            }
        }
        for(int i = 0; i < nearestGrids.size(); i++) {
            Tuple3<Integer, Integer, Integer> tuple = nearestGrids.get(i);
            for(int j = 0; j < nearestForwards.size(); j++) {
                dataIds.add(getDataId(tuple._1(), tuple._2(), tuple._3(), nearestForwards.get(j)));
            }
        }
        return dataIds;
    }

    public static List<GridData> nearestNeighbors2ByPos(int gridX, int gridY, int gridZ, Position position, GridData[][][] trainData) {
        List<GridData> list = new ArrayList<GridData>();
        List<Tuple2<Integer, Double>> sim = new ArrayList<Tuple2<Integer, Double>>();
        List<GridData> neighbors7 = nearestNeighbors7(gridX, gridY, gridZ, trainData);
        for(int i = 0; i < neighbors7.size(); i++) {
            Position gridPosition = neighbors7.get(i).primitives.get(0).getPosition();
            double sim_i = Tools.euclideanDistanceSim(position.getPosition(), gridPosition.getPosition());
            sim.add(new Tuple2<Integer, Double>(i, sim_i));
        }
        Collections.sort(sim, new Comparator<Tuple2<Integer, Double>>() {
            public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
                if(o2._2 > o1._2) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
        list.add(neighbors7.get(sim.get(0)._1));
        list.add(neighbors7.get(sim.get(1)._1));
        return list;
    }

//    public static List<Integer> getTrainIdFromNeighborGrid(List<GridData> neighbors, Direction direction, int k) {
//        List<Integer> trainDataId = new ArrayList<Integer>();
//        for(int i = 0; i < neighbors.size(); i++) {
//            List<PrimitiveData> primitiveData = neighbors.get(i).primitives;
//            List<Tuple2<Integer, Double>> sim = new ArrayList<Tuple2<Integer, Double>>();
//            for (int j = 0; j < primitiveData.size(); j++) {
//                Direction direction1 = primitiveData.get(j).getDirection();
//                sim.add(new Tuple2<Integer, Double>(j, Tools.vectorSimlarity(direction.getDirection(), direction1.getDirection())));
//            }
//            Collections.sort(sim, new Comparator<Tuple2<Integer, Double>>() {
//                public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
//                    if(o2._2 > o1._2) {
//                        return 1;
//                    } else if(o2._2.equals(o1._2)){
//                        return 0;
//                    } else {
//                        return -1;
//                    }
//                }
//            });
////            System.out.println("primitiveDataSize : " + primitiveData.size());
////            System.out.println("simSize : " + sim.size());
//            for(int j = 0; j < k; j++) {
////                System.out.println("get: " + primitiveData.get(j));
//                trainDataId.add(primitiveData.get(sim.get(j)._1).dataId);
//            }
//        }
//        return trainDataId;
//    }

}
