package tools;

public class Configuration {
    private String ip;
    private int readPort;
    private int writePort;
    private String[] trainDataPath;
    private String[] testDataPath;
    private String[] trainTargetPath;
    private String[] testTargetPath;
    private int knnPositionk;
    private int knnDirectionk;
    private int CFHowMany;
    private int Reck;
    private int RecHowMany;
    private String KnnWriteToFile;

    private static Configuration instance = null;

    private Configuration() {

    }

    public static Configuration getInstance() {
        if(instance == null) {
            instance = new Configuration();
        } else {
            return instance;
        }
        return instance;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getReadPort() {
        return readPort;
    }

    public void setReadPort(int readPort) {
        this.readPort = readPort;
    }

    public int getWritePort() {
        return writePort;
    }

    public void setWritePort(int writePort) {
        this.writePort = writePort;
    }

    public String[] getTrainDataPath() {
        return trainDataPath;
    }

    public void setTrainDataPath(String[] trainDataPath) {
        this.trainDataPath = trainDataPath;
    }

    public String[] getTrainTargetPath() {
        return trainTargetPath;
    }

    public void setTrainTargetPath(String[] trainTargetPath) {
        this.trainTargetPath = trainTargetPath;
    }

    public String[] getTestDataPath() {
        return testDataPath;
    }

    public void setTestDataPath(String[] testDataPath) {
        this.testDataPath = testDataPath;
    }

    public String[] getTestTargetPath() {
        return testTargetPath;
    }

    public void setTestTargetPath(String[] testTargetPath) {
        this.testTargetPath = testTargetPath;
    }

    public int getKnnPositionk() {
        return knnPositionk;
    }

    public void setKnnPositionk(int knnPositionk) {
        this.knnPositionk = knnPositionk;
    }

    public int getKnnDirectionk() {
        return knnDirectionk;
    }

    public void setKnnDirectionk(int knnDirectionk) {
        this.knnDirectionk = knnDirectionk;
    }

    public int getCFHowMany() {
        return CFHowMany;
    }

    public void setCFHowMany(int CFHowMany) {
        this.CFHowMany = CFHowMany;
    }

    public int getReck() {
        return Reck;
    }

    public void setReck(int reck) {
        Reck = reck;
    }

    public int getRecHowMany() {
        return RecHowMany;
    }

    public void setRecHowMany(int recHowMany) {
        RecHowMany = recHowMany;
    }

    public String getKnnWriteToFile() {
        return KnnWriteToFile;
    }

    public void setKnnWriteToFile(String knnWriteToFile) {
        KnnWriteToFile = knnWriteToFile;
    }
}
