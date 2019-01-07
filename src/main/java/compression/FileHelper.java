package compression;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileHelper {
    public static int readInt32(FileInputStream file) {
        int a, b, c, d;
        try {
            a = file.read();
            b = file.read();
            c = file.read();
            d = file.read();
            return (a << 24) | (b << 16) | (c << 8) | d;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static short readInt16(FileInputStream file) {
        int c, d;
        try {
            c = file.read();
            d = file.read();
            return (short)((c << 8) | d);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static byte readInt8(FileInputStream file) {
        int d;
        try {
            d = file.read();
            return (byte)d;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void writeInt16(FileOutputStream file, short v) {
        try {
            file.write((v >>> 8) & 0xff);
            file.write(v & 0xff);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

