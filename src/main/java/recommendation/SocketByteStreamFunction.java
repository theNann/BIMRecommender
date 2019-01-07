package recommendation;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketByteStreamFunction implements SourceFunction<byte[]> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SocketByteStreamFunction.class);
    private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;
    private static final int CONNECTION_TIMEOUT_TIME = 0;
    private final String hostname;
    private final int port;
    private transient Socket currentSocket;
    private volatile boolean isRunning;
    private final long maxNumRetries;
    private final long delayBetweenRetries;
    private int byteNum;
    private byte[] buffer;
    private transient InputStream input;
    public SocketByteStreamFunction(String hostname, int port, int byteNum, long maxNumRetries) {
        this.hostname = hostname;
        this.port = port;
        this.byteNum = byteNum;
        this.isRunning = true;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = 500L;
        this.buffer = new byte[byteNum];
    }

//    public void run(SourceContext<byte[]> ctx) throws Exception {
//        if (this.currentSocket == null) {
//            this.currentSocket = new Socket();
//            System.out.println("new Socket!!!!!!!!!!!");
//            try {
//                this.currentSocket.connect(new InetSocketAddress(hostname, port), 0);
//                input = this.currentSocket.getInputStream();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
////        System.out.println("function socket : " + this.currentSocket);//null?
//        while(this.isRunning) {
//            int bytesRead = 0;
//            int len = 0;
//            while(this.isRunning && (len = input.read(buffer, bytesRead, byteNum - bytesRead)) != -1) {
//                bytesRead += len;
//            }
//            ctx.collect(buffer);
//        }
//    }

    public void run(SourceContext<byte[]> ctx) throws Exception {
        long attempt = 0L;
//        System.out.println("function socket : " + this.currentSocket);//null?
        while(this.isRunning) {
            Socket socket = new Socket();
            // System.out.println("new Socket!!!!!");
            Throwable var6 = null;
            try {
                this.currentSocket = socket;
                // LOG.info("Connecting to server socket " + hostname + ':' + port);
                socket.connect(new InetSocketAddress(hostname, port), 0);
//                System.out.println("Connect!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                InputStream input = socket.getInputStream();
                byte[] in = new byte[byteNum];//因为每个数据是68byte,8160/68=120,也就是每次读120个数据
                int bytesRead;
                while(this.isRunning && (bytesRead = input.read(in, 0, byteNum)) != -1) {
                    while(bytesRead < byteNum) {
                        bytesRead += input.read(in, bytesRead, byteNum-bytesRead);
                    }
                    ctx.collect(in);
                }
            } catch (Throwable var19) {
                var6 = var19;
            } finally {
                if(socket != null) {
                    socket.close();
                }
            }
//            if(this.isRunning) {
//                ++attempt;
//                if(this.maxNumRetries != -1L && attempt >= this.maxNumRetries) {
//                    break;
//                }
//
//                LOG.warn("Lost connection to server socket. Retrying in " + this.delayBetweenRetries + " msecs...");
//                Thread.sleep(this.delayBetweenRetries);
//            }
        }
    }

    public void cancel() {
        this.isRunning = false;
        Socket theSocket = this.currentSocket;
        if(theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }

    }
}
