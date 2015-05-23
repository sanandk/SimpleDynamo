package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.provider.CalendarContract;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import static edu.buffalo.cse.cse486586.simpledynamo.Ring.handleNetworkRequests;
import static edu.buffalo.cse.cse486586.simpledynamo.Ring.who_is_he;

/**
 * Created by anand on 3/30/15.
 */
public class NetworkTasks {

    static public String MYPORT=null;
    public final static String  queryresponse="queryresponse ",blind_insert="blindinsert ", get_all_keys_at="getallkeysat ", at_bundle="atbundle",
            bundleforwardreq="bundleforward ",forwardreq="forward ",get_all_keys="getallkeys ",allkeys="allkeys ",forwardquery="forwardq ",forwarddelete="forwarddelete ";
    static ServerSocket SERVER_SOCKET=null;
    static String REMOTE_PORT[] = null;
    public static String oREMOTE_PORT[] = null;
    static ArrayList<String> dead;
    static Map<String, Integer> nodes, ringData;
    static HashMap<String, String> msgData;

    public static Timer timer;
    public static void initNetworkValues() throws NoSuchAlgorithmException {

        REMOTE_PORT=new String[5];
        REMOTE_PORT[0] = "11124"; //5562 - avd4
        REMOTE_PORT[1] = "11112"; //5556 - avd0
        REMOTE_PORT[2]=  "11108"; //5554 - avd3
        REMOTE_PORT[3] = "11116"; //5558 - avd1
        REMOTE_PORT[4] = "11120"; //5560 - avd2
        oREMOTE_PORT=REMOTE_PORT.clone();
        msgData=new HashMap<String, String>();
        nodes = Collections.synchronizedMap(new TreeMap<String, Integer>());
        ringData = Collections.synchronizedMap(new TreeMap<String, Integer>());
        dead=new ArrayList<String>();

        for(int i=0;i<5;i++) {
            nodes.put(genHash(Integer.parseInt(REMOTE_PORT[i])/2+""), i);
            ringData.put(genHash(Integer.parseInt(REMOTE_PORT[i])/2+""), i);
        }
        try {
            SERVER_SOCKET = new ServerSocket(10000);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    public static class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort = msgs[1];
                Log.d("msgs2", msgs.length+":"+msgs[0]+msgs[1]);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                socket.setSoTimeout(500);
                String msgToSend = msgs[0];
                String [] spl = msgToSend.split(" ");

                msgToSend = spl[0] + " " + MYPORT + " " + spl[1];

                Log.d("network", msgToSend);

                OutputStream outputStream = socket.getOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(outputStream);
                oos.writeObject(msgToSend);
                if(!(msgs.length>2 && msgs[2].equals("dont")) && remotePort!=MYPORT) {
                    Log.e("ACK","GOING TO WAIT FOR ACK FROM "+remotePort);
                    ACKWaitTask(socket, remotePort, msgs[0]);
                }

            } catch (UnknownHostException e) {
                Log.e("TAG", "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch(SocketTimeoutException e){
                Log.e("TAG", "ClientTask socket Timeout IOException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("TAG", "ClientTask socket IOException");
                e.printStackTrace();
            } catch (Exception e){
                Log.e("TAG", "ClientTask socket Exception");
                e.printStackTrace();
            }

            return null;
        }
    }

    /***
     * SkippingClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    public static class SkippingClientTask extends AsyncTask<String, Void, Void> {
        // I wont wait for ACK!
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort = msgs[1];
                Log.d("msgs2", msgs.length+":"+msgs[0]+msgs[1]);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                socket.setSoTimeout(500);
                String msgToSend = msgs[0];
                String [] spl = msgToSend.split(" ");

                msgToSend = spl[0] + " " + MYPORT + " " + spl[1];

                Log.d("network", msgToSend);

                OutputStream outputStream = socket.getOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(outputStream);
                oos.writeObject(msgToSend);

            } catch (UnknownHostException e) {
                Log.e("TAG", "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch(SocketTimeoutException e){
                Log.e("TAG", "ClientTask socket Timeout IOException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("TAG", "ClientTask socket IOException");
                e.printStackTrace();
            } catch (Exception e){
                Log.e("TAG", "ClientTask socket Exception");
                e.printStackTrace();
            }

            return null;
        }
    }

    static void liveCheck(String rp) throws Exception {
            Log.e("DEATH", "RECOVERING " + rp);
            dead.clear();
            REMOTE_PORT=oREMOTE_PORT.clone();
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, bundleforwardreq + MYPORT + "|" + SimpleDynamoProvider.getAllKeys(true), rp);
    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    public static class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        SimpleDynamoProvider provider;
        Context context;
        ServerTask(Context ctx, SimpleDynamoProvider sp){
            context=ctx;
            provider=sp;
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             * Accept connection from client.
             * As long as data is received, send it to onProgressUpdate
             */
            ServerSocket serverSocket = sockets[0];
            String text;
            try {


                while(true)
                {
                    Socket clientSocket=serverSocket.accept();

                    ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());

                    text = (String)ois.readObject();

                    try {
                        ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                        oos.writeObject("ACK REPLY BY ME");
                        oos.flush();
                        oos.close();

                        clientSocket.close();
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }

                    if(text!=null) {
                            String [] spl = text.split(" ");
                            String rp=spl[1];
                            text=spl[0]+" "+spl[2];
                            Log.e("TAG", "msg from"+ rp + "=" + text);
                            if(text.contains("IAMREBORN")){
                                liveCheck(rp);
                            }
                            else
                            {
                                publishProgress(text.trim());
                            }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            catch (Exception e){

            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

            String strReceived = strings[0].trim();

            try {
                handleNetworkRequests(provider, context, strReceived);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return;
        }
    }
    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static class ACKTask extends AsyncTask<Object, Void, Void> {
        String remotePort;
        @Override
        protected Void doInBackground(Object... objs) {
            try {
                Socket clientSocket=(Socket)objs[0];
                ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                oos.writeObject("ACK REPLY BY ME");
                oos.flush();
                oos.close();

                clientSocket.close();
            }
            catch (Exception e){
                Log.e("liveCheck","Something else in CheckTask-"+remotePort);
                e.printStackTrace();
            }

            return null;
        }
    }
    public static void ACKWaitTask(Object... msgs) {
        String remotePort = null, resend=null, newPort=null;
        try {
            Socket socket=(Socket)msgs[0];
            remotePort=(String)msgs[1];
            resend=(String)msgs[2];
            InputStream stream = socket.getInputStream();
            ObjectInputStream ois = new ObjectInputStream(stream);
            String reply = (String)ois.readObject();
            Log.d("ACKREPLY", reply + "");
            ois.close();
            socket.close();
        }
        catch(SocketTimeoutException e){
            Log.e("ZOMBIE", "NO ACTION TAKEN! "+remotePort);
        }
        catch (Exception e) {
            int ind=-1;
            if(!dead.contains(remotePort)) {
                Log.e("DEATH", "DEAD :( -" + remotePort);
                dead.add(remotePort);
                e.printStackTrace();
            }
                for (int i = 0; i < 5; i++) {
                    if (REMOTE_PORT[i].equals(remotePort)) {
                        REMOTE_PORT[i] = oREMOTE_PORT[(i + 1) % 5];
                        newPort = oREMOTE_PORT[(i + 1) % 5];
                        ind = (i + 1) % 5;
                        break;
                    }
                }

                Log.e("RESENT", resend + ":" + newPort + "," + oREMOTE_PORT[ind]);
                new SkippingClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, resend, oREMOTE_PORT[ind], "dont");
        }
    }


}
