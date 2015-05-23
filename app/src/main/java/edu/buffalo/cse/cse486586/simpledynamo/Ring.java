package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.buffalo.cse.cse486586.simpledynamo.NetworkTasks.*;
import static edu.buffalo.cse.cse486586.simpledynamo.NetworkTasks.REMOTE_PORT;
import static edu.buffalo.cse.cse486586.simpledynamo.NetworkTasks.genHash;
import static edu.buffalo.cse.cse486586.simpledynamo.NetworkTasks.initNetworkValues;
import static edu.buffalo.cse.cse486586.simpledynamo.Ring.whoami.OWNER;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.at_query_status1;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.at_query_status2;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.insertIntoStorage;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.pref;

/**
 * Created by anand on 3/30/15.
 */
public class Ring {

    static MatrixCursor DHTcursor=null;
    static Map<String, String>[] DHT;
    static String answer=null;
    public static enum whoami{OWNER, COORDINATOR, REPLICA1,REPLICA2, USELESS};
    static whoami who_am_I(String keystr) throws NoSuchAlgorithmException {

        int owner=who_is_owner_ind(keystr);
        int coord=who_is_coord_ind(keystr);
        int me=-1,me2=-1;

        for(int i=0;i<5;i++)
        {
            if(REMOTE_PORT[i].equals(MYPORT))
            {
                if(me==-1)
                    me=i;
                else if(me2==-1) {
                    me2=i;
                    break;
                }
            }
        }
        if(coord==me || coord==me2)
            return whoami.COORDINATOR;
        if((coord+1)%5==me || (coord+1)%5==me2)
            return whoami.REPLICA1;
        if((coord+2)%5==me || (coord+2)%5==me2)
            return whoami.REPLICA2;
        if(owner==me || owner==me2)
            return OWNER;

        return whoami.USELESS;
    }
    public static whoami who_is_he(String rp, String keystr) throws NoSuchAlgorithmException {

        int owner=who_is_owner_ind(keystr);

        int me=-1;

        for(int i=0;i<5;i++)
        {
            if(oREMOTE_PORT[i].equals(rp))
            {
                me=i;
                break;
            }
        }
        if(owner==me)
            return OWNER;
        if(((owner+1)%5)==me)
            return whoami.COORDINATOR;
        if(((owner+2)%5)==me)
            return whoami.REPLICA1;
        if(((owner+3)%5)==me)
            return whoami.REPLICA2;

        return whoami.USELESS;
    }
    static int who_is_coord_ind(String key) throws NoSuchAlgorithmException {
        return (who_is_owner_ind(key)+1)%5;
    }
    static int who_is_owner_ind(String keystr) throws NoSuchAlgorithmException {

        int owner=-1,last=-1;
        String key=genHash(keystr);
        synchronized (ringData)
        {
            if(!ringData.containsKey(key))
                ringData.put(key,-1);
            Iterator it = ringData.keySet().iterator();
            while(it.hasNext()) {
                String keyd = (String) it.next();
                if (ringData.get(keyd) != -1)
                    last = ringData.get(keyd);
                else if (keyd.equals(key)) {
                    owner = last;
                }
            }
        }

        if(owner==-1)
            owner=last;

        return owner;
    }


    static int getIndex(String port)
    {
        for(int i=0;i<5;i++)
            if(oREMOTE_PORT[i].equals(port))
                return i;
        return -1;
    }

    static String getmyPort(Context context){
        if(MYPORT==null) {
            TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
            MYPORT = Integer.parseInt(tel.getLine1Number().substring((tel.getLine1Number().length() - 4)))* 2+"";
        }
        return MYPORT;
    }

    static void init(Context context) throws NoSuchAlgorithmException {
        DHT=new Map[5];
        initNetworkValues();
    }

    static boolean strContains(String source, String subItem){
        String pattern = "\\b"+subItem+"\\b";
        Pattern p=Pattern.compile(pattern);
        Matcher m=p.matcher(source);
        return m.find();
    }


    static void handleNetworkRequests(SimpleDynamoProvider provider, Context context, String strReceived) throws NoSuchAlgorithmException {
        Log.d("HANDLE",strReceived+"|");
        if(strContains(strReceived,forwardreq))
        {
            String [] msgs=strReceived.split(forwardreq)[1].split(",");

            ContentValues cv = new ContentValues();
            cv.put("key", msgs[0]);
            cv.put("value", msgs[1]);
            cv.put("tstp",msgs[2]);
            provider.insert(null, cv);
        }
        else if(strContains(strReceived,blind_insert)){
            String [] msgs=strReceived.split(blind_insert)[1].split(",");
            String key=msgs[0];
            String value=msgs[1];
            String tstp=msgs[2];
            ringData.put(genHash(key),-1);
            Log.d("blindins",key+","+value);
            insertIntoStorage(key, value, Long.parseLong(tstp));
        }
        else if(strContains(strReceived,forwardquery))
        {
            String [] data=strReceived.split(forwardquery)[1].split(",");
            String restoredText = provider.process_query(data[1],data[0]);
        }
        else if(strContains(strReceived,forwarddelete))
        {
            String key=strReceived.split(forwarddelete)[1];
            provider.delete(null, key, null);
        }
        else if(strContains(strReceived,queryresponse))
        {
            answer=strReceived.split(queryresponse)[1];
            Log.d("qr", answer);
            String[] sp=answer.split(",");
            msgData.put(sp[0],sp[1]);
        }
        else if(strContains(strReceived,get_all_keys))
        {
            String sendto=strReceived.split(get_all_keys)[1];

            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, allkeys + getmyPort(context) + "|" + provider.getAllKeys(true), sendto);
        }
        else if(strContains(strReceived,get_all_keys_at))
        {
            String sendto=strReceived.split(get_all_keys_at)[1];
            String [] sp=sendto.split(",");
            sendto=sp[0];

            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, at_bundle + getmyPort(context)+"-"+sp[1] + "|" + provider.getAllKeys(true), sendto);
        }
        else if(strContains(strReceived,at_bundle)){
            String[] keyArr=strReceived.split(at_bundle)[1].split("\\|");
            String[] spl=keyArr[0].split(",");
            String recvfrom=spl[0];
            String [] sp=recvfrom.split("-");
            recvfrom=sp[0];

            int ind=getIndex(recvfrom);

            for(int i=1;i<keyArr.length;i++) {
                String[] data = keyArr[i].split(",");

                String key=data[0];
                String val=data[1];
                whoami status = who_am_I(key);
                if(status==whoami.COORDINATOR || status==whoami.REPLICA1 || status==whoami.REPLICA2)
                    provider.insertLatest(0, key, val, System.currentTimeMillis(), false);
            }
            if(sp[1].equals("1"))
                at_query_status1=1;
            else
                at_query_status2=1;
        }
        else if(strContains(strReceived,bundleforwardreq)){
            String[] keyArr=strReceived.split(bundleforwardreq)[1].split("\\|");
            String[] spl=keyArr[0].split(",");
            String recvfrom=spl[0];

            int ind=getIndex(recvfrom);

            for(int i=1;i<keyArr.length;i++)
            {
                String[] data=keyArr[i].split(",");
               String key=data[0];
                String val=data[1];
                Long t1 = Long.parseLong(data[2]);
                String already= pref.getString(key, null);
                if(already!=null) {
                    Long t2 = Long.parseLong(already.split(",")[1]);
                    if (t1 < t2) {
                        val = already.split(",")[0];
                        t1=t2;
                    }
                }
                whoami status = who_am_I(key);
                if(status==whoami.COORDINATOR || status==whoami.REPLICA1 || status==whoami.REPLICA2)
                    insertIntoStorage(key, val, t1);
            }
        }
        else if(strContains(strReceived,allkeys))
        {
            String[] keyArr=strReceived.split(allkeys)[1].split("\\|");
            String[] spl=keyArr[0].split(",");
            String recvfrom=spl[0];
            int wait=0;


            int ind=getIndex(recvfrom);
            DHT[ind]=new HashMap<String,String>();
            for(int i=1;i<keyArr.length;i++)
            {
                String[] data=keyArr[i].split(",");
                DHT[ind].put(data[0],data[1]);
            }

            for(int i=0;i<5;i++)
                if(i!=getIndex(MYPORT) && DHT[i]==null && !dead.contains(oREMOTE_PORT[i])) {
                    wait = 1;
                    break;
                }

            Log.d("wr", wait+","+recvfrom);
            if(wait==0)
            {
                MatrixCursor cursor = (MatrixCursor) provider.getAllKeys(false);
                for(int i=0;i<5;i++)
                    if(i!=getIndex(getmyPort(context)) && DHT[i]!=null)
                        for(Map.Entry<String,?> entry : DHT[i].entrySet())
                            cursor.addRow(new String[]{entry.getKey(), entry.getValue().toString()});
                DHTcursor=cursor;
            }
        }

    }
}