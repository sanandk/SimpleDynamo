package edu.buffalo.cse.cse486586.simpledynamo;

import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Map;
import java.util.Timer;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import static edu.buffalo.cse.cse486586.simpledynamo.Ring.*;
import static edu.buffalo.cse.cse486586.simpledynamo.NetworkTasks.*;

public class SimpleDynamoProvider extends ContentProvider {

    private static String spname="DHT";
    public static SharedPreferences pref;

    void deleteFromStorage(String selection){
        Log.v("delete", selection);
        SharedPreferences.Editor editor = pref.edit();
        editor.remove(selection);
        editor.commit();
    }

    static void insertIntoStorage(String key, String value, Long tstp){
        value=value.concat(","+tstp);
        Log.v("insert", key + "," + value);
        SharedPreferences.Editor editor = pref.edit();
        editor.putString(key, value);
        editor.commit();
    }



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // Deletion is initated by coordinator only

        try {
            ringData.put(genHash(selection), -1);
            whoami status=who_am_I(selection);
            int destnode=who_is_owner_ind(selection);

            if(status==whoami.OWNER || status==whoami.USELESS){
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwarddelete+selection, REMOTE_PORT[(destnode+1)%5]);
            }
            else
            {
                String restoredText = pref.getString(selection, null);
                if(restoredText!=null)
                    deleteFromStorage(selection);
                if(status==whoami.COORDINATOR){
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwarddelete+selection, REMOTE_PORT[(destnode+2)%5]);
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwarddelete+selection, REMOTE_PORT[(destnode+3)%5]);
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
    public void insertLatest(int myind, String key, String value, Long timenow, boolean inform){
        String data=pref.getString(key, null);
        if(data==null){
            insertIntoStorage(key, value, timenow);
        }
        else
        {
            String[] sp=data.split(",");
            Long ot=Long.parseLong(sp[1]);
            if(timenow>ot)                      // Received request is latest!
            {
                insertIntoStorage(key, value, timenow);
            }
            else                                // Old is latest!
            {
                value=sp[0];
                timenow=ot;
            }
        }

        if(inform) {
            // Inform mates to blindly insert
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, blind_insert + key + "," + value + "," + timenow, REMOTE_PORT[(myind + 1) % 5]);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, blind_insert + key + "," + value + "," + timenow, REMOTE_PORT[(myind + 2) % 5]);
        }
    }

    @Override
     public Uri insert(Uri uri, ContentValues values) {
        // Only coordinator resolves disputes in latest message and later asks mates to blindly insert
        Long timenow=System.currentTimeMillis();
        if(uri==null)
            timenow=Long.parseLong(values.getAsString("tstp"));

        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            ringData.put(genHash(key),-1);
            String p1=null,p2=null,p3=null;
            whoami status = who_am_I(key);
            int destnode=who_is_coord_ind(key);

            if(status==whoami.COORDINATOR)
                insertLatest(destnode, key, value, timenow, true);
            else
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardreq + key + "," + value + "," + timenow, REMOTE_PORT[destnode]);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return uri;
    }


    @Override
    public boolean onCreate() {

        pref= getContext().getSharedPreferences(spname, getContext().MODE_PRIVATE);
        try {
            init(getContext());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        /*
         * Create a server socket as well as a thread (AsyncTask) that listens on the server
         * port.
         *
         * AsyncTask is a simplified thread construct that Android provides. Please make sure
         * you know how it works by reading
         * http://developer.android.com/reference/android/os/AsyncTask.html
         */

        new ServerTask(getContext(), this).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, SERVER_SOCKET);

        SharedPreferences pref2 = getContext().getSharedPreferences(spname + "init", getContext().MODE_PRIVATE);
        String init=pref2.getString("init", null);
        getmyPort(getContext());
        if(init==null){
            // First Time
            SharedPreferences.Editor ed=pref2.edit();
            ed.putString("init", "start");
            ed.commit();
        }else
        {
            SharedPreferences.Editor edit=pref.edit();
            edit.clear();
            edit.commit();

            Log.e("DEATH", "I am recovered! ;) ");
            for(int i=0;i<5;i++)
                if(oREMOTE_PORT[i]!=MYPORT)
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "IAMREBORN " + MYPORT + " " + System.currentTimeMillis(), oREMOTE_PORT[i], "dont");
        }

        return false;
    }
    public static int at_query_status1=0, at_query_status2=0;
    public static Object getAllKeys(boolean serialize) throws NoSuchAlgorithmException {
        StringBuffer sb=new StringBuffer();
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});

        Map<String,?> keys = pref.getAll();
        String selection, restoredText;
        int destnode;
        for(Map.Entry<String,?> entry : keys.entrySet()){
            selection=entry.getKey();
            whoami myself = who_am_I(selection);
            destnode = who_is_coord_ind(selection);
            if(myself==whoami.OWNER || myself==whoami.USELESS)
            {
                continue;
            }
            if(entry.getValue()==null)
                restoredText=null;
            else
                restoredText = entry.getValue().toString();

            Log.d("starquery",selection+","+restoredText);
            if(serialize) {
                sb.append(selection);
                sb.append(",");
                sb.append(restoredText);
                sb.append("|");
            }
            else {
                restoredText=restoredText.split(",")[0];
                cursor.addRow(new String[]{selection, restoredText});
            }
        }
        if(serialize)
            return sb.toString();
        else
            return cursor;
    }

     public String process_query(String forwhom, String selection) {
        String restoredText=null;
        try {
            ringData.put(genHash(selection), -1);
            whoami me = who_am_I(selection);
            int destnode = who_is_coord_ind(selection);

            if(me==whoami.COORDINATOR){
                restoredText = pref.getString(selection, "null");
                Log.v("query", selection + "," + restoredText);
                while(restoredText==null || restoredText.equals("null"))
                {
                    // I am dying hard here to get the query answer!
                    if(me==whoami.COORDINATOR) {
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery + selection + "," + forwhom, REMOTE_PORT[(destnode + 1) % 5]);
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery + selection + "," + forwhom, REMOTE_PORT[(destnode + 2) % 5]);
                    }
                    else {
                        if(REMOTE_PORT[(destnode)%5].equals(MYPORT))
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery + selection + "," + forwhom, REMOTE_PORT[(destnode+1)%5]);
                        else
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery + selection + "," + forwhom, REMOTE_PORT[(destnode)%5]);
                    }
                    restoredText = pref.getString(selection, "null");
                }
                if (forwhom != null)   // query is not initiated by me, so I just answer the initiator
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, queryresponse + selection + "," + restoredText , forwhom);
            }
            else {
                // I will forward to the coordinator, he ll take care
                if(forwhom==null)
                    forwhom=MYPORT;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, forwardquery + selection + "," + forwhom, REMOTE_PORT[destnode]);
            }
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
         if(restoredText!=null)
            restoredText=restoredText.split(",")[0];
         return restoredText;
    }

    @Override
     public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        Log.d("qsel", selection);
        if(selection.equals("\"*\""))
        {
            DHTcursor=null;
            DHT=new Map[5];

            for(int i=0;i<5;i++)
                if(!REMOTE_PORT[i].equals(MYPORT))
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, get_all_keys + getmyPort(getContext()),REMOTE_PORT[i]);

                try {
                    while (DHTcursor == null)
                        Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                cursor = DHTcursor;
        }
        else if(selection.equals("\"@\""))
        {
            try {
                cursor=(MatrixCursor) getAllKeys(false);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else {
            String restoredText = null;
            String [] sp;
            restoredText = process_query(null,selection);

            if(restoredText==null) {    // forwarded query
                try {
                    while (msgData.get(selection)==null)  // wait for reply
                        Thread.sleep(500);
                    Log.d("queryans","Thread woke up!"+msgData.get(selection));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                restoredText=msgData.get(selection);
                msgData.remove(selection);
            }
            Log.d("queryans",restoredText);
            cursor.addRow(new String[]{selection, restoredText});
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

}

