package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Hashtable;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


@SuppressWarnings("SpellCheckingInspection")
public class SimpleDhtProvider extends ContentProvider {

    public SQLiteHelper databaseHelper;
    SQLiteDatabase database;

    public static final String DATABASE_NAME = "SimpleDHT";
    public static final int DATABASE_VERSION = 1;
    public static final String TABLE_NAME = "messages";


    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;


    Hashtable<String, String> queryResults = new Hashtable();

    int successor;
    int predecessor;

    int myPort;
    String myHash;

    int leader=11108;



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String key = selection;

        Message message = new Message("delete");
        message.setKey(key);
        message.setPort(String.valueOf(myPort));
        message.setDestination(String.valueOf(successor));

        if(key.equals("*")) {
            databaseHelper.getWritableDatabase().rawQuery("DELETE * from messages", null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            return 0;
        }
        else if(key.equals("@")) {
            databaseHelper.getWritableDatabase().rawQuery("DELETE * from messages",null);
            return 0;
        }

        boolean forward = lookup(key);
        if (forward)
        {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            return 0;
        }

        String[] sArgs = {key};
        databaseHelper.getReadableDatabase().delete("messages", "key = ?", sArgs);

        return 0;

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = values.getAsString("key");
        String value = values.getAsString("value");

        boolean forward = lookup(key);
        Log.v("INSERT","\n key "+key+" value "+value);
        if(forward) {
            Log.v("INSERT","Forwarding");

            // DONE: Forward to successor.

            Message message = new Message("insert");
            message.setKey(key);
            message.setValue(value);
            message.setDestination(String.valueOf(successor));
            message.setPort(String.valueOf(myPort));



            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);

            return uri;

        }
        else {
            Log.v("INSERT","Saving to local partition");

            database = databaseHelper.getWritableDatabase();
            // Replace method to store only the most recent value
            database.replace(TABLE_NAME, null, values);
            database.close();


            Log.v("insert", values.toString());
            return uri;
        }
    }

    @Override
    public boolean onCreate() {
        databaseHelper=new SQLiteHelper(getContext(),DATABASE_NAME,DATABASE_VERSION);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE); // need getContext() to use getSystemService
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort =(Integer.parseInt(portStr)) * 2;

        try {
            myHash = genHash(portStr);
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
        }


        // Server Task to listen to incoming messages.
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        predecessor=-1;
        successor=-1;


        // Send join message to leader.

        if( myPort != (leader) ) {
            Message join = new Message("join");

            join.setPort(String.valueOf(myPort));
            join.setDestination(String.valueOf(leader));
            join.setBody("Join!");

            // Send join request.
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, join);

        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {


        String key = selection;

        if(key.equals("*")) {
            Log.v("QUERY", "Returning data from all partitions");

            Cursor cursor =  databaseHelper.getReadableDatabase().rawQuery("SELECT * from messages",null);

            if(successor == -1)
                return cursor;


            try {

                Message message = new Message("query");
                message.setKey(key);
                Log.v("QUERY","Sending "+key+" to "+String.valueOf(successor));
                message.setDestination(String.valueOf(successor));
                message.setPort(String.valueOf(myPort));


                int port= Integer.parseInt(message.getDestination());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),port);

                ObjectOutputStream outgoingStream = new ObjectOutputStream(socket.getOutputStream());
                outgoingStream.writeObject(message);
                outgoingStream.flush();
                outgoingStream.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            // Add key,values from queryResults to cursor.

            Log.v("QUERY","Sleeping");

            // Using Hashtable here kills the process for some reason.

            try {
                Thread.sleep(1000);
            }
            catch(InterruptedException e) {
                e.printStackTrace();

            }

            Log.v("QUERY","Waking up");

            String [] fields = {"key","value"};
            MatrixCursor newCursor = new MatrixCursor(fields);
            for (String k:queryResults.keySet())
            {
                String v = queryResults.get(k);
                String [] row = {k,v};

                newCursor.addRow(row);
            }

            // From stack overflow
            MergeCursor mergeCursor = new MergeCursor(new Cursor[]{newCursor,cursor});



            return mergeCursor;

        }

        if(key.equals("@")) {
            Log.v("QUERY", "Returning data from local partition");
            return databaseHelper.getReadableDatabase().rawQuery("SELECT * from messages",null);
        }

        // Normal Query
        boolean forward = lookup(key);

        if(forward) {

            Log.v("QUERY","key "+key);

            Log.v("QUERY", "Forwarding");


            Message message = new Message("query");
            message.setKey(key);
            message.setDestination(String.valueOf(successor));
            message.setPort(String.valueOf(myPort));



            // new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);

            try {
                int port= Integer.parseInt(message.getDestination());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),port);

                ObjectOutputStream outgoingStream = new ObjectOutputStream(socket.getOutputStream());
                outgoingStream.writeObject(message);
                outgoingStream.flush();
                outgoingStream.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            // Sleeping till my HashTable(queryResults) conatins the value for requested key.
            while(true){
                Log.v("QUERY","Sleeping");

                try {
                    Thread.sleep(10);
                }
                catch(InterruptedException e) {
                    e.printStackTrace();

                }
                if(queryResults.get(key) != null) {
                    break;
                }
            }

            String [] fields = {"key","value"};
            MatrixCursor cursor = new MatrixCursor(fields);
            String [] values = {key,queryResults.get(key)};
            cursor.addRow(values);

            Log.v("QUERY_RESULT", key);
            return cursor;




        }
        else {
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(TABLE_NAME);
            Cursor cursor = queryBuilder.query(databaseHelper.getReadableDatabase(),
                    projection, "key=?" , new String[]{key}, null, null, sortOrder);

            database.close();

            Log.v("QUERY_RESULT", key);
            return cursor;
        }



    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean lookup(String key) {
        try {
            if(predecessor == -1 && successor == -1)
                return false;


            String keyHash = genHash(key);
            String predecessorHash = genHash(String.valueOf(predecessor / 2));
            String myHash = genHash(String.valueOf(myPort/2));
            Log.v("LOOKUP_","Comparing "+keyHash+"\n with "+predecessorHash+" \n and "+myHash);

            if(predecessorHash.compareTo(myHash) > 0) {
                if (keyHash.compareTo(predecessorHash) > 0 || keyHash.compareTo(myHash) <= 0) {
                    return false;
                }else {
                    return true;
                }
            }
            else if(keyHash.compareTo(predecessorHash) > 0 && keyHash.compareTo(myHash) <= 0)
                return false;
            else
                return true;

        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
            return true; // Supress Android Studio warning.
        }

    }



    // Added by akannan4

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];


            try {
                while(true) {
                    Socket incomingSocket = serverSocket.accept();

                    ObjectInputStream incomingStream = new ObjectInputStream(incomingSocket.getInputStream());

                    Message message = null;

                    try {
                        message = (Message) incomingStream.readObject();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    Log.v("incoming", " Received a message of type " + message.getType() + " from " + String.valueOf(message.getPort()));

                    incomingSocket.close();

                    String type=message.getType();

                    if(type.equals("update_successor"))
                    {
                        successor = Integer.parseInt(message.getPort());
                        Log.v("RING_UPDATE","My predeccessor is "+String.valueOf(predecessor));
                        Log.v("RING_UPDATE","My new successor is "+String.valueOf(successor));


                    }
                    else if(type.equals("update_predecessor")){
                        predecessor = Integer.parseInt(message.getPort());
                        Log.v("RING_UPDATE","My new predecessor is "+String.valueOf(predecessor));
                        Log.v("RING_UPDATE","My successor is "+String.valueOf(successor));


                    }
                    else if(type.equals("insert")){

                        String key = message.getKey();
                        String value = message.getValue();
                        Log.v("INSERT","key "+key+" value "+value);
                        ContentValues values = new ContentValues();

                        values.put("key", key);
                        values.put("value", value);

                        insert(null,values);

                    }
                    else if(type.equals("delete")){

                        String key = message.getKey();
                        String requestingNode = message.getPort();

                        if(key.equals("*")) {
                            databaseHelper.getWritableDatabase().rawQuery("DELETE * from messages", null);

                            if(!requestingNode.equals(String.valueOf(myPort))) {
                                message.setDestination(String.valueOf(successor));
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                            }
                        }

                        boolean forward = lookup(key);
                        if (forward)
                        {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        }
                        else {
                            String[] sArgs = {key};
                            databaseHelper.getReadableDatabase().delete("messages", "key = ?", sArgs);
                        }


                    }
                    else if(type.equals("query")){


                        String key = message.getKey();
                        boolean forward = lookup(key);
                        String requestingNode = message.getPort();



                        if(key.equals("*") && !requestingNode.equals(String.valueOf(myPort))) {
                            forward = false;

                            Cursor cursor = databaseHelper.getReadableDatabase().rawQuery("SELECT * from messages",null);

                            Message result = new Message("result");

                            result.setKey("*");
                            result.setDestination(requestingNode);
                            result.setPort(String.valueOf(myPort));

                            cursor.moveToPosition(-1);
                            result.body = "";
                            while(cursor.moveToNext()) {

                                result.body += cursor.getString(0)+","+ cursor.getString(1)+";";
                            }
                            cursor.close();
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, result);

                            message.setDestination(String.valueOf(successor));
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                        }
                        else if (key.equals("*") && requestingNode.equals(String.valueOf(myPort))) {

                            forward = false;

                        }
                        if(forward) { // Forwarding request
                            Log.v("QUERY","Forwarding query request for key " + key + " from " +requestingNode+" to "+String.valueOf(successor));
                            message.setDestination(String.valueOf(successor));
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                        }
                        else if(!key.equals("*")) { // Querying local DB


                            Log.v("QUERY","Querying local partition for query request for key " + key + " from " +requestingNode);

                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(TABLE_NAME);
                            Cursor cursor = queryBuilder.query(databaseHelper.getReadableDatabase(),
                                    null, "key=?" , new String[]{key}, null, null, null);


                            String value = "key_not_found";

                            if (cursor.moveToFirst())
                                value = cursor.getString(cursor.getColumnIndex("value"));

                            cursor.close();

                            Message result = new Message("result");
                            result.setDestination(requestingNode);
                            result.setPort(String.valueOf(myPort));
                            result.setKey(key);
                            result.setValue(value);

                            Log.v("QUERY_RESULT", "key " + key + " value " + value);


                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, result);

                        }

                    }
                    else if(type.equals("result")){


                        String key = message.getKey();
                        String value = message.getValue();

                        if(key.equals("*")) {
                            Log.v("QUERY_ALL_REPLY","Received query all reply from "+message.getPort());
                            Log.v("QUERY_ALL_REPLY","Received data "+message.getBody());

                            StringBuilder messages = new StringBuilder(message.getBody());
                            if(messages.length()<2) break;

                            messages.deleteCharAt(message.getBody().length()-1);
                            String [] allMessages = messages.toString().split(";");

                            for( String msg : allMessages) {
                                String[] msgParts = msg.split(",");
                                key = msgParts[0];
                                value = msgParts[1];
                                Log.v("QUERY_ALL_REPLY","key "+key+" value "+value);
                                queryResults.put(key,value);

                            }



                        }
                        else {
                            queryResults.put(key,value);
                            Log.v("QUERY_RESULT", "Recevied result from " + message.getPort() + " key " + key + " value " + value);

                        }


                    }
                    else if(type.equals("join")){
                        String newNodeHash = "_";


                        try {
                            newNodeHash = genHash(String.valueOf(Integer.parseInt(message.getPort())/2));
                            Log.v("RING_","Join Request "+message.getPort()+" \n "+newNodeHash);


                        }
                        catch(NoSuchAlgorithmException e) {
                            e.printStackTrace();
                            System.exit(1);
                        }

                        if(predecessor == -1 && successor == -1) { // First node is trying join me.
                            successor =  Integer.parseInt(message.getPort());
                            predecessor =  Integer.parseInt(message.getPort());

                            // DONE: Tell successor to update his predecessor.
                            Message newMessage = new Message("update_predecessor");

                            newMessage.setPort(String.valueOf(myPort));
                            newMessage.setDestination(String.valueOf(successor));
                            newMessage.setBody("update_predecessor");



                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);

                            // DONE: Tell predecessor to uo update his successor
                            newMessage = new Message("update_successor");

                            newMessage.setPort(String.valueOf(myPort));
                            newMessage.setDestination(String.valueOf(predecessor));
                            newMessage.setBody("update_successor");

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);



                        }
                        else {
                        /* Start logic */
                            Boolean forward = lookup(String.valueOf(Integer.parseInt(message.getPort()) / 2));

                            if (!forward) {
                                Log.v("RING_",message.getPort()+" is in my range.");
                                // Tell new node to update his predecessor.
                                Message newMessage = new Message("update_predecessor");
                                newMessage.setPort(String.valueOf(predecessor));
                                newMessage.setDestination(String.valueOf(message.getPort()));
                                newMessage.setBody("update_predecessor");

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);


                                // Tell new node to update his successor.
                                newMessage = new Message("update_successor");
                                newMessage.setPort(String.valueOf(myPort));
                                newMessage.setDestination(String.valueOf(message.getPort()));
                                newMessage.setBody("update_successor");

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);


                                // Tell my old predecessor to update his successor.
                                newMessage = new Message("update_successor");
                                newMessage.setPort(String.valueOf(message.getPort()));
                                newMessage.setDestination(String.valueOf(predecessor));
                                newMessage.setBody("update_successor");

                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);

                                // Finally, update self
                                predecessor = Integer.valueOf(message.getPort());
                                Log.v("RING_UPDATE", "My new predecessor is " + String.valueOf(predecessor));
                                Log.v("RING_UPDATE","My successor is "+String.valueOf(successor));


                            } else {
                                Log.v("RING_",message.getPort()+" is not my range. Forwarding to "+String.valueOf(successor));

                                message.setDestination(String.valueOf(successor));
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);


                            }
                        }

                        /* End logic */



                    }
                }

            } catch (IOException e) {
                Log.e(TAG, "I am not able to accept the incoming connection! :( ");
            }
            return null;
        }

        protected void onProgressUpdate(Message...messages) {
            return;
        }



    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            // abstracted, so that thread doesn't know type of message :)

            Log.v("outgoing","Sending "+message.getType()+ " to "+message.getDestination() + " ( port "+message.getPort()+" )");

            try {
                int port= Integer.parseInt(message.getDestination());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),port);
                Log.v(TAG, "Socket to " + String.valueOf(port) + " created");


                ObjectOutputStream outgoingStream = new ObjectOutputStream(socket.getOutputStream());

                outgoingStream.writeObject(message);

                outgoingStream.flush();
                outgoingStream.close();
                socket.close();

                Log.v("outgoing", "Sent " + message.getType() + " to " + message.getDestination() + " ( port " + message.getPort() + " )");


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }


    }
}
