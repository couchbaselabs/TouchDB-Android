package com.fscz.medialib;

import org.ektorp.CouchDbInstance;
import org.ektorp.DbAccessException;
import org.ektorp.DbPath;
import org.ektorp.ReplicationCommand;
import org.ektorp.impl.StdCouchDbInstance;

import android.util.Log;

import com.couchbase.touchdb.TDDatabase;
import com.couchbase.touchdb.TDServer;
import com.couchbase.touchdb.TDView;
import com.couchbase.touchdb.ektorp.TouchDBHttpClient;
import com.couchbase.touchdb.javascript.TDJavaScriptViewCompiler;
import com.couchbase.touchdb.listener.TDListener;
import com.couchbase.touchdb.support.ReplicationCallback;

public class MediaServer extends Thread implements DBAccess {
	private TDListener listener;
    private TDServer server;
    private TouchDBHttpClient httpClient;
    private CouchDbInstance dbInstance;
    private String mFilesDir;
    private int mPort;
    public static final String TAG = "MediaServer";
    
    public MediaServer(int port, String filesDir) {
    	mFilesDir = filesDir;
    	mPort = port;
    }
    
    @Override 
    public void start() {
        try {
        	server = new TDServer(mFilesDir);
            listener = new TDListener(server, mPort);
            httpClient =  new TouchDBHttpClient(server);
            dbInstance = new StdCouchDbInstance(httpClient);
            TDView.setCompiler(new TDJavaScriptViewCompiler());
            listener.start();
        } catch (DbAccessException dbaex) {
	        Log.e(TAG, "cannot connect db, error: "+dbaex.getMessage());
	        dbaex.printStackTrace();
        }
        catch (Exception e) {
            Log.e(TAG, "Unable to create TDServer", e);
        }
    }
    
    public void replicate(String serverURL, String database, boolean continuous, int timeout, ReplicationCallback cb) {
    	try {
    		dbInstance.createDatabase(database);
    	} catch (Exception e){}
    	server.setReplicationCallback(cb);
    	ReplicationCommand replicationCommand;
        replicationCommand = new ReplicationCommand.Builder()
                .target(database)
                .source(serverURL+"/"+database)
                .continuous(continuous)
                .timeout(timeout)
                .build();
        dbInstance.replicate(replicationCommand);
    }
    
    public TDDatabase getDatabase(String name, boolean create) {
    	return server.getDatabaseNamed(name, create);
    }
}
