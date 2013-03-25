package com.couchbase.touchdb.listener;

import java.util.concurrent.ScheduledExecutorService;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;

import com.couchbase.touchdb.TDServer;
import com.couchbase.touchdb.router.TDURLStreamHandlerFactory;

public class TDListener implements Runnable {

    private Thread thread;
    private TDHTTPServer httpServer;
    private TDServer server;

    //static inializer to ensure that touchdb:// URLs are handled properly
    {
        TDURLStreamHandlerFactory.registerSelfIgnoreError();
    }

    public TDListener(TDServer server, int port) {
        this.httpServer = new TDHTTPServer();
        this.server = server;
        this.httpServer.setServer(server);
        this.httpServer.setListener(this);
        this.httpServer.setPort(port);
    }

    @Override
    public void run() {
        httpServer.serve();
    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

    public void stop() {
        httpServer.notifyStop();
    }

    public void onServerThread(Runnable r) {
        ScheduledExecutorService workExecutor = this.server.getWorkExecutor();
        workExecutor.submit(r);
    }

    public String getStatus() {
        String status = this.httpServer.getServerInfo();
        return status;
    }

}
