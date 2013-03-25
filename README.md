# TouchDB-Android #


## Status
- this is a fork from https://github.com/couchbaselabs/TouchDB-Android
- I am willing to receive issues, whishes, etc. and will try to incorporate them
- See the include Android project "MediaLib" as a usage example

## Improvements
- added support for large attachments, as they crashed the version from above
- added callback mechanism for replication. This works as follows:

set a "ReplicationCallback" on your "TDServer" instance

```Java
server.setReplicationCallback(new ReplicationCallback() {
    @Override
    public void onTimeout() {
        // do something
    }
});
```

set continious mode on your "ReplicationCommand" and specify timeout != -1

```Java
ReplicationCommand replicationCommand;
        replicationCommand = new ReplicationCommand.Builder()
                .target(database)
                .source(serverURL+"/"+database)
                .continuous(true)
                .timeout(10000)
                .build();
});
```
If no changes are received in the changes tracker for the specified amount of time,
the timeout callback will be invoked. You can use that to start a replication and 
receive a notification, once it is complete.


## Sample Activity for MediaLib
```Java
public class MainActivity extends Activity {
    private MediaWebView webView;
    private MediaServer mediaServer;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        File filesDir = new File(Environment.getExternalStorageDirectory(), "sample");
        
        mediaServer = new MediaServer(8888, filesDir.getAbsolutePath());
        mediaServer.start();
        mediaServer.replicate("http://your.domain:5984", "yourdb"true, 5000, new ReplicationCallback() {
            @Override
        	public void onTimeout() {
        		webView.loadUrl("http://0.0.0.0:8888/yourdb/_design/yourdoc/index.html");
        	}
        });
        webView = new MediaWebView(MainActivity.this, "http://0.0.0.0:8888/yourdb/_design/home/index.html", mediaServer);
        
        setContentView(webView);
        
    }

}
```

## Requirements
- Android 2.2 or newer
- Jackson JSON Parser/Generator

## License
- Apache License 2.0

## Known Issues
- Exception Handling in the current implementation makes things less readable.  This was a deliberate decision I made to make it more of a literal port of the iOS version.  Once the majority of code is in place and working I would like to revisit this and handle exceptions in more natural Android/Java way.

## TODO
- Finish porting all of TDRouter so that all operations are supported

## Getting Started using TouchDB-Android

See the Wiki:  https://github.com/couchbaselabs/TouchDB-Android/wiki
