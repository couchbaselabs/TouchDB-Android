package com.couchbase.touchdb.testapp.tests;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.codehaus.jackson.map.ObjectMapper;

import android.test.InstrumentationTestCase;
import android.util.Log;

import com.couchbase.touchdb.TDBody;
import com.couchbase.touchdb.TDDatabase;
import com.couchbase.touchdb.TDServer;
import com.couchbase.touchdb.TDStatus;
import com.couchbase.touchdb.TDView;
import com.couchbase.touchdb.TDViewMapBlock;
import com.couchbase.touchdb.TDViewMapEmitBlock;
import com.couchbase.touchdb.router.TDRouter;
import com.couchbase.touchdb.router.TDURLConnection;
import com.couchbase.touchdb.router.TDURLStreamHandlerFactory;
import com.couchbase.touchdb.support.DirUtils;

public class Router extends InstrumentationTestCase {
    private static boolean initializedUrlHandler = false;

    public static final String TAG = "Router";

    protected String getServerPath() {
        String filesDir = getInstrumentation().getContext().getFilesDir().getAbsolutePath() + "/tests";
        return filesDir;
    }

    @Override
    protected void setUp() throws Exception {

        //delete and recreate the server path
        String serverPath = getServerPath();
        File serverPathFile = new File(serverPath);
        DirUtils.deleteRecursive(serverPathFile);
        serverPathFile.mkdir();

        //for some reason a traditional static initializer causes junit to die
        if(!initializedUrlHandler) {
            URL.setURLStreamHandlerFactory(new TDURLStreamHandlerFactory());
            initializedUrlHandler = true;
        }
    }

    protected static TDURLConnection sendRequest(TDServer server, String method, String path, Map<String,String> headers, Object bodyObj) {
        try {
            URL url = new URL("touchdb://" + path);
            TDURLConnection conn = (TDURLConnection)url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod(method);
            if(headers != null) {
                for (String header : headers.keySet()) {
                    conn.setRequestProperty(header, headers.get(header));
                }
            }
            Map<String, List<String>> allProperties = conn.getRequestProperties();
            if(bodyObj != null) {
                conn.setDoInput(true);
                ObjectMapper mapper = new ObjectMapper();
                OutputStream os = conn.getOutputStream();
                os.write(mapper.writeValueAsBytes(bodyObj));
            }

            TDRouter router = new TDRouter(server, conn);
            router.start();
            return conn;
        } catch (MalformedURLException e) {
            fail();
        } catch(IOException e) {
            fail();
        }
        return null;
    }

    protected static Object parseJSONResponse(TDURLConnection conn) {
        Object result = null;
        TDBody responseBody = conn.getResponseBody();
        if(responseBody != null) {
            byte[] json = responseBody.getJson();
            String jsonString = null;
            if(json != null) {
                jsonString = new String(json);
                ObjectMapper mapper = new ObjectMapper();
                try {
                    result = mapper.readValue(jsonString, Object.class);
                } catch (Exception e) {
                    fail();
                }
            }
        }
        return result;
    }

    protected static Object sendBody(TDServer server, String method, String path, Object bodyObj, int expectedStatus, Object expectedResult) {
        TDURLConnection conn = sendRequest(server, method, path, null, bodyObj);
        Object result = parseJSONResponse(conn);
        Log.v(TAG, String.format("%s %s --> %d", method, path, conn.getResponseCode()));
        Assert.assertEquals(expectedStatus, conn.getResponseCode());
        if(expectedResult != null) {
            Assert.assertEquals(expectedResult, result);
        }
        return result;
    }

    protected static Object send(TDServer server, String method, String path, int expectedStatus, Object expectedResult) {
        return sendBody(server, method, path, null, expectedStatus, expectedResult);
    }

    public void testServer() {

        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        Map<String,Object> responseBody = new HashMap<String,Object>();
        responseBody.put("TouchDB", "Welcome");
        responseBody.put("couchdb", "Welcome");
        responseBody.put("version", TDRouter.getVersionString());
        send(server, "GET", "/", TDStatus.OK, responseBody);

        List<String> allDbs = new ArrayList<String>();
        send(server, "GET", "/_all_dbs", TDStatus.OK, allDbs);

        send(server, "GET", "/non-existant", TDStatus.NOT_FOUND, null);
        send(server, "GET", "/BadName", TDStatus.BAD_REQUEST, null);
        send(server, "PUT", "/", TDStatus.BAD_REQUEST, null);
        send(server, "POST", "/", TDStatus.BAD_REQUEST, null);

        server.close();
    }

    public void testDatabase() {

        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        send(server, "PUT", "/database", TDStatus.CREATED, null);

        Map<String,Object> dbInfo = (Map<String,Object>)send(server, "GET", "/database", TDStatus.OK, null);
        Assert.assertEquals(0, dbInfo.get("doc_count"));
        Assert.assertEquals(0, dbInfo.get("update_seq"));
        Assert.assertTrue((Integer)dbInfo.get("disk_size") > 8000);

        send(server, "PUT", "/database", TDStatus.PRECONDITION_FAILED, null);
        send(server, "PUT", "/database2", TDStatus.CREATED, null);

        List<String> allDbs = new ArrayList<String>();
        allDbs.add("database");
        allDbs.add("database2");
        send(server, "GET", "/_all_dbs", TDStatus.OK, allDbs);
        dbInfo = (Map<String,Object>)send(server, "GET", "/database2", TDStatus.OK, null);
        Assert.assertEquals("database2", dbInfo.get("db_name"));

        send(server, "DELETE", "/database2", TDStatus.OK, null);
        allDbs.remove("database2");
        send(server, "GET", "/_all_dbs", TDStatus.OK, allDbs);

        send(server, "PUT", "/database%2Fwith%2Fslashes", TDStatus.CREATED, null);
        dbInfo = (Map<String,Object>)send(server, "GET", "/database%2Fwith%2Fslashes", TDStatus.OK, null);
        Assert.assertEquals("database/with/slashes", dbInfo.get("db_name"));

        server.close();
    }

    public void testDocs() {

        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        send(server, "PUT", "/db", TDStatus.CREATED, null);

        // PUT:
        Map<String,Object> doc1 = new HashMap<String,Object>();
        doc1.put("message", "hello");
        Map<String,Object> result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc1", doc1, TDStatus.CREATED, null);
        String revID = (String)result.get("rev");
        Assert.assertTrue(revID.startsWith("1-"));

        // PUT to update:
        doc1.put("message", "goodbye");
        doc1.put("_rev", revID);
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc1", doc1, TDStatus.CREATED, null);
        Log.v(TAG, String.format("PUT returned %s", result));
        revID = (String)result.get("rev");
        Assert.assertTrue(revID.startsWith("2-"));

        doc1.put("_id", "doc1");
        doc1.put("_rev", revID);
        result = (Map<String,Object>)send(server, "GET", "/db/doc1", TDStatus.OK, doc1);

        // Add more docs:
        Map<String,Object> docX = new HashMap<String,Object>();
        docX.put("message", "hello");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc3", docX, TDStatus.CREATED, null);
        String revID3 = (String)result.get("rev");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc2", docX, TDStatus.CREATED, null);
        String revID2 = (String)result.get("rev");

        // _all_docs:
        result = (Map<String,Object>)send(server, "GET", "/db/_all_docs", TDStatus.OK, null);
        Assert.assertEquals(3, result.get("total_rows"));
        Assert.assertEquals(0, result.get("offset"));

        Map<String,Object> value1 = new HashMap<String,Object>();
        value1.put("rev", revID);
        Map<String,Object> value2 = new HashMap<String,Object>();
        value2.put("rev", revID2);
        Map<String,Object> value3 = new HashMap<String,Object>();
        value3.put("rev", revID3);

        Map<String,Object> row1 = new HashMap<String,Object>();
        row1.put("id", "doc1");
        row1.put("key", "doc1");
        row1.put("value", value1);
        Map<String,Object> row2 = new HashMap<String,Object>();
        row2.put("id", "doc2");
        row2.put("key", "doc2");
        row2.put("value", value2);
        Map<String,Object> row3 = new HashMap<String,Object>();
        row3.put("id", "doc3");
        row3.put("key", "doc3");
        row3.put("value", value3);

        List<Map<String,Object>> expectedRows = new ArrayList<Map<String,Object>>();
        expectedRows.add(row1);
        expectedRows.add(row2);
        expectedRows.add(row3);

        List<Map<String,Object>> rows = (List<Map<String,Object>>)result.get("rows");
        Assert.assertEquals(expectedRows, rows);

        // DELETE:
        result = (Map<String,Object>)send(server, "DELETE", String.format("/db/doc1?rev=%s", revID), TDStatus.OK, null);
        revID = (String)result.get("rev");
        Assert.assertTrue(revID.startsWith("3-"));

        send(server, "GET", "/db/doc1", TDStatus.NOT_FOUND, null);

        // _changes:
        value1.put("rev", revID);
        List<Object> changes1 = new ArrayList<Object>();
        changes1.add(value1);
        List<Object> changes2 = new ArrayList<Object>();
        changes2.add(value2);
        List<Object> changes3 = new ArrayList<Object>();
        changes3.add(value3);

        Map<String,Object> result1 = new HashMap<String,Object>();
        result1.put("id", "doc1");
        result1.put("seq", 5);
        result1.put("deleted", true);
        result1.put("changes", changes1);
        Map<String,Object> result2 = new HashMap<String,Object>();
        result2.put("id", "doc2");
        result2.put("seq", 4);
        result2.put("changes", changes2);
        Map<String,Object> result3 = new HashMap<String,Object>();
        result3.put("id", "doc3");
        result3.put("seq", 3);
        result3.put("changes", changes3);

        List<Object> results = new ArrayList<Object>();
        results.add(result3);
        results.add(result2);
        results.add(result1);

        Map<String,Object> expectedChanges = new HashMap<String,Object>();
        expectedChanges.put("last_seq", 5);
        expectedChanges.put("results", results);

        send(server, "GET", "/db/_changes", TDStatus.OK, expectedChanges);

        // _changes with ?since:
        results.remove(result3);
        results.remove(result2);
        expectedChanges.put("results", results);
        send(server, "GET", "/db/_changes?since=4", TDStatus.OK, expectedChanges);

        results.remove(result1);
        expectedChanges.put("results", results);
        send(server, "GET", "/db/_changes?since=5", TDStatus.OK, expectedChanges);

        server.close();
    }

    public void testLocalDocs() {

        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        send(server, "PUT", "/db", TDStatus.CREATED, null);

        // PUT a local doc:
        Map<String,Object> doc1 = new HashMap<String,Object>();
        doc1.put("message", "hello");
        Map<String,Object> result = (Map<String,Object>)sendBody(server, "PUT", "/db/_local/doc1", doc1, TDStatus.CREATED, null);
        String revID = (String)result.get("rev");
        Assert.assertTrue(revID.startsWith("1-"));

        // GET it:
        doc1.put("_id", "_local/doc1");
        doc1.put("_rev", revID);
        result = (Map<String,Object>)send(server, "GET", "/db/_local/doc1", TDStatus.OK, doc1);

        // Local doc should not appear in _changes feed:
        Map<String,Object> expectedChanges = new HashMap<String,Object>();
        expectedChanges.put("last_seq", 0);
        expectedChanges.put("results", new ArrayList<Object>());
        send(server, "GET", "/db/_changes", TDStatus.OK, expectedChanges);

        server.close();
    }

    public void testAllDocs() {
        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        send(server, "PUT", "/db", TDStatus.CREATED, null);

        Map<String,Object> result;
        Map<String,Object> doc1 = new HashMap<String,Object>();
        doc1.put("message", "hello");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc1", doc1, TDStatus.CREATED, null);
        String revID = (String)result.get("rev");
        Map<String,Object> doc3 = new HashMap<String,Object>();
        doc3.put("message", "bonjour");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc3", doc3, TDStatus.CREATED, null);
        String revID3 = (String)result.get("rev");
        Map<String,Object> doc2 = new HashMap<String,Object>();
        doc2.put("message", "guten tag");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc2", doc2, TDStatus.CREATED, null);
        String revID2 = (String)result.get("rev");

        // _all_docs:
        result = (Map<String,Object>)send(server, "GET", "/db/_all_docs", TDStatus.OK, null);
        Assert.assertEquals(3, result.get("total_rows"));
        Assert.assertEquals(0, result.get("offset"));

        Map<String,Object> value1 = new HashMap<String,Object>();
        value1.put("rev", revID);
        Map<String,Object> value2 = new HashMap<String,Object>();
        value2.put("rev", revID2);
        Map<String,Object> value3 = new HashMap<String,Object>();
        value3.put("rev", revID3);

        Map<String,Object> row1 = new HashMap<String,Object>();
        row1.put("id", "doc1");
        row1.put("key", "doc1");
        row1.put("value", value1);
        Map<String,Object> row2 = new HashMap<String,Object>();
        row2.put("id", "doc2");
        row2.put("key", "doc2");
        row2.put("value", value2);
        Map<String,Object> row3 = new HashMap<String,Object>();
        row3.put("id", "doc3");
        row3.put("key", "doc3");
        row3.put("value", value3);

        List<Map<String,Object>> expectedRows = new ArrayList<Map<String,Object>>();
        expectedRows.add(row1);
        expectedRows.add(row2);
        expectedRows.add(row3);

        List<Map<String,Object>> rows = (List<Map<String,Object>>)result.get("rows");
        Assert.assertEquals(expectedRows, rows);

        // ?include_docs:
        result = (Map<String,Object>)send(server, "GET", "/db/_all_docs?include_docs=true", TDStatus.OK, null);
        Assert.assertEquals(3, result.get("total_rows"));
        Assert.assertEquals(0, result.get("offset"));

        doc1.put("_id", "doc1");
        doc1.put("_rev", revID);
        row1.put("doc", doc1);

        doc2.put("_id", "doc2");
        doc2.put("_rev", revID2);
        row2.put("doc", doc2);

        doc3.put("_id", "doc3");
        doc3.put("_rev", revID3);
        row3.put("doc", doc3);

        List<Map<String,Object>> expectedRowsWithDocs = new ArrayList<Map<String,Object>>();
        expectedRowsWithDocs.add(row1);
        expectedRowsWithDocs.add(row2);
        expectedRowsWithDocs.add(row3);

        rows = (List<Map<String,Object>>)result.get("rows");
        Assert.assertEquals(expectedRowsWithDocs, rows);

        server.close();
    }

    public void testViews() {

        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        send(server, "PUT", "/db", TDStatus.CREATED, null);

        Map<String,Object> result;
        Map<String,Object> doc1 = new HashMap<String,Object>();
        doc1.put("message", "hello");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc1", doc1, TDStatus.CREATED, null);
        String revID = (String)result.get("rev");
        Map<String,Object> doc3 = new HashMap<String,Object>();
        doc3.put("message", "bonjour");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc3", doc3, TDStatus.CREATED, null);
        String revID3 = (String)result.get("rev");
        Map<String,Object> doc2 = new HashMap<String,Object>();
        doc2.put("message", "guten tag");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc2", doc2, TDStatus.CREATED, null);
        String revID2 = (String)result.get("rev");

        TDDatabase db = server.getDatabaseNamed("db");
        TDView view = db.getViewNamed("design/view");
        view.setMapReduceBlocks(new TDViewMapBlock() {

            @Override
            public void map(Map<String, Object> document, TDViewMapEmitBlock emitter) {
                emitter.emit(document.get("message"), null);
            }
        }, null, "1");

        // Build up our expected result

        Map<String,Object> row1 = new HashMap<String,Object>();
        row1.put("id", "doc1");
        row1.put("key", "hello");
        Map<String,Object> row2 = new HashMap<String,Object>();
        row2.put("id", "doc2");
        row2.put("key", "guten tag");
        Map<String,Object> row3 = new HashMap<String,Object>();
        row3.put("id", "doc3");
        row3.put("key", "bonjour");

        List<Map<String,Object>> expectedRows = new ArrayList<Map<String,Object>>();
        expectedRows.add(row3);
        expectedRows.add(row2);
        expectedRows.add(row1);

        Map<String,Object> expectedResult = new HashMap<String,Object>();
        expectedResult.put("offset", 0);
        expectedResult.put("total_rows", 3);
        expectedResult.put("rows", expectedRows);

        // Query the view and check the result:
        send(server, "GET", "/db/_design/design/_view/view", TDStatus.OK, expectedResult);

        // Check the ETag:
        TDURLConnection conn = sendRequest(server, "GET", "/db/_design/design/_view/view", null, null);
        String etag = conn.getHeaderField("Etag");
        Assert.assertEquals(String.format("\"%d\"", view.getLastSequenceIndexed()), etag);

        // Try a conditional GET:
        Map<String,String> headers = new HashMap<String,String>();
        headers.put("If-None-Match", etag);
        conn = sendRequest(server, "GET", "/db/_design/design/_view/view", headers, null);
        Assert.assertEquals(TDStatus.NOT_MODIFIED, conn.getResponseCode());

        // Update the database:
        Map<String,Object> doc4 = new HashMap<String,Object>();
        doc4.put("message", "aloha");
        result = (Map<String,Object>)sendBody(server, "PUT", "/db/doc4", doc4, TDStatus.CREATED, null);

        // Try a conditional GET:
        conn = sendRequest(server, "GET", "/db/_design/design/_view/view", headers, null);
        Assert.assertEquals(TDStatus.OK, conn.getResponseCode());
        result = (Map<String,Object>)parseJSONResponse(conn);
        Assert.assertEquals(4, result.get("total_rows"));
        
        server.close();
    }

    public void testPostBulkDocs() {

        TDServer server = null;
        try {
            server = new TDServer(getServerPath());
        } catch (IOException e) {
            fail("Creating server caused IOException");
        }

        send(server, "PUT", "/db", TDStatus.CREATED, null);

        Map<String,Object> bulk_doc1 = new HashMap<String,Object>();
        bulk_doc1.put("_id", "bulk_message1");
        bulk_doc1.put("baz", "hello");

        Map<String,Object> bulk_doc2 = new HashMap<String,Object>();
        bulk_doc2.put("_id", "bulk_message2");
        bulk_doc2.put("baz", "hi");

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        list.add(bulk_doc1);
        list.add(bulk_doc2);

        Map<String,Object> bodyObj = new HashMap<String,Object>();
        bodyObj.put("docs", list);

        List<Map<String,Object>> bulk_result  =
                (ArrayList<Map<String,Object>>)sendBody(server, "POST", "/db/_bulk_docs", bodyObj, TDStatus.CREATED, null);

        Assert.assertEquals(2, bulk_result.size());
        Assert.assertEquals(bulk_result.get(0).get("id"),  bulk_doc1.get("_id"));
        Assert.assertNotNull(bulk_result.get(0).get("rev"));
        Assert.assertEquals(bulk_result.get(1).get("id"),  bulk_doc2.get("_id"));
        Assert.assertNotNull(bulk_result.get(1).get("rev"));

        server.close();

    }
    
    public void testPostKeysView() {

    	TDServer server = null;
    	try {
    		server = new TDServer(getServerPath());
    	} catch (IOException e) {
    		fail("Creating server caused IOException");
    	}

    	send(server, "PUT", "/db", TDStatus.CREATED, null);

    	Map<String,Object> result;

    	TDDatabase db = server.getDatabaseNamed("db");
    	TDView view = db.getViewNamed("design/view");
    	view.setMapReduceBlocks(new TDViewMapBlock() {

    		@Override
    		public void map(Map<String, Object> document, TDViewMapEmitBlock emitter) {
    			emitter.emit(document.get("message"), null);
    		}
    	}, null, "1");

    	Map<String,Object> key_doc1 = new HashMap<String,Object>();
    	key_doc1.put("parentId", "12345");
    	result = (Map<String,Object>)sendBody(server, "PUT", "/db/key_doc1", key_doc1, TDStatus.CREATED, null);
    	view = db.getViewNamed("design/view");
    	view.setMapReduceBlocks(new TDViewMapBlock() {
    		@Override
    		public void map(Map<String, Object> document, TDViewMapEmitBlock emitter) {
    			if (document.get("parentId").equals("12345")) {
    				emitter.emit(document.get("parentId"), document);
    			}
    		}
    	}, null, "1");

    	List<Object> keys = new ArrayList<Object>();
    	keys.add("12345");
    	Map<String,Object> bodyObj = new HashMap<String,Object>();
    	bodyObj.put("keys", keys);
    	TDURLConnection conn = Router.sendRequest(server, "POST", "/db/_design/design/_view/view", null, bodyObj);
    	result = (Map<String, Object>) Router.parseJSONResponse(conn);
    	Assert.assertEquals(1, result.get("total_rows"));
    	server.close();
    }

}
