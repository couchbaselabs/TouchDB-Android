package com.fscz.medialib;

import java.lang.reflect.Method;
import java.util.EnumSet;

import com.couchbase.touchdb.TDDatabase;
import com.couchbase.touchdb.TDRevision;
import com.couchbase.touchdb.TDStatus;
import com.couchbase.touchdb.TDDatabase.TDContentOptions;

import android.annotation.SuppressLint;
import android.app.ProgressDialog;
import android.content.Context;
import android.content.Intent;
import android.view.MotionEvent;
import android.view.View;
import android.webkit.JavascriptInterface;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.webkit.WebSettings.PluginState;

public class MediaWebView extends WebView {
	
	private DBAccess access;
	private String mHomeLocation;
	private String mLastLocation;
	ProgressDialog mProgressDialog;
	public static final String INTENT_EXTRA_VIDEO = "video_path";
	
	public class MediaInterface {
		private String mError;
		
		@JavascriptInterface
		public void home() {
			MediaWebView.this.loadHome();
		}
		
		@JavascriptInterface
		public void back() {
			MediaWebView.this.loadUrl(mLastLocation);
		}
		
		@JavascriptInterface
		public String getError() {
			return mError;
		}
		
		@JavascriptInterface
		public int invoke(String methodName, Object... args) {
			try {
				Method m = MediaWebView.this.getClass().getMethod(methodName);
				m.invoke(MediaInterface.this, args);
			} catch(Exception e) {
				mError = e.getMessage();
				return 1;
			}
			return 0;
		}
	}
	
	public MediaWebView(Context context, String homeLocation, DBAccess access) {
		this(context, homeLocation);
		this.access = access;
	}

	@SuppressLint("SetJavaScriptEnabled")
	public MediaWebView(final Context context, String homeLocation) {
		super(context);
		mProgressDialog = new ProgressDialog(context);
		mHomeLocation = homeLocation;
		this.addJavascriptInterface(new MediaInterface(), "MG");
        this.setWebChromeClient(new WebChromeClient());
        this.getSettings().setJavaScriptEnabled(true);
		this.getSettings().setPluginState(PluginState.ON);
		this.getSettings().setCacheMode(WebSettings.LOAD_DEFAULT);
		this.getSettings().setDomStorageEnabled(true);
		this.setWebViewClient(new WebViewClient() {
			
        	@Override
        	public boolean shouldOverrideUrlLoading (WebView view, String url) {
        		if (url.endsWith("mp4")) {
        			String path = getAttachmentPath(url);
        			if (path == null) return true;
        			Intent intent = new Intent(context, VideoActivity.class);
        			intent.putExtra(INTENT_EXTRA_VIDEO, path);
        			context.startActivity(intent);
        		} else {
	        		view.setVisibility(View.GONE);
	                mProgressDialog.setTitle("Loading");
	                mProgressDialog.show();
	                mProgressDialog.setMessage("Loading " + url);
        		}
        		return true;
        	}

            @Override
            public void onPageFinished(WebView view, String url) {
                mProgressDialog.dismiss();
                view.setVisibility(View.VISIBLE);
                super.onPageFinished(view, url);
            }
        });
		this.setScrollBarStyle(View.SCROLLBARS_INSIDE_OVERLAY);
		
		this.requestFocus(View.FOCUS_DOWN);
	    this.setOnTouchListener(new View.OnTouchListener() {
	        @Override
	        public boolean onTouch(View v, MotionEvent event) {
	            switch (event.getAction()) {
	                case MotionEvent.ACTION_DOWN:
	                case MotionEvent.ACTION_UP:
	                    if (!v.hasFocus()) {
	                        v.requestFocus();
	                    }
	                    break;
	            }
	            return false;
	        }
	    });
	}
	
	@Override
	public void loadUrl(String url) {
		super.loadUrl(url);
		mLastLocation = url;
	}
	
	public void loadHome() {
		super.loadUrl(mHomeLocation);
	}
	
	public void setDBAccess(DBAccess access) {
		this.access = access;
	}
	
	public void js(String js) {
		this.loadUrl("javascript:"+js);
	}
	
	private String getAttachmentPath(String url) {
    	String dbName = url.split("/")[3];
    	String docID = url.split("/")[4]+'/'+url.split("/")[5];
    	String _attachmentName = url.split(docID+"/")[1];
    	
        EnumSet<TDContentOptions> options = EnumSet.noneOf(TDContentOptions.class);
        options.add(TDContentOptions.TDNoBody);
        TDDatabase db = access.getDatabase(dbName, false);
        if (!db.open()) {
        	return null;
        }
    	TDRevision rev = db.getDocumentWithIDAndRev(docID, null, options);
    	
    	TDStatus status = new TDStatus();
    	String path = db.getAttachmentAsPathForDocument(rev.getDocId(), _attachmentName, status);
    	db.close();
    	return path;
    }
}
