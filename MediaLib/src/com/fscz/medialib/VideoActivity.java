package com.fscz.medialib;

import com.fscz.medialib.R;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.widget.MediaController;
import android.widget.VideoView;

public class VideoActivity extends Activity {
	@Override
    public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.video);
		
		Intent intent = getIntent();
	    String path = intent.getStringExtra(MediaWebView.INTENT_EXTRA_VIDEO);
		setContentView(R.layout.video);
        VideoView myVideoView = (VideoView)findViewById(R.id.videoview);
        myVideoView.setVideoURI(Uri.parse(path));
        myVideoView.setMediaController(new MediaController(this));
        myVideoView.requestFocus();
        myVideoView.start();
	}
}
