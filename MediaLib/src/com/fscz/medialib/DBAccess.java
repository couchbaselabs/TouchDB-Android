package com.fscz.medialib;

import com.couchbase.touchdb.TDDatabase;

public interface DBAccess {
	public TDDatabase getDatabase(String name, boolean create);
}
