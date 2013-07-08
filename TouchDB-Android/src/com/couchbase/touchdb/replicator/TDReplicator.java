package com.couchbase.touchdb.replicator;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.impl.client.DefaultHttpClient;

import android.util.Log;

import com.couchbase.touchdb.TDDatabase;
import com.couchbase.touchdb.TDMisc;
import com.couchbase.touchdb.TDRevision;
import com.couchbase.touchdb.TDRevisionList;
import com.couchbase.touchdb.support.HttpClientFactory;
import com.couchbase.touchdb.support.TDBatchProcessor;
import com.couchbase.touchdb.support.TDBatcher;
import com.couchbase.touchdb.support.TDRemoteRequest;
import com.couchbase.touchdb.support.TDRemoteRequestCompletionBlock;

public abstract class TDReplicator extends Observable {

	private static int lastSessionID = 0;

	protected ScheduledExecutorService workExecutor;
	protected TDDatabase db;
	protected URL remote;
	protected boolean continuous;
	protected String lastSequence;
	protected boolean lastSequenceChanged;
	protected Map<String, Object> remoteCheckpoint;
	protected boolean savingCheckpoint;
	protected boolean overdueForSave;
	protected boolean running;
	protected boolean active;
	protected Throwable error;
	protected String sessionID;
	protected TDBatcher<TDRevision> batcher;
	protected int asyncTaskCount;
	private int changesProcessed;
	private int changesTotal;
	protected final HttpClientFactory clientFactory;
	protected String filterName;
	protected Map<String, Object> filterParams;

	protected static final int PROCESSOR_DELAY = 500;
	protected static final int INBOX_CAPACITY = 100;

	protected String access_token = null;
	// protected AtomicBoolean pending_changes_running = new
	// AtomicBoolean(false);
	protected AtomicBoolean refiller_scheduled = new AtomicBoolean(false);

	private ExecutorService remoteRequestExecutor;

	public TDReplicator(TDDatabase db, URL remote, String access_token,
			boolean continuous, ScheduledExecutorService workExecutor) {
		this(db, remote, access_token, continuous, null, workExecutor);
	}

	public TDReplicator(TDDatabase db, URL remote, String access_token,
			boolean continuous, HttpClientFactory clientFacotry,
			ScheduledExecutorService workExecutor) {

		this.db = db;
		this.remote = remote;
		this.access_token = access_token;
		this.continuous = continuous;
		this.workExecutor = workExecutor;

		this.remoteRequestExecutor = Executors.newCachedThreadPool();

		batcher = new TDBatcher<TDRevision>(workExecutor, INBOX_CAPACITY,
				PROCESSOR_DELAY, new TDBatchProcessor<TDRevision>() {

					@Override
					public void process(List<TDRevision> inbox) {
						Log.v(TDDatabase.TAG, "*** " + toString()
								+ ": BEGIN processInbox (" + inbox.size()
								+ " sequences)");
						processInbox(new TDRevisionList(inbox));
						Log.v(TDDatabase.TAG, "*** " + toString()
								+ ": END processInbox (lastSequence="
								+ lastSequence);
						active = false;
					}
				});

		this.clientFactory = clientFacotry != null ? clientFacotry
				: new HttpClientFactory() {
					@Override
					public HttpClient getHttpClient() {
						return new DefaultHttpClient();
					}
				};
	}

	public void setFilterName(String filterName) {
		this.filterName = filterName;
	}

	public void setFilterParams(Map<String, Object> filterParams) {
		this.filterParams = filterParams;
	}

	public boolean isRunning() {
		return running;
	}

	public URL getRemote() {
		return remote;
	}

	public void databaseClosing() {
		saveLastSequence();
		stop();
		db = null;
	}

	public String toString() {
		String maskedRemoteWithoutCredentials = (remote != null ? remote
				.toExternalForm() : "");
		maskedRemoteWithoutCredentials = maskedRemoteWithoutCredentials
				.replaceAll("://.*:.*@", "://---:---@");
		String name = getClass().getSimpleName() + "["
				+ maskedRemoteWithoutCredentials + "]";
		return name;
	}

	public boolean isPush() {
		return false;
	}

	public String getLastSequence() {
		return lastSequence;
	}

	public void setLastSequence(String lastSequenceIn) {
		if (!lastSequenceIn.equals(lastSequence)) {
			Log.v(TDDatabase.TAG, toString() + ": Setting lastSequence to "
					+ lastSequenceIn + " from( " + lastSequence + ")");
			lastSequence = lastSequenceIn;
			if (!lastSequenceChanged) {
				lastSequenceChanged = true;
				workExecutor.schedule(new Runnable() {

					@Override
					public void run() {
						saveLastSequence();
					}
				}, 2 * 1000, TimeUnit.MILLISECONDS);
			}
		}
	}

	public int getChangesProcessed() {
		return changesProcessed;
	}

	public void setChangesProcessed(int processed) {
		this.changesProcessed = processed;
		setChanged();
		notifyObservers();
	}

	public int getChangesTotal() {
		return changesTotal;
	}

	public void setChangesTotal(int total) {
		this.changesTotal = total;
		setChanged();
		notifyObservers();
	}

	public String getSessionID() {
		return sessionID;
	}

	public void start() {
		if (running) {
			return;
		}
		this.sessionID = String.format("repl%03d", ++lastSessionID);
		Log.v(TDDatabase.TAG, toString() + " STARTING ...");
		running = true;
		lastSequence = null;

		fetchRemoteCheckpointDoc();
	}

	public void beginReplicating() {
		// This is useful for the first run after the replicator starts
		Log.d(getLogTag(), "Called by ChangeTracker");
		scheduleRefiller();
	}

	public void stop() {
		if (!running) {
			return;
		}
		Log.v(TDDatabase.TAG, toString() + " STOPPING...");
		batcher.flush();
		continuous = false;
		if (asyncTaskCount == 0) {
			stopped();
		}

		// All the revisions that have a timestamp for this replicator are reset
		// to 0. So that they get picked up in the next run.
		resetRevisions();
	}

	public void stopped() {
		Log.v(TDDatabase.TAG, toString() + " STOPPED");
		running = false;
		this.changesProcessed = this.changesTotal = 0;

		saveLastSequence();

		if (db != null) {
			db.getActiveReplicators().remove(this);
		}

		batcher = null;
		db = null;
	}

	public synchronized void asyncTaskStarted() {
		++asyncTaskCount;
	}

	public synchronized void asyncTaskFinished(int numTasks) {
		this.asyncTaskCount -= numTasks;
		if (asyncTaskCount == 0) {
			if (!continuous) {
				stopped();
			}
		}
	}

	public void addToInbox(TDRevision rev) {
		if (batcher.count() == 0) {
			active = true;
		}
		batcher.queueObject(rev);
		// Log.v(TDDatabase.TAG, String.format("%s: Received #%d %s",
		// toString(), rev.getSequence(), rev.toString()));
	}

	public void processInbox(TDRevisionList inbox) {

	}

	public void sendAsyncRequest(String method, String relativePath,
			Object body, TDRemoteRequestCompletionBlock onCompletion) {
		// Log.v(TDDatabase.TAG, String.format("%s: %s .%s", toString(), method,
		// relativePath));
		String urlStr = remote.toExternalForm() + relativePath;
		try {
			URL url = new URL(urlStr);
			TDRemoteRequest request = new TDRemoteRequest(workExecutor,
					clientFactory, method, url, body, onCompletion);
			remoteRequestExecutor.execute(request);
		} catch (MalformedURLException e) {
			Log.e(TDDatabase.TAG, "Malformed URL for async request", e);
		}
	}

	public boolean logRevisions(ArrayList<TDRevision> revs) {
		this.db.beginTransaction();
		boolean success = true;
		for (int i = 0; i < revs.size() && success; i++) {
			success = success && logRevision(revs.get(i));
		}
		this.db.endTransaction(success);
		return success;
	}

	public boolean logRevision(TDRevision rev) {
		return this.db.logRevision(this.remote, isPush(), rev);
	}

	public TDRevisionList getPendingRevisions(long lastUpdated) {
		return this.db.getPendingRevisions(this.remote, isPush(), lastUpdated);
	}

	public void updateLogRevision(TDRevision rev, long lastUpdated) {
		this.db.updateLogRevision(getRemote(), isPush(), rev, lastUpdated);
	}

	public void removeLogForRevision(TDRevision rev) {
		this.db.removeLogForRevision(this.remote, isPush(), rev);
	}

	public void resetRevisions() {
		if (this.db != null) {
			this.db.resetRevisions(this.remote, isPush());
		}
	}

	/** CHECKPOINT STORAGE: **/

	public void maybeCreateRemoteDB() {
		// TDPusher overrides this to implement the .createTarget option
	}

	/**
	 * This is the _local document ID stored on the remote server to keep track
	 * of state. Its ID is based on the local database ID (the private one, to
	 * make the result unguessable) and the remote database's URL.
	 */
	public String remoteCheckpointDocID() {
		if (db == null) {
			return null;
		}
		String input = db.privateUUID() + "\n" + remote.toExternalForm() + "\n"
				+ (isPush() ? "1" : "0");
		return TDMisc.TDHexSHA1Digest(input.getBytes());
	}

	public void fetchRemoteCheckpointDoc() {
		lastSequenceChanged = false;
		final String localLastSequence = db.lastSequenceWithRemoteURL(remote,
				isPush());
		if (localLastSequence == null) {
			maybeCreateRemoteDB();
			beginReplicating();
			return;
		}

		asyncTaskStarted();
		sendAsyncRequest("GET", "/_local/" + remoteCheckpointDocID(), null,
				new TDRemoteRequestCompletionBlock() {

					@Override
					public void onCompletion(Object result, Throwable e) {
						if (e != null
								&& e instanceof HttpResponseException
								&& ((HttpResponseException) e).getStatusCode() != 404) {
							error = e;
						} else {
							if (e instanceof HttpResponseException
									&& ((HttpResponseException) e)
											.getStatusCode() == 404) {
								maybeCreateRemoteDB();
							}
							Map<String, Object> response = (Map<String, Object>) result;
							remoteCheckpoint = response;
							String remoteLastSequence = null;
							if (response != null) {
								remoteLastSequence = (String) response
										.get("lastSequence");
							}
							if (remoteLastSequence != null
									&& remoteLastSequence
											.equals(localLastSequence)) {
								lastSequence = localLastSequence;
								Log.v(TDDatabase.TAG, this
										+ ": Replicating from lastSequence="
										+ lastSequence);
							} else {
								Log.v(TDDatabase.TAG, this
										+ ": lastSequence mismatch: I had "
										+ localLastSequence + ", remote had "
										+ remoteLastSequence);
							}
							beginReplicating();
						}
						asyncTaskFinished(1);
					}

				});
	}

	public void saveLastSequence() {
		if (!lastSequenceChanged) {
			return;
		}
		if (savingCheckpoint) {
			// If a save is already in progress, don't do anything. (The
			// completion block will trigger
			// another save after the first one finishes.)
			overdueForSave = true;
			return;
		}

		lastSequenceChanged = false;
		overdueForSave = false;

		Log.v(TDDatabase.TAG, this + " checkpointing sequence=" + lastSequence);
		final Map<String, Object> body = new HashMap<String, Object>();
		if (remoteCheckpoint != null) {
			body.putAll(remoteCheckpoint);
		}
		body.put("lastSequence", lastSequence);

		String remoteCheckpointDocID = remoteCheckpointDocID();
		if (remoteCheckpointDocID == null) {
			return;
		}
		savingCheckpoint = true;
		sendAsyncRequest("PUT", "/_local/" + remoteCheckpointDocID, body,
				new TDRemoteRequestCompletionBlock() {

					@Override
					public void onCompletion(Object result, Throwable e) {
						savingCheckpoint = false;
						if (e != null) {
							Log.v(TDDatabase.TAG, this
									+ ": Unable to save remote checkpoint", e);
							// TODO: If error is 401 or 403, and this is a pull,
							// remember that remote is read-only and don't
							// attempt to read its checkpoint next time.
						} else {
							Map<String, Object> response = (Map<String, Object>) result;
							body.put("_rev", response.get("rev"));
							remoteCheckpoint = body;
						}
						if (overdueForSave) {
							saveLastSequence();
						}
					}

				});
		db.setLastSequence(lastSequence, remote, isPush());
	}

	protected class Refill implements Runnable {

		private long lastUpdated;

		public Refill() {
			this(-1);
		}

		public Refill(long lastUpdated) {
			this.lastUpdated = lastUpdated;
		}

		@Override
		public void run() {
			Log.d(getLogTag(), isPush() ? "PUSH" : "PULL");
			TDRevisionList revisions = getPendingRevisions(lastUpdated);
			if (revisions.size() > 0) {
				for (TDRevision rev : revisions) {
					batcher.queueObject(rev);
				}
				Log.d(getLogTag(), "Revs count: " + revisions.size()
						+ ", should I have set the flag?");
			} else {
				// The first time we start replication, we will have zero
				// changes. We will need to kick start replication when
				// changeTracker receives changes
				synchronized (refiller_scheduled) {
					refiller_scheduled.set(false);
					Log.d(getLogTag(), "I set scheduled to false");
				}
			}
		}
	}

	protected void scheduleRefiller() {
		scheduleRefiller(-1);
	}

	protected void scheduleRefiller(long lastUpdated) {
		synchronized (refiller_scheduled) {
			if (!refiller_scheduled.get()) {
				Log.d(getLogTag(), "started with --" + lastUpdated);
				refiller_scheduled.set(true);
				workExecutor.submit(new Refill(lastUpdated));
				Log.d(getLogTag(), "started with --" + lastUpdated);
			} else {
				Log.d(getLogTag(), "Didn't start");
			}
		}
	}

	protected String getLogTag() {
		return "ARTOOREFILLER" + (isPush() ? "PUSH" : "PULL");
	}
}
