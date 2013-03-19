/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.touchdb;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores information about a revision -- its docID, revID, and whether it's deleted.
 *
 * It can also store the sequence number and document contents (they can be added after creation).
 */
public class TDRevision {

    private String docId;
    public String path;
    private String revId;
    private boolean deleted;
    private TDBody body;
    private long sequence;

    public TDRevision(String docId, String revId, boolean deleted) {
        this.docId = docId;
        this.revId = revId;
        this.deleted = deleted;
    }

    public TDRevision(TDBody body) {
        this((String)body.getPropertyForKey("_id"),
                (String)body.getPropertyForKey("_rev"),
                (((Boolean)body.getPropertyForKey("_deleted") != null)
                        && ((Boolean)body.getPropertyForKey("_deleted") == true)));
        this.body = body;
    }

    public TDRevision(Map<String,Object> properties) {
        this(new TDBody(properties));
    }

    public Map<String,Object> getProperties() {
        Map<String,Object> result = null;
        if(body != null) {
            result = body.getProperties();
        }
        return result;
    }

    public void setProperties(Map<String,Object> properties) {
        this.body = new TDBody(properties);
    }

    public byte[] getJson() {
        byte[] result = null;
        if(body != null) {
            result = body.getJson();
        }
        return result;
    }

    public void setJson(byte[] json) {
        this.body = new TDBody(json);
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if(o instanceof TDRevision) {
            TDRevision other = (TDRevision)o;
            if(docId.equals(other.docId) && revId.equals(other.revId)) {
                result = true;
            }
        }
        return result;
    }

    @Override
    public int hashCode() {
        return docId.hashCode() ^ revId.hashCode();
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getRevId() {
        return revId;
    }

    public void setRevId(String revId) {
        this.revId = revId;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public TDBody getBody() {
        return body;
    }

    public void setBody(TDBody body) {
        this.body = body;
    }

    public TDRevision copyWithDocID(String docId, String revId) {
        assert((docId != null) && (revId != null));
        assert((this.docId == null) || (this.docId.equals(docId)));
        TDRevision result = new TDRevision(docId, revId, deleted);
        Map<String, Object> properties = getProperties();
        if(properties == null) {
            properties = new HashMap<String, Object>();
        }
        properties.put("_id", docId);
        properties.put("_rev", revId);
        result.setProperties(properties);
        return result;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "{" + this.docId + " #" + this.revId + (deleted ? "DEL" : "") + "}";
    }

    /**
     * Generation number: 1 for a new document, 2 for the 2nd revision, ...
     * Extracted from the numeric prefix of the revID.
     */
    public int getGeneration() {
        return generationFromRevID(revId);
    }

    public static int generationFromRevID(String revID) {
        int generation = 0;
        int dashPos = revID.indexOf("-");
        if(dashPos > 0) {
            generation = Integer.parseInt(revID.substring(0, dashPos));
        }
        return generation;
    }

}
