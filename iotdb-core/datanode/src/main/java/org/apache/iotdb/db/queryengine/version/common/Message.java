package org.apache.iotdb.db.queryengine.version.common;

import org.apache.iotdb.commons.path.IFullPath;

public class Message {
    long thread_id;
    SIGNAL signal;
    long timestamp_st;
    long timestamp_ed;
    IFullPath path;
    boolean virtual_path;

    public Message(long thread_id, SIGNAL signal, long timestamp_st, long timestamp_ed, IFullPath path, boolean virtual_path) {
        this.thread_id = thread_id;
        this.signal = signal;
        this.timestamp_st = timestamp_st;
        this.timestamp_ed = timestamp_ed;
        this.path = path;
        this.virtual_path = virtual_path;
    }
}
