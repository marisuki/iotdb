package org.apache.iotdb.db.queryengine.version.common;

import org.apache.iotdb.commons.path.IFullPath;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SeriesScanService implements Runnable {
  private static SeriesScanService singleton = new SeriesScanService();
  // read-only fields for external process, to guarantee lock-free for most cases.
  // path is either a device.sensor path or a hashcode assigned by SeriesScanService to cache an
  // intermediate page
  private List<IFullPath> srcPaths;
  public ConcurrentLinkedDeque<Message> msg_queue;
  private List<Deque<SIGNAL>> pathLDStatus; // has the same length as srcPaths, one map-to one

  private SeriesScanService() {}

  public static SeriesScanService getInstance() {
    return singleton;
  }

  @Override
  public void run() {
    while (true) {
      if (!msg_queue.isEmpty()) {
        Message msg = msg_queue.poll();
        switch (msg.signal) {
          case SIG_LD:
        }
      }
    }
  }

  private void handle_load() {}
}
