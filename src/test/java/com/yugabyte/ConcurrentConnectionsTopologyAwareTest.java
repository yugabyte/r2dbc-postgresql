package com.yugabyte;

import java.util.HashMap;
import java.util.Map;

public class ConcurrentConnectionsTopologyAwareTest extends ConcurrentConnectionsTest {

    public static void main(String[] args) throws  InterruptedException {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalStateException("YBDB_PATH not defined.");
        }

        int total = numThreads * numConnectionsPerThread;
        Map<String, Integer> expected = new HashMap<>();

        String tkValues = "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2";
        expected.put("127.0.0.1", total/2);
        expected.put("127.0.0.2", total/2);
        expected.put("127.0.0.3", 0);
        controlConnection = "127.0.0.3";
        testConcurrentConnectionCreations( expected, tkValues);
    }
}
