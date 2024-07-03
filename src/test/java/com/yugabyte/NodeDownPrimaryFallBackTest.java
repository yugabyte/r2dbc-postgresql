package com.yugabyte;

import java.util.Arrays;

public class NodeDownPrimaryFallBackTest extends FallBackTopologyTestExtended{

    private static final String path = System.getenv("YBDB_PATH");

    public static void main(String args[]) throws InterruptedException {

        int numConnections = 18;

        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);

        executeCmd(path + "/bin/yb-ctl --rf 3 start --placement_info \"aws.us-west.us-west-1a\" ",
                "Start YugabyteDB rf=3 cluster", 15);

        System.out.println("Sleeping");

        Thread.sleep(10000);

        try {
            controlConnection = "127.0.0.3";
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.*:1", Arrays.asList(6, 6, 6));

            executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop node 1", 10);
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.*:1", Arrays.asList(-1, 15, 15));

            executeCmd(path + "/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\"",
                    "Start node 1", 10);
            try {
                Thread.sleep(15000);
            } catch (InterruptedException ie) {
            }
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.*:1", Arrays.asList(16, 16, 16));

        } finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
        }
    }
}
