package com.yugabyte;

import java.util.Arrays;

public class RestOfTheClusterFallBackTest extends FallBackTopologyTestExtended{

    private static final String path = System.getenv("YBDB_PATH");

    public static void main(String args[]) throws InterruptedException {

        int numConnections = 12;

        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);

        executeCmd(path + "/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-2a,aws.us-west.us-west-2b,aws.us-west.us-west-2b\"",
                "Start YugabyteDB rf=3 cluster", 15);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2c\"",
                "Add a node", 10);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2c\"",
                "Add a node", 10);
        executeCmd(path + "/bin/yb-ctl add_node --placement_info \"aws.us-west.us-west-2d\"",
                "Add a node", 10);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {}

        try{
            controlConnection = "127.0.0.3";
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(12, 0, 0, 0, 0, 0));
            executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop node 1", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 2", "Stop node 2", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 3", "Stop node 3", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 4", "Stop node 4", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 5", "Stop node 5", 10);
            Thread.sleep(10000);

            controlConnection = "127.0.0.6";
            createConnectionsWithoutClosingAndVerify(numConnections, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", Arrays.asList(-1, -1, -1, -1, -1, 12));

        }finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
            System.out.println("Done");
        }

    }
}
