package com.yugabyte;

import java.util.Arrays;

public class MultiNodeDownFallbackTopologyTest extends FallbackTopologyTest{
    private static final String path = System.getenv("YBDB_PATH");

    public static void main(String args[]){

        System.out.println("Checking Multi Node Down Behaviour...");

        startYBDBClusterWithSixNodes();

        try{
            controlConnection = "127.0.0.3";
            createConnectionsAndVerify("aws.us-west.us-west-1a:1,aws.us-west.us-west-2a:2,aws.us-west.us-west-2b:3,aws.us-west.us-west-2c:4", Arrays.asList(4, 4, 4, 0, 0, 0));

            // Stop 3 nodes

            executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop node 1", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 2", "Stop node 2", 10);
            executeCmd(path + "/bin/yb-ctl stop_node 3", "Stop node 3", 10);

            controlConnection = "127.0.0.4";

            createConnectionsAndVerify("aws.us-west.us-west-1a:1,aws.us-west.us-west-2a:2,aws.us-west.us-west-2b:3,aws.us-west.us-west-2c:4", Arrays.asList(-1, -1, -1, 12, 0, 0));

            // Stop 1 more node

            executeCmd(path + "/bin/yb-ctl stop_node 4", "Stop node 4", 10);

            createConnectionsAndVerify("aws.us-west.us-west-1a:1,aws.us-west.us-west-2a:2,aws.us-west.us-west-2b:3,aws.us-west.us-west-2c:4", Arrays.asList(-1, -1, -1, -1, 12, 0));

        }finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
            System.out.println("Done");
        }
    }
}
