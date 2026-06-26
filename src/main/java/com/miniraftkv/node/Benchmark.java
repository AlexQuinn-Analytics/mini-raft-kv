package com.miniraftkv.node;

import java.util.Arrays;
import com.miniraftkv.rpc.RaftClient;
import com.miniraftkv.rpc.ClientResponse;

public class Benchmark {
    public static void main(String[] args)throws Exception {
        // 1. Start 3 nodes
        Node node1 = new Node("Node1", 5001, Arrays.asList("localhost:5002", "localhost:5003"));
        Node node2 = new Node("Node2", 5002, Arrays.asList("localhost:5001", "localhost:5003"));
        Node node3 = new Node("Node3", 5003, Arrays.asList("localhost:5001", "localhost:5002"));
        node1.start();
        node2.start();
        node3.start();
        Thread.sleep(1000);
        node1.resetElectionTimer();
        node2.resetElectionTimer();
        node3.resetElectionTimer();
        // 2. Wait for leader election to stabilize
        System.out.println("Waiting for leader election...");
        Thread.sleep(8000);
        // 3. Find the leader (only the leader accepts client commands)
        String[] ports = {"5001", "5002", "5003"};
        RaftClient leaderClient = null;
        for (String port : ports) {
            RaftClient client = new RaftClient("localhost", Integer.parseInt(port));
            ClientResponse resp = client.sendClientCommand("set test = 1");
            if (resp.getSuccess()) {
                leaderClient = client;
                System.out.println("Leader found on port: " + port);
                break;
            }
            client.shutdown();
        }
        if (leaderClient == null) {
            System.out.println("No leader found!");
            return;
        }
        // 4. Benchmark: send a large number of commands and measure time
        int totalRequests = 1000;
        System.out.println("Starting benchmark: " + totalRequests + " requests...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalRequests; i++) {
            leaderClient.sendClientCommand("set key" + i + "=" + i);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        // 5. Calculate QPS and average latency
        double qps = totalRequests * 1000.0 / duration;
        double avgLatency = (double) duration / totalRequests;

        System.out.println("========== Benchmark Result ==========");
        System.out.println("Total requests: " + totalRequests);
        System.out.println("Total time:     " + duration + " ms");
        System.out.println("QPS:            " + String.format("%.2f", qps));
        System.out.println("Avg latency:    " + String.format("%.2f", avgLatency) + " ms");

        leaderClient.shutdown();
        System.exit(0);
    }
}
