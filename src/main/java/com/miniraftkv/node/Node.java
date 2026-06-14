package com.miniraftkv.node;

import java.io.IOException;

import com.miniraftkv.rpc.AppendEntriesResponse;
import com.miniraftkv.rpc.RaftClient;
import com.miniraftkv.rpc.RaftServiceImpl;
import com.miniraftkv.rpc.RequestVoteResponse;
import com.miniraftkv.state.NodeState;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class Node {
    private String nodeId;
    private long currentTerm;
    private NodeState state;
    private int port;
    private Server server;

    
    public Node(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
        this.currentTerm = 0;
        this.state = NodeState.FOLLOWER;
    }
    
    public void printStatus() {
        System.out.println("Node:" + nodeId + ", Term:" + currentTerm + ", State:" + state);
    }
    public void start() throws IOException{
        server =  ServerBuilder.forPort(port)
        .addService(new RaftServiceImpl(nodeId))
        .build()
        .start();
        System.out.println("[" + nodeId +"] gRPC Server started, listening on port " + port);
    }
    public void blockUntilShutdown() throws InterruptedException{
        if (server!=null){
            server.awaitTermination();
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
    
    Node node1 = new Node("Node1", 5001);
    node1.printStatus();
    node1.start();

    Thread.sleep(1000);
        
    RaftClient client = new RaftClient("localhost", 5001);

    System.out.println("\n--- Testing RequestVote ---");
    RequestVoteResponse voteResponse = client.sendRequestVote("Node2", 1);
    System.out.println("Got vote response: term=" + voteResponse.getTerm() 
        + ", voteGranted=" + voteResponse.getVoteGranted());

    System.out.println("\n--- Testing AppendEntries ---");
    AppendEntriesResponse appendResponse = client.sendAppendEntries("Node2", 1);
    System.out.println("Got append response: term=" + appendResponse.getTerm() 
        + ", success=" + appendResponse.getSuccess());

    client.shutdown();

    node1.blockUntilShutdown();
    }
}
