package com.miniraftkv.node;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    private String votedFor;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> electionTimer;
    private final Random random = new Random();
    private List<String> peers;
    private int voteCount;

    
    public Node(String nodeId, int port, List<String> peers) {
        this.nodeId = nodeId;
        this.port = port;
        this.currentTerm = 0;
        this.peers = peers;
        this.state = NodeState.FOLLOWER;
        this.votedFor = null;
        this.scheduler = Executors.newScheduledThreadPool(1);
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

    public void resetElectionTimer(){
        if (electionTimer!=null){
            electionTimer.cancel(false);
        }
        int timeout = 800 + random.nextInt(800);
        electionTimer = scheduler.schedule(
            ()->startElection(),
            timeout,
            TimeUnit.MILLISECONDS
        );
    }

    public void startElection(){
        state = NodeState.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        voteCount = 1;
        System.out.println("[" + nodeId + "] Became CANDIDATE, term =" + currentTerm + ", requesting votes from peers...");
        for (String peer:peers){
            requestVoteFromPeer(peer);
        }
        resetElectionTimer();

    }

    public void requestVoteFromPeer(String peer){
        try{
            String[] parts = peer.split(":");
            String host = parts[0];
            int peerPort = Integer.parseInt(parts[1]);
            RaftClient client = new RaftClient(host, peerPort);
            RequestVoteResponse response= client.sendRequestVote(nodeId, currentTerm);
            if (response.getVoteGranted()){
                voteCount++;
                System.out.println("[" + nodeId + "] Got vote from " + peer 
                + ", total votes: " + voteCount);
            }
            client.shutdown();
        }catch(Exception e){
            System.out.println("[" + nodeId + "] Failed to contact " + peer);
        }
    }

    public void blockUntilShutdown() throws InterruptedException{
        if (server!=null){
            server.awaitTermination();
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {

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

    node1.blockUntilShutdown();
    }
}
