package com.miniraftkv.node;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Random;
import java.util.List;
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
    private List<LogEntry> log = new ArrayList<>();
    private int commitIndex = 0;
    private Map<String, String> kvStore = new HashMap<>();
    private int lastApplied = 0;

    
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
        .addService(new RaftServiceImpl(nodeId, this))
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

    public void becomeLeader(){
        state = NodeState.LEADER;
        System.out.println("[" + nodeId +"] 🎉 Became LEADER for term" + currentTerm + "!");
        if (electionTimer != null){
            electionTimer.cancel(false);
        }
        startHeartbeat();
    }

    public void startElection(){
        state = NodeState.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        voteCount = 1;
        persist();
        System.out.println("[" + nodeId + "] Became CANDIDATE, term =" + currentTerm + ", requesting votes from peers...");
        for (String peer:peers){
            requestVoteFromPeer(peer);
        }
        resetElectionTimer();

    }

    public void startHeartbeat(){
        scheduler.scheduleAtFixedRate(
            ()->sendHeartbeats(),
            0,
            100,
            TimeUnit.MILLISECONDS
        );
    }

    public void sendHeartbeats(){
        if (state != NodeState.LEADER){
            return;
        }
        int successCount = 1;

        for (String peer:peers){
            boolean success = sendHeartbeatToPeer(peer);
            if (success){
                successCount++;
            }
        }
        if (successCount > (peers.size()+1)/2){
            if (commitIndex < log.size()){
                commitIndex = log.size();
                System.out.println("[" + nodeId + "] Committed up to index" + commitIndex);
            }
        }
    }

    public boolean sendHeartbeatToPeer(String peer){
        try{
            String[] parts = peer.split(":");
            String host = parts[0];
            int peerPort = Integer.parseInt(parts[1]);
            RaftClient client = new RaftClient(host, peerPort);
            AppendEntriesResponse response = client.sendAppendEntries(nodeId, currentTerm, log, prevLogIndex, prevLogTerm);
            client.shutdown();
            return response.getSuccess();
        }catch(Exception e){
            return false;
        }
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

    public void onHeartbeat(long leaderTerm){
        if (leaderTerm >= currentTerm){
            state = NodeState.FOLLOWER;
            currentTerm = leaderTerm;
            resetElectionTimer();
        }
    }

    public void appendCommand(String command){
        if (state != NodeState.LEADER){
            System.out.println("[" + nodeId + "] Not leader, cannot append command");
            return;
        }
        int newIndex = log.size() + 1;
        LogEntry entry = new LogEntry(currentTerm, newIndex, command);
        log.add(entry);
        persist();
        System.out.println("[" + nodeId + "] Appended:" + entry);
    }

    public void handleAppendEntries(List<com.miniraftkv.rpc.LogEntry> protoEntries){
    if (protoEntries.isEmpty()) return;
    for (int i = 0; i < protoEntries.size(); i++){
        int index = i + 1; 
        com.miniraftkv.rpc.LogEntry protoEntry = protoEntries.get(i);   // 取当前日志
        if (index <= log.size()) {
            LogEntry existing = log.get(index - 1);
            if (existing.getTerm() != protoEntry.getTerm()){
                while (log.size() >= index) {
                    log.remove(log.size() - 1);
                }
                LogEntry entry = new LogEntry(protoEntry.getTerm(), index, protoEntry.getCommand());
                log.add(entry);
                System.out.println("[" + nodeId + "] Conflict at index " + index + ", replaced");
            }    
        } else {
            LogEntry entry = new LogEntry(protoEntry.getTerm(), index, protoEntry.getCommand());
            log.add(entry);
            System.out.println("[" + nodeId + "] Replicated:" + entry);
        }
    }
    }

    public void applyLog(){
        while (lastApplied < commitIndex){
            lastApplied++;
            LogEntry entry = log.get(lastApplied - 1);
            applyToStateMachine(entry.getCommand(
            ));
            System.out.println("[" + nodeId + "] Applied" + entry.getCommand());
        }
    }

    public void applyToStateMachine(String command){
        String[] parts = command.split(" ");
        if (parts[0].equals("set")){
            String[] kv = parts[1].split("=");
            kvStore.put(kv[0], kv[1]);
        }
    }

    public void blockUntilShutdown() throws InterruptedException{
        if (server!=null){
            server.awaitTermination();
        }
    }

    public void persist(){
        try {
            FileWriter writer = new FileWriter("node_" + nodeId + ".txt");
            writer.write(currentTerm + "\n");
            writer.write((votedFor == null? "null" : votedFor) + "\n");
            for (LogEntry entry : log){
                writer.write(entry.getTerm() + "," + entry.getCommand() + "\n");
            }
            writer.close();
        } catch (Exception e){
            System.out.println("[" + nodeId + "] Persist failed: " + e.getMessage());
        }
    }

    public boolean handleRequestVote(long candidateTerm, String candidateId) {
        if (candidateTerm < currentTerm) {
            return false;
        }

        if (candidateTerm > currentTerm) {
            state = NodeState.FOLLOWER;
            currentTerm = candidateTerm;
            votedFor = null;
        }
        
        if (votedFor == null || votedFor.equals(candidateId)){
            votedFor = candidateId;
            persist();
            resetElectionTimer();
            return true;
        }

        return false;
    }

    public long getCurrentTerm(){
        return currentTerm;
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
