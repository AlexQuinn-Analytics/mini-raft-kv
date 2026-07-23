package com.miniraftkv.node;

import java.io.IOException;
import java.io.FileWriter;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Arrays;
import java.util.Random;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;


import com.miniraftkv.log.LogEntry;
import com.miniraftkv.rpc.AppendEntriesResponse;
import com.miniraftkv.rpc.RaftClient;
import com.miniraftkv.rpc.RaftServiceImpl;
import com.miniraftkv.rpc.RequestVoteResponse;
import com.miniraftkv.state.NodeState;
import com.miniraftkv.storage.LSMStore;

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
    private LSMStore lsmStore = new LSMStore(100);

    
    public Node(String nodeId, int port, List<String> peers) {
        this.nodeId = nodeId;
        this.port = port;
        this.currentTerm = 0;
        this.peers = peers;
        this.state = NodeState.FOLLOWER;
        this.votedFor = null;
        this.scheduler = Executors.newScheduledThreadPool(4);
        loadState();
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
        int timeout = 3000 + random.nextInt(3000);
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
        if (state == NodeState.CANDIDATE) {
        resetElectionTimer();
        }

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
            lsmStore.put(kv[0], kv[1]);
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

    public void loadState() {
        try {
            File file = new File("node_" + nodeId + ".txt");
            if (!file.exists()) {
                return;
            }
            BufferedReader reader = new BufferedReader(new FileReader(file));
            currentTerm = Long.parseLong(reader.readLine());
            String votedForLine = reader.readLine();
            votedFor = votedForLine.equals("null")? null : votedForLine;
            String line;
            int index = 1;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("," , 2);
                long term = Long.parseLong(parts[0]);
                String command = parts[1];
                log.add(new LogEntry(term, index, command));
                index++;
            }
            reader.close();
            System.out.println("[" + nodeId + "] Recovered: term=" + currentTerm + ", votedFor=" + votedFor + ", log size =" + log.size());
        }catch (Exception e) {
            System.out.println("[" + nodeId + "] Load failed: " + e.getMessage());
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

    public boolean handleClientCommand(String command) {
        if (state != NodeState.LEADER) {
            return false;
        }
        appendCommand(command);
        return true;
    }

    public String getValue(String key) {
        return lsmStore.get(key);
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
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

    // 2. Wait for election
    System.out.println(">>> STEP 1: waiting for election...");
    Thread.sleep(10000);

    // 3. Find the leader and send commands to it
    System.out.println(">>> STEP 2: finding leader...");
    Node[] nodes = {node1, node2, node3};
    Node leader = null;
    for (Node n : nodes) {
        if (n.handleClientCommand("set x=5")) {
            leader = n;
            System.out.println(">>> STEP 3: leader accepted!");
            break;
        }
    }

    System.out.println(">>> STEP 4: after leader loop");
    if (leader != null) {
        leader.handleClientCommand("set y=10");
    }

    // 4. Wait for commit + apply
    System.out.println(">>> STEP 5: sleeping 5s...");
    Thread.sleep(5000);

    // 5. Verify: read from LSM
    System.out.println(">>> STEP 6: reading from LSM");
    System.out.println("Node1: x = " + node1.getValue("x") + ", y = " + node1.getValue("y"));
    System.out.println("Node2: x = " + node2.getValue("x") + ", y = " + node2.getValue("y"));
    System.out.println("Node3: x = " + node3.getValue("x") + ", y = " + node3.getValue("y"));

    System.out.println(">>> STEP 7: done");
    System.exit(0);
    }
}
