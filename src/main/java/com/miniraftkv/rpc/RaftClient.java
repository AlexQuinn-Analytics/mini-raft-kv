package com.miniraftkv.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import com.miniraftkv.log.LogEntry;

public class RaftClient {
    private final ManagedChannel channel;
    private final RaftServiceGrpc.RaftServiceBlockingStub stub;
 /** 
 * @param host 
 * @param port 
 */
    public RaftClient(String host, int port){
        this.channel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build();
        this.stub = RaftServiceGrpc.newBlockingStub(channel);
    }
    public RequestVoteResponse sendRequestVote(String candidateId, long term){
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
        .setTerm(term)
        .setCandidateId(candidateId)
        .setLastLogIndex(0)
        .setLastLogTerm(0)
        .build();
        return stub.requestVote(request);
    }

    public AppendEntriesResponse sendAppendEntries(String leaderId, long term, List<LogEntry>entries){
        AppendEntriesRequest.Builder builder=AppendEntriesRequest.newBuilder()
        .setTerm(term)
        .setLeaderId(leaderId)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setLeaderCommit(0);

        for (LogEntry entry:entries){
            com.miniraftkv.rpc.LogEntry protoEntry = com.miniraftkv.rpc.LogEntry.newBuilder()
            .setTerm(entry.getTerm())
            .setCommand(entry.getCommand())
            .build();
            builder.addEntries(protoEntry);
        }
        return stub.appendEntries(builder.build());
    }
    public void shutdown() {
        channel.shutdown();
    }
}
