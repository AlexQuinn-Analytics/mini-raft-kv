package com.miniraftkv.rpc;
import com.miniraftkv.node.Node;

import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase{
    private final String nodeId;
    private final Node node;
    public RaftServiceImpl(String nodeId, Node node){
        this.nodeId = nodeId;
        this.node = node;
    }
    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver){
        RequestVoteResponse response = RequestVoteResponse.newBuilder()
        .setTerm(request.getTerm())
        .setVoteGranted(true)
        .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }  
    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver){
        System.out.println("[" + nodeId + "] Received AppendEntries from " + request.getLeaderId() + ", term=" + request.getTerm());
        node.onHeartbeat(request.getTerm());
        node.handleAppendEntries(request.getEntriesList());
        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
        .setTerm(request.getTerm())
        .setSuccess(true)
        .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
