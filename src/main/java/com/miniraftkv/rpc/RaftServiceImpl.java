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
        boolean granted = node.handleRequestVote(request.getTerm(), request.getCandidateId());
        RequestVoteResponse response = RequestVoteResponse.newBuilder()
        .setTerm(node.getCurrentTerm())
        .setVoteGranted(granted)
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

    @Override
    public void clientCommand(ClientRequest request, 
        StreamObserver<ClientResponse> responseObserver) {
    boolean success = node.handleClientCommand(request.getCommand());
    
    ClientResponse response = ClientResponse.newBuilder()
        .setSuccess(success)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
    }

}
