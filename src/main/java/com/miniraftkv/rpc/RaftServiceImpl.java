package com.miniraftkv.rpc;
import io.grpc.stub.StreamObserver;
public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase{
    private final String nodeId;
    public RaftServiceImpl(String nodeId){
        this.nodeId = nodeId;
    }
    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver){
        System.out.println("[" + nodeId + "] Received RequestVote from " + request.getCandidateId() + ", term=" + request.getTerm());
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
        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
        .setTerm(request.getTerm())
        .setSuccess(true)
        .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
