package com.miniraftkv.node;
import com.miniraftkv.state.NodeState;

public class Node{
    private String nodeId;
    private long currentTerm;
    private NodeState state;
    
    Node(String nodeId){
        this.nodeId=nodeId;
        this.currentTerm=0;
        this.state=NodeState.FOLLOWER;
    }
    
    public void printStatus(){
        System.out.println("Node:" + nodeId + ", Term:" + currentTerm + ", State:" + state);
    }
    
    public static void main(String[]args){
        Node node1=new Node("Node1");
        Node node2=new Node("Node2");
        Node node3=new Node("Node3");
        node1.printStatus();
        node2.printStatus();
        node3.printStatus();
    }
}
