/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 *
 * Author: Dominik Wisniewski (dw418484@students.mimuw.edu.pl)
 */

package cp1.solution;

import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class Transaction {
    private long timeOfCreation; /// < variable storing information when transaction was created
    private Map<ResourceId, Stack<ResourceOperation>> resourceToExecutedOperations; /// < map storing operations performed on assigned resources
    private ArrayList<ResourceId> usedResourcesId; /// < arraylist storing ids of resources which are assigned to this transaction
    public Transaction(long timeOfCreation) {
        this.timeOfCreation = timeOfCreation;
        this.resourceToExecutedOperations = new HashMap<>();
        this.usedResourcesId = new ArrayList<>();
    }

    public void addExecutedOperation(ResourceId rid, ResourceOperation executedOperation) {
        this.resourceToExecutedOperations.get(rid).add(executedOperation);
    }

    public long getTime() {
        return this.timeOfCreation;
    }

    public void insertResource(ResourceId rid) {
        this.usedResourcesId.add(rid);
        this.resourceToExecutedOperations.put(rid, new Stack<>());
    }

    public boolean accessedResource(Resource resource) {
        return this.resourceToExecutedOperations.containsKey(resource);
    }

    public ArrayList<ResourceId> getUsedResourcesId() {
        return new ArrayList<>(usedResourcesId);
    }

    public Map<ResourceId, Stack<ResourceOperation>> operationsToRollback() {
        return new HashMap<>(this.resourceToExecutedOperations);
    }
}
