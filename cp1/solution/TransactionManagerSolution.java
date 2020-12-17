/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 *
 * Author: Dominik Wisniewski (dw418484@students.mimuw.edu.pl)
 */

package cp1.solution;

import cp1.base.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class TransactionManagerSolution implements TransactionManager {
    LocalTimeProvider localTimeProvider; /// < time provider given to the Manager
    Collection<Resource> resources; /// < resources given to the Manager

    Map<Thread, Transaction> threadToCurrentTransaction; /// < map of thread to its current transaction
    Map<ResourceId, Resource> resourceIdToResource; /// < map of resource id to its resource
    Map<ResourceId, Thread> resourceIdToThread; /// < map of resource to thread which is using it in its transaction
    Map<Thread, Thread> threadAdjecencyList; /// < map that represents dependence of threads
                                            /// used when checking the existance of cycles
                                           /// when trying to apply operations to new Resources
    Map<ResourceId, ArrayList<Thread>> threadsWaitingForResource; /// < map represents  arraylist of
                                                                 /// threads which transactions wait for a resource
                                                                /// with given id
    Map<Thread, ResourceId> awaitedResource; /// < map represents for which resource are given threads transaction waiting
    Set<Thread> isThreadsTransactionAborted; /// < set of threads which transactions are aborted
    Map<ResourceId, Semaphore> resourceIdToSemaphore; /// < map of semaphores on which threads with transactions that
                                                     /// want to operate on given resource wait
    Map<Thread, Semaphore> threadToItsSemaphore; /// < map of threads to its semaphore
    Map<ResourceId, Queue<Thread>> resourceIdToWaitingThreads; /// < map of threads waiting to get access to resource given by ResourceId
    Semaphore protection; /// < binary semaphore providing protection during creating and rollbacking/commitng transactions
    Semaphore sequentialReleasing; /// < binary semaphore used to sequentially waking up resources when a transaction is
                                  /// rollbacked or commited


    /**
     * Construction for TransactionManagerSolution, which initializes
     * needed maps and sets.
     * @param resources - Collection of resources managed my Manager,
     * @param localTimeProvider - Object which provides initialization times for
     *                          transactions.
     */
    public TransactionManagerSolution(Collection<Resource> resources,
                                      LocalTimeProvider localTimeProvider) {
        this.resources = resources;
        this.localTimeProvider = localTimeProvider;
        this.threadToCurrentTransaction = new ConcurrentHashMap<>();
        this.resourceIdToResource = new ConcurrentHashMap<>();
        this.resourceIdToThread = new ConcurrentHashMap<>();
        this.awaitedResource = new ConcurrentHashMap<>();
        this.threadAdjecencyList = new ConcurrentHashMap<>();
        this.resourceIdToSemaphore = new ConcurrentHashMap<>();
        this.protection = new Semaphore(1, true);
        this.sequentialReleasing = new Semaphore(0, true);
        this.threadsWaitingForResource = new ConcurrentHashMap<>();
        this.isThreadsTransactionAborted = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.threadToItsSemaphore = new ConcurrentHashMap<>();
        this.resourceIdToWaitingThreads = new ConcurrentHashMap<>();
        for(Resource resource : resources) {
            ResourceId rid = resource.getId();
            this.resourceIdToResource.put(rid, resource);
            this.resourceIdToSemaphore.put(rid, new Semaphore(0, true));
            this.threadsWaitingForResource.put(rid, new ArrayList<>());
            this.resourceIdToWaitingThreads.put(rid, new LinkedList<>());
        }
    }

    /**
     * @return Current time.
     */
    private synchronized long getTime() {
        return this.localTimeProvider.getTime();
    }

    /**
     * Function checks if resource is currently used in transaction.
     * @param resource - resource to check,
     * @return information if resource is used in non-finised transaction.
     */
    private synchronized boolean isResourceInActiveTransaction(Resource resource) {
        ResourceId rid = resource.getId();
        return this.resourceIdToThread.containsKey(rid);
    }

    /**
     * Function checks if there exists a resource with
     * given id.
     * @param rid - id of resource to check.
     * @return boolean representing information.
     */
    private synchronized boolean exsistsResourceWithId(ResourceId rid) {
        return this.resourceIdToResource.containsKey(rid);
    }

    /**
     * Function that saves operation which was executed on given resource
     * in transaction that executed the operation.
     * @param rid - id of resource that operation was executed on,
     * @param executedOperation - operation that was executed on resource.
     */
    private synchronized void AddExecutedOperation(ResourceId rid, ResourceOperation executedOperation) {
        Thread thisThread = Thread.currentThread();
        Transaction currentTransaction = this.threadToCurrentTransaction.get(thisThread);
        currentTransaction.addExecutedOperation(rid, executedOperation);
    }

    /**
     * Function which assigns resource to transaction that uses it currently.
     * @param rid - resource id that is being used.
     */
    private synchronized void markResourceToThisTransaction(ResourceId rid) {
        Thread thisThread = Thread.currentThread();
        Transaction transaction = this.threadToCurrentTransaction.get(thisThread);
        transaction.insertResource(rid);
        this.resourceIdToThread.put(rid, thisThread);
    }

    /**
     * Function checks if thread has an active transaction.
     * @return boolean message.
     */
    @Override
    public synchronized boolean isTransactionActive() {
        Thread currentThread = Thread.currentThread();
        return this.threadToCurrentTransaction.containsKey(currentThread);
    }

    /**
     * Function checks if thread has an aborted transaction.
     * @return boolean message.
     */
    @Override
    public synchronized boolean isTransactionAborted() {
        Thread thisThread = Thread.currentThread();
        return isThreadsTransactionAborted.contains(thisThread);
    }

    /**
     * Function which creates new transaction for thread.
     * @throws AnotherTransactionActiveException - thrown if thread has a transaction
     * which is neither rollbacked nor committed.
     */
    @Override
    public synchronized void startTransaction() throws AnotherTransactionActiveException {
        if(this.isTransactionActive()) {
            throw new AnotherTransactionActiveException();
        }
        else {
            Thread thisThread = Thread.currentThread();
            Semaphore createdSemaphore = new Semaphore(0, true);
            this.threadToItsSemaphore.put(thisThread, createdSemaphore);
            Transaction newTransaction = new Transaction(getTime());
            threadToCurrentTransaction.put(thisThread, newTransaction);
        }
    }

    /**
     * Function which solves deadlock issues when a cycle is detected.
     * It finds thread which transaction should be aborted, markes it as interrupted,
     * and decrements counter of a resource for which this thread was waiting.
     * @param path - DFS path during which cycle was detected,
     * @param cycleBegin - Vertex (thread) from which cycle starts.
     */
    private void solveDeadlock(Stack<Thread> path, Thread cycleBegin) {
        ArrayList<Thread> cycle = new ArrayList<>();
        while(!path.empty()) {
            Thread topThread = path.pop();
            cycle.add(topThread);
            if(topThread == cycleBegin) break;
        }
        long youngestTransactionBirthTime = -1;
        Thread youngestThread = null;
        long idOfThread = 0;
        for(Thread t : cycle) {
            Transaction threadsTransaction = this.threadToCurrentTransaction.get(t);
            long thisTransactionBirthTime = threadsTransaction.getTime();
            if(youngestThread == null || thisTransactionBirthTime > youngestTransactionBirthTime
                || (thisTransactionBirthTime == youngestTransactionBirthTime && t.getId() > idOfThread)) {
                youngestThread = t;
                youngestTransactionBirthTime = thisTransactionBirthTime;
                idOfThread = t.getId();
            }
        }
        if(youngestThread != null) {
            youngestThread.interrupt();
            this.isThreadsTransactionAborted.add(youngestThread);
            ResourceId awaitedRid = this.awaitedResource.get(youngestThread);
            this.threadsWaitingForResource.get(awaitedRid).remove(youngestThread);
            this.resourceIdToWaitingThreads.get(awaitedRid).remove(youngestThread);
            Semaphore threadsSemaphore = this.threadToItsSemaphore.get(youngestThread);
            if(threadsSemaphore != null) threadsSemaphore.release();
        }
    }

    /**
     * Function which detects cycles in adjecencylist given by
     * threadAdjecencyList
     * @param startingThread - thread from which we start DFS in search of a cycle
     */
    private void dealWithDeadlock(Thread startingThread) {
        Set<Thread> visited = new HashSet<>();
        Stack<Thread> path = new Stack<>();
        path.push(startingThread);
        visited.add(startingThread);
        /* DFS */
        while(!path.empty()) {
            Thread currentThread = path.peek();
            Thread nextThread = threadAdjecencyList.get(currentThread);
            if(nextThread == null) {
                return;
            }
            else if(visited.contains(nextThread)) {
                solveDeadlock(path, nextThread);
                return;
            }
            else {
                path.add(nextThread);
                visited.add(nextThread);
            }
        }
    }

    /**
     * Function which makes thread wait for a resource
     * @param rid - id of resource that is awaited
     * @throws InterruptedException - thrown if interrupted thread wants to acquire semaphore.
     */
    private void waitForResourceId(ResourceId rid) throws InterruptedException {
        Thread thisThread = Thread.currentThread();
        this.resourceIdToWaitingThreads.get(rid).add(thisThread);
        this.protection.release();
        this.threadToItsSemaphore.get(thisThread).acquireUninterruptibly();
    }

    /**
     * Function that applies operation in the name of transaction.
     * @param rid - id of resource on which operation is being applied,
     * @param operation - operation that is applied.
     * @throws ResourceOperationException - thrown if operation caused an exception.
     */
    void applyOperation(ResourceId rid, ResourceOperation operation) throws ResourceOperationException {
        Resource resource = this.resourceIdToResource.get(rid);
        resource.apply(operation);
        AddExecutedOperation(rid, operation);
    }

    /**
     * Function which tries to perfrom operation on cetrain resource.
     * Either performs this operation and assigns the resource to transaction or
     * makes a transaction wait till the resource is free to use.
     * @param rid - id of resource on which transaction wants to perform operation,
     * @param operation - operation which transaction wants to perform.
     * @throws NoActiveTransactionException - thrown if thread has no active transaction,
     * @throws UnknownResourceIdException - thrown if id of resource is unknown,
     * @throws ActiveTransactionAborted - thrown if threads transaction is aborted,
     * @throws ResourceOperationException - thrown by resource operation.
     * @throws InterruptedException - thrown if interrupted thread wants to acquire semaphore.
     */
    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {
        Thread thisThread = Thread.currentThread();
        if(thisThread.isInterrupted()) {
            throw new InterruptedException();
        }
        else if(!this.isTransactionActive()) {
            throw new NoActiveTransactionException();
        }
        else if(!this.exsistsResourceWithId(rid)) {
            throw new UnknownResourceIdException(rid);
        }
        else if(this.isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }
        else {
            this.protection.acquireUninterruptibly();
            Resource resourceOfInterest = this.resourceIdToResource.get(rid);
            Transaction currentTransaction = this.threadToCurrentTransaction.get(thisThread);
            if(currentTransaction.accessedResource(resourceOfInterest)) {
                applyOperation(rid, operation);
                this.protection.release();
            }
            else if(!this.isResourceInActiveTransaction(resourceOfInterest)) {
                markResourceToThisTransaction(rid);
                applyOperation(rid, operation);
                this.protection.release();
            }
            else {
                Thread awaitedThread = this.resourceIdToThread.get(rid);
                this.threadAdjecencyList.put(thisThread, awaitedThread);
                this.threadsWaitingForResource.get(rid).add(thisThread);
                this.awaitedResource.put(thisThread, rid);
                dealWithDeadlock(thisThread);


                this.waitForResourceId(rid);
                if(thisThread.isInterrupted()) {
                    Thread t = this.resourceIdToWaitingThreads.get(rid).poll();
                    if(t != null) {
                        Semaphore threadsSemaphore = threadToItsSemaphore.get(t);
                        threadsSemaphore.release();
                    }
                    return;
                }
                this.protection.acquireUninterruptibly();

                this.awaitedResource.remove(thisThread);
                ArrayList<Thread> threadsWaitingForThisResource = this.threadsWaitingForResource.get(rid);
                threadsWaitingForThisResource.remove(thisThread);

                for(Thread waitingThread : threadsWaitingForThisResource) {
                    threadAdjecencyList.put(waitingThread, thisThread);
                }

                this.threadAdjecencyList.remove(thisThread);

                markResourceToThisTransaction(rid);
                applyOperation(rid, operation);

                Semaphore threadsSemaphore = this.threadToItsSemaphore.get(thisThread);
                threadsSemaphore.release();
                this.protection.release();
            }
        }
    }

    /**
     * Function which deletes information connected to thread that is kept in this data structure.
     * Used only after rollback or commit.
     */
    private void deleteThreadsTransaction() {
        Thread thisThread = Thread.currentThread();
        this.isThreadsTransactionAborted.remove(thisThread);
        Transaction threadsTransaction = threadToCurrentTransaction.get(thisThread);
        ArrayList<ResourceId> usedResourcesId = threadsTransaction.getUsedResourcesId();
        this.threadToCurrentTransaction.remove(thisThread);
        for(ResourceId id : usedResourcesId) {
            Thread t = this.resourceIdToWaitingThreads.get(id).poll();
            if (t != null) {
                Semaphore threadsSemaphore = this.threadToItsSemaphore.get(t);
                if(threadsSemaphore != null)threadsSemaphore.release();
            }
            else {
                resourceIdToThread.remove(id);
            }
        }
        this.threadToItsSemaphore.remove(thisThread);
        this.threadAdjecencyList.remove(thisThread);
    }

    /**
     * Commits current transaction.
     * @throws NoActiveTransactionException - thrown if thread doesnt have active transaction,
     * @throws ActiveTransactionAborted - thrown if threads transaction is aborted.
     */
    @Override
    public synchronized void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
            if(!this.isTransactionActive()) {
                throw new NoActiveTransactionException();
            }
            else if(this.isTransactionAborted()) {
                throw new ActiveTransactionAborted();
            }
            deleteThreadsTransaction();
    }

    /**
     * Rollbacks thread transaction.
     */
    @Override
    public synchronized void rollbackCurrentTransaction() {
        if(!this.isTransactionActive()) {
            return;
        }
        Thread thisThread = Thread.currentThread();
        Transaction currentTransaction = this.threadToCurrentTransaction.get(thisThread);
        Map<ResourceId, Stack<ResourceOperation>> operationsToRollback = currentTransaction.operationsToRollback();

        for(Map.Entry<ResourceId, Stack<ResourceOperation>> entry : operationsToRollback.entrySet()) {
            Stack<ResourceOperation> operationsToUnapply = entry.getValue();
            ResourceId rid = entry.getKey();
            Resource resourceGettingUnapplied = this.resourceIdToResource.get(rid);
            while(!operationsToUnapply.empty()) {
                ResourceOperation operationToUnapply = operationsToUnapply.pop();
                resourceGettingUnapplied.unapply(operationToUnapply);
            }
        }

        deleteThreadsTransaction();
    }
}
