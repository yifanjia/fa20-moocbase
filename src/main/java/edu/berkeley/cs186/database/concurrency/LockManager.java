package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we recommend you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum != except && !LockType.compatible(lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            // update the list of locks in this resource entry
            int i = 0;
            int lockIndex = -1;
            Lock toReplaceLock = null;
            for (Lock prevLock : locks) {
                if (prevLock.transactionNum == lock.transactionNum) {
                    lockIndex = i;
                    toReplaceLock = prevLock;
                    break;
                }
                i++;
            }
            if (lockIndex < 0) {
                // transaction not in list before case
                locks.add(lock);
                // add the lock to the transaction's locks
                if (!LockManager.this.transactionLocks.containsKey(lock.transactionNum)) {
                    LockManager.this.transactionLocks.put(lock.transactionNum, new ArrayList<>());
                }
                LockManager.this.transactionLocks.get(lock.transactionNum).add(lock);
            }
            else {
                // transaction in list before case
                locks.set(lockIndex, lock);
                // replace the prev lock of the transaction with the new one
                List<Lock> lockLst = LockManager.this.transactionLocks.get(lock.transactionNum);
                lockLst.set(lockLst.indexOf(toReplaceLock), lock);
                // the queue needs to be processed because the lock has be updated
                processQueue();
            }
            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            boolean lockExist = locks.remove(lock);
            if (lockExist) {
                LockManager.this.transactionLocks.get(lock.transactionNum).remove(lock);
                processQueue();
            }
            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            }
            else {
                waitingQueue.addLast(request);
            }
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                if (checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    requests.remove();
                    // lock the specified lock
                    grantOrUpdateLock(request.lock);
                    request.transaction.unblock();
                    // release the specified locks
                    for (Lock releaseLock : request.releasedLocks) {
                        LockManager.this.resourceEntries.get(releaseLock.name).releaseLock(releaseLock);
                    }
                }
                else {
                    break;
                }
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // helper functions added by me
    private List<Lock> getTransactionLocks(TransactionContext transaction) {
        // return the list of locks that the transaction now hold
        return transactionLocks.getOrDefault(transaction.getTransNum(), new ArrayList<>());
    }

    private boolean isHoldLock(TransactionContext transaction, ResourceName name) {
        // return whether the lock of the given name is held by the transaction
        for (Lock lock : getTransactionLocks(transaction)) {
            if (lock.name.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private Map<ResourceName, List<Lock>> transactionReleaseLocks(TransactionContext transaction, Set<ResourceName> releaseNameSet)
            throws NoLockHeldException {
        // return a map from release name to the list of locks that the transaction should release
        // throw NoLockHeldException if the corresponding resource is not held
        Map<ResourceName, List<Lock>> results = new HashMap<>();
        for (Lock lock : getTransactionLocks(transaction)) {
            ResourceName name = lock.name;
            if (releaseNameSet.contains(name)) {
                // the lock should be released case
                if (!results.containsKey(name)) {
                    results.put(name, new ArrayList<>());
                }
                results.get(name).add(lock);
            }
        }
        if (results.keySet().size() < releaseNameSet.size()) {
            throw new NoLockHeldException("Not holding the resource which is to be released");
        }
        return results;
    }

    private List<Lock> mergeReleaseLocks(Map<ResourceName, List<Lock>> toReleaseLockLsts) {
        // merge all locks (corresponding to different resource names) to be released
        List<Lock> releasedLocks = new ArrayList<>();
        for (List<Lock> lockLst : toReleaseLockLsts.values()) {
            releasedLocks.addAll(lockLst);
        }
        return releasedLocks;
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // record whether the name for locking itself will need to be released
            boolean nameBeRemove = releaseNames.contains(name);
            Set<ResourceName> releaseNameSet = new HashSet<>(releaseNames);
            // check duplicate lock request on the same resource case
            if (isHoldLock(transaction, name) && !nameBeRemove) {
                throw new DuplicateLockRequestException("Duplicate lock request error");
            }
            // get the locks held by this transaction to be released for each resource
            // may throw NoLockHeldException if a resource needs to be realized but is not held
            Map<ResourceName, List<Lock>> toReleaseLockLsts = transactionReleaseLocks(transaction, releaseNameSet);
            // remove locks corresponding to name in the locks to released because it will be handled in grantOrUpdate
            toReleaseLockLsts.remove(name);
            // two exception check has been passed
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            ResourceEntry resourceEntry = getResourceEntry(name);
            // check compatibility
            if (resourceEntry.checkCompatible(lockType, transaction.getTransNum())) {
                // request is compatible case
                // grant or update the lock
                resourceEntry.grantOrUpdateLock(newLock);
                // release all locks corresponding to each resource to release
                for (Map.Entry<ResourceName, List<Lock>> mapEntry : toReleaseLockLsts.entrySet()) {
                    ResourceEntry releaseEntry = getResourceEntry(mapEntry.getKey());
                    for (Lock lock : mapEntry.getValue()) {
                        releaseEntry.releaseLock(lock);
                    }
                }
            }
            else {
                // request is not compatible case
                // block the transaction and put the request at the front of the queue
                shouldBlock = true;
                transaction.prepareBlock();
                // merge all locks to be released
                List<Lock> releasedLocks = mergeReleaseLocks(toReleaseLockLsts);
                // add the lock and release request to queue front
                resourceEntry.addToQueue(new LockRequest(transaction, newLock, releasedLocks), true);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // check duplicate lock request on the same resource case
            if (isHoldLock(transaction, name)) {
                throw new DuplicateLockRequestException("Duplicate lock request error");
            }
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            ResourceEntry resourceEntry = getResourceEntry(name);
            if (resourceEntry.waitingQueue.size() == 0 && resourceEntry.checkCompatible(lockType, -1)) {
                // empty queue and compatible case
                // grant the lock
                resourceEntry.grantOrUpdateLock(newLock);
            }
            else {
                // block the transaction and put the request to the back of the queue
                shouldBlock = true;
                transaction.prepareBlock();
                resourceEntry.addToQueue(new LockRequest(transaction, newLock), false);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            for (Lock lock : resourceEntry.locks) {
                if (lock.transactionNum == transaction.getTransNum()) {
                    resourceEntry.releaseLock(lock);
                    return;
                }
            }
            // reach here only if the lock is not helf
            throw new NoLockHeldException("Not holding the resource which is to be released");
        }
    }
    /*
     * do the check the will invoke the three exception when fail
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    private void checkPromote(TransactionContext transaction, ResourceName name,
          LockType newLockType) throws DuplicateLockRequestException, NoLockHeldException,
            InvalidLockException {
        for (Lock lock : getTransactionLocks(transaction)) {
            if (lock.name.equals(name)) {
                if (lock.lockType == newLockType){
                    // duplicate same type lock case
                    throw new DuplicateLockRequestException("Transaction already has a newLockType lock");
                }
                else if (LockType.substitutable(newLockType, lock.lockType)) {
                    // correct case
                    return;
                }
                else {
                    // not a promotion case
                    throw new InvalidLockException("New lock is not a promotion");
                }
            }
        }
        // not find the lock to replace case
        throw new NoLockHeldException("Transaction has no lock on the name");
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            checkPromote(transaction, name, newLockType);
            for (Lock lock : getTransactionLocks(transaction)) {
                Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
                ResourceEntry resourceEntry = getResourceEntry(name);
                if (resourceEntry.checkCompatible(newLockType, transaction.getTransNum())) {
                    // compatible case
                    resourceEntry.grantOrUpdateLock(newLock);
                }
                else {
                    // not compatible case
                    // block the transaction and put to the front of the queue
                    shouldBlock = true;
                    transaction.prepareBlock();
                    // add the lock and release request to queue front
                    resourceEntry.addToQueue(new LockRequest(transaction, newLock), true);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        return getResourceEntry(name).getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
