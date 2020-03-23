package simpledb;

import com.sun.net.ssl.TrustManagerFactorySpi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private LockManager() {}

//    private static LockManager instance;

    public static LockManager getInstance() {
        return new LockManager();
    }

    class Lock {

        private PageId pid;
        private int lockType = 0;// read:-1 write:1 neither:0
        private Set<TransactionId> set;

        public Lock(PageId pid) {
            this.pid = pid;
            this.lockType = 0;
            this.set = new HashSet<>();
        }

        public boolean acquireReadLock(TransactionId tid) {
            int k = 0;
            while (true) {
                k ++;
                synchronized (this) {
                    if (set.contains(tid)) return true;
                    if (lockType != 1) {
                        lockType = -1;
                        set.add(tid);
                        synchronized (tpMap) {
                            tpMap.get(tid).add(pid);
                        }
                        return true;
                    }
                }
                if (k > 10) return false;
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public boolean acquireWriteLock(TransactionId tid) {
            int k = 0;
            while (true) {
                k ++;
                synchronized (this) {
                    if (lockType == 1 && set.contains(tid)) return true;
                    if (lockType < 0 && set.size() == 1 && set.contains(tid)) {
                        lockType = 1;
                        return true;
                    }
                    if (lockType == 0) {
                        lockType = 1;
                        set.add(tid);
                        synchronized (tpMap) {
                            tpMap.get(tid).add(pid);
                        }
                        return true;
                    }
                }
                if (k > 10) return false;
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void releaseLock(TransactionId tid) {
            synchronized (this) {
                set.remove(tid);
                if (set.size() == 0) lockType = 0;
//                synchronized (tpMap) {
//                    tpMap.get(tid).remove(pid);
//                }
            }
        }

        public boolean holdsLock(TransactionId tid) {
            synchronized (this) {
                return set.contains(tid);
            }
        }
    }

    private Map<PageId, Lock> map = new HashMap<>(); //一个Page对应一把Lock
    private Map<TransactionId, List<PageId>> tpMap = new HashMap<>(); //一个Transaction对应多个page

    public boolean acquireReadLock(PageId pid, TransactionId tid) {
        Lock lock = null;
        synchronized (map) {
            if (!map.containsKey(pid)) {
                map.put(pid, new Lock(pid));
            }
            lock = map.get(pid);
        }
        synchronized (tpMap) {
            if (!tpMap.containsKey(tid)) {
                tpMap.put(tid, new ArrayList<PageId>());
            }
        }
        return lock.acquireReadLock(tid);
    }

    public boolean acquireWriteLock(PageId pid, TransactionId tid) {
        Lock lock = null;
        synchronized (map) {
            if (!map.containsKey(pid)) {
                map.put(pid, new Lock(pid));
            }
            lock = map.get(pid);
        }
        synchronized (tpMap) {
            if (!tpMap.containsKey(tid)) {
                tpMap.put(tid, new ArrayList<PageId>());
            }
        }
        return lock.acquireWriteLock(tid);
    }

    public void releasePage(PageId pid, TransactionId tid, boolean tpMapRemove) {
        Lock lock = null;
        synchronized (map) {
            lock = map.get(pid);
        }
        if (tpMapRemove) {
            synchronized (tpMap) {
                tpMap.get(tid).remove(pid);
            }
        }
        lock.releaseLock(tid);
    }

    public void releaseLock(TransactionId tid) {
        List<PageId> pageIds = null;
        synchronized (tpMap) {
            pageIds = tpMap.get(tid);
            if (pageIds != null) {
                for (PageId pageId : pageIds) {
                    releasePage(pageId, tid, false);
                }
                tpMap.get(tid).clear();
            }
        }
    }

    public boolean holdsLock(PageId pid, TransactionId tid) {
        Lock lock = null;
        synchronized (map) {
            lock = map.get(pid);
            if (lock == null) return false;
        }
        return lock.holdsLock(tid);
    }

    public List<PageId> getPages(TransactionId tid) {
        synchronized (tpMap) {
            if (!tpMap.containsKey(tid)) return null;
            return new ArrayList<>(tpMap.get(tid));
        }
    }
}
