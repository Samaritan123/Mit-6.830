package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        FileInputStream fileInputStream = null;
        byte b[] = new byte[pageSize];
        try {
            fileInputStream = new FileInputStream(f);
            for (int i = 0; i <= pid.getPageNumber(); i ++) {
                fileInputStream.read(b, 0, pageSize);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Page page = null;
        try {
            page = new HeapPage((HeapPageId) pid, b);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] pageData = page.getPageData();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        synchronized (f) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            raf.write(pageData, 0, BufferPool.getPageSize());
            raf.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0; i < numPages(); i ++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            Page page = Database.getBufferPool().getPage(tid, heapPageId,
                    Permissions.READ_WRITE);
            if (((HeapPage)page).getNumEmptySlots() == 0) {
                Database.getBufferPool().releasePage(tid, heapPageId);
                continue;
            }
            ((HeapPage)page).insertTuple(t);
//            ((HeapPage)page).markDirty(true, tid);
            return new ArrayList<Page>(Arrays.asList(page));
        }
        byte[] b = new byte[BufferPool.getPageSize()];
        FileOutputStream fileOutputStream = new FileOutputStream(f, true);
        fileOutputStream.write(b);
        fileOutputStream.close();
        HeapPageId heapPageId = new HeapPageId(getId(), numPages() - 1);
        Page page = Database.getBufferPool().getPage(tid, heapPageId,
                Permissions.READ_WRITE);
        ((HeapPage)page).insertTuple(t);
//        ((HeapPage)page).markDirty(true, tid);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Page page = Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE);
        ((HeapPage)page).deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private boolean open = false;
            private TransactionId transactionId = tid;
            private int pageNum = -1;
            private Iterator<Tuple> iterator = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (open == false) return false;
                if (iterator == null || !iterator.hasNext()) {
                    while (true) {
                        synchronized (f) {
                            if (pageNum >= numPages() - 1) return false;
                            pageNum ++;
                        }
                        PageId pid = new HeapPageId(getId(), pageNum);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        iterator = page.iterator();
                        if (iterator.hasNext()) break;
                    }
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageNum = -1;
                iterator = null;
            }

            @Override
            public void close() {
                pageNum = -1;
                iterator = null;
                open = false;
            }
        };
    }

}

