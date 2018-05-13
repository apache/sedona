package org.datasyslab.geospark.thirdlibraray.rtree3d.spatial;

/*
 * #%L
 * Conversant RTree
 * ~~
 * Conversantmedia.com © 2016, Conversant, Inc. Conversant® is a trademark of Conversant, Inc.
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

/**
 * Created by jcovert on 12/30/15.
 */
public class ConcurrentRTree<T> implements SpatialSearch<T> {

    private final SpatialSearch<T> rTree;
    private final Lock readLock;
    private final Lock writeLock;

    protected ConcurrentRTree(SpatialSearch<T> rTree, ReadWriteLock lock) {
        this.rTree = rTree;
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    @Override
    public int intersects(HyperRect rect, T[] t) {
        readLock.lock();
        try {
            return rTree.intersects(rect, t);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void intersects(HyperRect rect, Consumer<T> consumer) {
        readLock.lock();
        try {
            rTree.intersects(rect, consumer);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Blocking locked search
     *
     * @param rect - HyperRect to search
     * @param t - array to hold results
     *
     * @return number of entries found
     */
    @Override
    public int search(final HyperRect rect, final T[] t) {
        readLock.lock();
        try {
            return rTree.search(rect, t);
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Blocking locked add
     *
     * @param t - entry to add
     */
    @Override
    public void add(final T t) {
        writeLock.lock();
        try {
            rTree.add(t);
        }
        finally {
            writeLock.unlock();
        }
    }

    /**
     * Blocking locked remove
     *
     * @param t - entry to remove
     */
    @Override
    public void remove(final T t) {
        writeLock.lock();
        try {
            rTree.remove(t);
        }
        finally {
            writeLock.unlock();
        }
    }

    /**
     * Blocking locked update
     *
     * @param told - entry to update
     * @param tnew - entry with new value
     */
    @Override
    public void update(final T told, final T tnew) {
        writeLock.lock();
        try {
            rTree.update(told, tnew);
        }
        finally {
            writeLock.unlock();
        }
    }

    /**
     * Non-blocking locked search
     *
     * @param rect - HyperRect to search
     * @param t - array to hold results
     *
     * @return number of entries found or -1 if lock was not acquired
     */
    public int trySearch(final HyperRect rect, final T[] t) {
        if(readLock.tryLock()) {
            try {
                return rTree.search(rect, t);
            } finally {
                readLock.unlock();
            }
        }
        return -1;
    }

    /**
     * Non-blocking locked add
     *
     * @param t - entry to add
     *
     * @return true if lock was acquired, false otherwise
     */
    public boolean tryAdd(T t) {
        if(writeLock.tryLock()) {
            try {
                rTree.add(t);
            } finally {
                writeLock.unlock();
            }
            return true;
        }
        return false;
    }

    /**
     * Non-blocking locked remove
     *
     * @param t - entry to remove
     *
     * @return true if lock was acquired, false otherwise
     */
    public boolean tryRemove(T t) {
        if(writeLock.tryLock()) {
            try {
                rTree.remove(t);
            } finally {
                writeLock.unlock();
            }
            return true;
        }
        return false;
    }

    /**
     * Non-blocking locked update
     *
     * @param told - entry to update
     * @param tnew - entry with new values
     *
     * @return true if lock was acquired, false otherwise
     */
    public boolean tryUpdate(T told, T tnew) {
        if(writeLock.tryLock()) {
            try {
                rTree.update(told, tnew);
            } finally {
                writeLock.unlock();
            }
            return true;
        }
        return false;
    }

    @Override
    public int getEntryCount() {
        return rTree.getEntryCount();
    }

    @Override
    public void forEach(final Consumer<T> consumer) {
        readLock.lock();
        try {
            rTree.forEach(consumer);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void search(final HyperRect rect, final Consumer<T> consumer) {
        readLock.lock();
        try {
            rTree.search(rect, consumer);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean contains(T t) {
        readLock.lock();
        try {
            return rTree.contains(t);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Stats collectStats() {
        readLock.lock();
        try {
            return rTree.collectStats();
        } finally {
            readLock.unlock();
        }
    }
}
