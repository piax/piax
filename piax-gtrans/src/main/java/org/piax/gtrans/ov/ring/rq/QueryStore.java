/*
 * QueryStore.java - A storage of query result.
 * 
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.PeerId;
import org.piax.gtrans.RemoteValue;

public class QueryStore {
	int storedCount;
	int expiredCount;
	int readCount;
	class QueryStoreEntry {
		public QueryId qid;
		public long timestamp;
		public RemoteValue<?> rval;
		public QueryStoreEntry(QueryId qid, RemoteValue<?> rval) {
			this.qid = qid;
			this.rval = rval;
			this.timestamp = System.currentTimeMillis();
		}
	}
	ConcurrentHashMap<QueryId,QueryStoreEntry> store;
	
	
	/**
	 * 
	 */
	public QueryStore() {
		store = new ConcurrentHashMap<QueryId,QueryStoreEntry>();
		storedCount = 0;
		expiredCount = 0;
		readCount = 0;
	}
	
	/**
	 * @param qid
	 * @return
	 */
	public RemoteValue<?> get(QueryId qid) {
		QueryStoreEntry entry = store.get(qid);
		if (entry != null) readCount++; 
		// XXX should consider timestamp?
		return entry != null ? entry.rval : null;
	}
	
	/**
	 * @param qid
	 * @param rval
	 */
	public void put(QueryId qid, RemoteValue<?> rval) {
		store.put(qid, new QueryStoreEntry(qid, rval));
		storedCount++;
	}
	
	/**
	 * @param expireTime
	 */
	public void removeExpired(long expireTime) {
		final long threshold = System.currentTimeMillis() - expireTime;
		for (Iterator<QueryId> it = store.keySet().iterator(); it.hasNext();) {
			QueryId qid = it.next();
			QueryStoreEntry entry = store.get(qid);
            if (entry.timestamp < threshold) {
                it.remove();
                expiredCount++;
            }
		}
	}
	
	/**
	 * @return
	 */
	public int getStoredCount() {
		return storedCount;
	}
	
	/**
	 * @return
	 */
	public int getExpiredCount() {
		return expiredCount;
	}
	
	/**
	 * @return
	 */
	public int getReadCount() {
		return readCount;
	}
	
	static public void main(String args[]) {
		final long start = System.currentTimeMillis();
		QueryStore store = new QueryStore();
		class Expirer extends Thread {
			QueryStore store;
			public Expirer(QueryStore store) {
				this.store = store;
			}
			public void run() {
				while (System.currentTimeMillis() - start < 2000) {
					store.removeExpired(1000);
				}
			}
		}
		class Writer extends Thread {
			QueryStore store;
			public Writer(QueryStore store) {
				this.store = store;
			}
			public void run() {
				int i = 0;
				int p = 0;
				while (System.currentTimeMillis() - start < 2000) {
					PeerId pid = new PeerId("p" + p++);
					store.put(new QueryId(pid, i++), new RemoteValue(pid, null));
				}
			}
		}
		class Reader extends Thread {
			QueryStore store;
			public Reader(QueryStore store) {
				this.store = store;
			}
			public void run() {
				int i = 0;
				int p = 0;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				while (System.currentTimeMillis() - start < 2000) {
					PeerId pid = new PeerId("p" + p++);
					store.get(new QueryId(pid, i++));
				}
			}
		}
		new Writer(store).start();
		new Reader(store).start();
		new Expirer(store).start();
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Stored:" + store.getStoredCount() + ", Expired:" + store.getExpiredCount() + ", Read: " + store.getReadCount());
	}
}
