package net.dataforte.infinispan.amanuensis;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.dataforte.commons.slf4j.LoggerFactory;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;

/**
 * This <code>ReaderProvider</code> shares IndexReaders as long as they are
 * "current"; main difference with SharedReaderProvider is the way to update the
 * Readers when needed: this uses IndexReader.reopen() which should improve
 * performance on larger indexes as it shares buffers with previous IndexReader
 * generation for the segments which didn't change.
 * 
 * Taken mostly from Hibernate Search's
 * org.hibernate.search.reader.SharingBufferReaderProvider
 * 
 * @author Sanne Grinovero
 */
public class AmanuensisIndexReader {
	private static final Logger log = LoggerFactory.make();
	protected final Map<IndexReader, ReaderUsagePair> allReaders = new ConcurrentHashMap<IndexReader, ReaderUsagePair>();
	protected PerDirectoryLatestReader currentReader;

	AmanuensisManager amanuensisManager;

	public AmanuensisIndexReader(AmanuensisManager amanuensisManager, Directory directory) throws IndexerException {
		try {
			this.amanuensisManager = amanuensisManager;
			this.currentReader = new PerDirectoryLatestReader(directory);
		} catch (Exception e) {
			throw new IndexerException("Could not create IndexReader", e);
		}
	}

	public IndexReader get() throws IndexerException {
		return currentReader.refreshAndGet();
	}

	public void release(IndexReader r) {
		if (r != null) {
			allReaders.get(r).close();
		}
	}

	public void close() {
		currentReader.current.close();
	}

	// overridable method for testability:
	protected IndexReader readerFactory(final Directory directory) throws IOException {
		return IndexReader.open(directory, true);
	}

	/**
	 * Container for the couple IndexReader,UsageCounter.
	 */
	protected final class ReaderUsagePair {

		public final IndexReader reader;
		/**
		 * When reaching 0 (always test on change) the reader should be really
		 * closed and then discarded. Starts at 2 because: first usage token is
		 * artificial: means "current" is not to be closed (+1) additionally
		 * when creating it will be used (+1)
		 */
		protected final AtomicInteger usageCounter = new AtomicInteger(2);

		ReaderUsagePair(IndexReader r) {
			reader = r;
		}

		/**
		 * Closes the <code>IndexReader</code> if no other resource is using it
		 * in which case the reference to this container will also be removed.
		 */
		public void close() {
			int refCount = usageCounter.decrementAndGet();
			if (refCount == 0) {
				// TODO I've been experimenting with the idea of an async-close:
				// didn't appear to have an interesting benefit,
				// so discarded the code. should try with bigger indexes to see
				// if the effect gets more impressive.
				ReaderUsagePair removed = allReaders.remove(reader);// remove
																	// ourself
				try {
					reader.close();
				} catch (IOException e) {
					log.warn("Unable to close Lucene IndexReader", e);
				}
				assert removed != null;
			} else if (refCount < 0) {
				// doesn't happen with current code, could help spotting future
				// bugs?
				throw new IllegalStateException("Closing an IndexReader for which you didn't own a lock-token, or somebody else which didn't own closed already.");
			}
		}

		public String toString() {
			return "Reader:" + this.hashCode() + " ref.count=" + usageCounter.get();
		}

	}

	/**
	 * An instance for each DirectoryProvider, establishing the association
	 * between "current" ReaderUsagePair for a DirectoryProvider and it's lock.
	 */
	protected final class PerDirectoryLatestReader {

		/**
		 * Reference to the most current IndexReader for a DirectoryProvider;
		 * guarded by lockOnReplaceCurrent;
		 */
		public ReaderUsagePair current; // guarded by lockOnReplaceCurrent
		private final Lock lockOnReplaceCurrent = new ReentrantLock();

		/**
		 * @param directory
		 *            The <code>Directory</code> for which we manage the
		 *            <code>IndexReader</code>.
		 * 
		 * @throws IOException
		 *             when the index initialization fails.
		 */
		public PerDirectoryLatestReader(Directory directory) throws IOException {
			IndexReader reader = readerFactory(directory);
			ReaderUsagePair initialPair = new ReaderUsagePair(reader);
			initialPair.usageCounter.set(1);// a token to mark as active
											// (preventing real close).
			lockOnReplaceCurrent.lock();// no harm, just ensuring safe
										// publishing.
			current = initialPair;
			lockOnReplaceCurrent.unlock();
			allReaders.put(reader, initialPair);
		}

		/**
		 * Gets an updated IndexReader for the current Directory; the index
		 * status will be checked.
		 * 
		 * @return the current IndexReader if it's in sync with underlying
		 *         index, a new one otherwise.
		 * @throws IndexerException
		 */
		public IndexReader refreshAndGet() throws IndexerException {
			ReaderUsagePair previousCurrent;
			IndexReader updatedReader;
			lockOnReplaceCurrent.lock();
			try {
				IndexReader beforeUpdateReader = current.reader;
				try {
					updatedReader = beforeUpdateReader.reopen();
				} catch (IOException e) {
					throw new IndexerException("Unable to reopen IndexReader", e);
				}
				if (beforeUpdateReader == updatedReader) {
					previousCurrent = null;
					current.usageCounter.incrementAndGet();
				} else {
					ReaderUsagePair newPair = new ReaderUsagePair(updatedReader);
					// no need to increment usageCounter in newPair, as it is
					// constructed with correct number 2.
					assert newPair.usageCounter.get() == 2;
					previousCurrent = current;
					current = newPair;
					allReaders.put(updatedReader, newPair);// unfortunately
															// still needs lock
				}
			} finally {
				lockOnReplaceCurrent.unlock();
			}
			// doesn't need lock:
			if (previousCurrent != null) {
				previousCurrent.close();// release a token as it's not the
										// current any more.
			}
			return updatedReader;
		}
	}

}
