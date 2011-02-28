/**
 * Amanuensis, a distributed Lucene Index Writer for Infinispan
 *
 * Copyright (c) 2011, Tristan Tarrant
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

package net.dataforte.infinispan.amanuensis.backend.jgroups;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;
import net.dataforte.infinispan.amanuensis.OperationDispatcher;

import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.mux.MuxMessageDispatcher;
import org.slf4j.Logger;

/**
 * This class takes care of dispatching {@link IndexOperations} messages from
 * the slaves to the master.
 * 
 * @author Tristan Tarrant
 */
public class JGroupsOperationDispatcher implements OperationDispatcher {
	private static final Logger log = LoggerFactory.make();
	private AmanuensisManager manager;
	private MuxMessageDispatcher messageDispatcher;
	private RequestOptions requestOptions;
	private int maxRetries = 10;
	private int minTimeout = 10000;

	public JGroupsOperationDispatcher(AmanuensisManager manager, MuxMessageDispatcher messageDispatcher) {
		this.manager = manager;
		this.messageDispatcher = messageDispatcher;
		this.requestOptions = new RequestOptions(Request.GET_ALL, 10000); // We want synchronous, and we can wait for 10 seconds
	}

	public RequestOptions getRequestOptions() {
		return requestOptions;
	}

	public void setRequestOptions(RequestOptions requestOptions) {
		this.requestOptions = requestOptions;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(int retries) {
		this.maxRetries = retries;
	}

	public int getMinTimeout() {
		return minTimeout;
	}

	public void setMinTimeout(int minTimeout) {
		this.minTimeout = minTimeout;
	}

	@Override
	public void dispatch(final IndexOperations ops) throws IndexerException {
		final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		
		executor.scheduleWithFixedDelay(new Runnable() {
			int retryCount = 0;

			@Override
			public void run() {
				Address dest = ((JGroupsAddress) manager.getMasterAddress()).getJGroupsAddress();
				Address src = ((JGroupsAddress) manager.getLocalAddress()).getJGroupsAddress();
				if(dest.equals(src)) {
					try {
						manager.dispatchOperations(ops);
						executor.shutdown();
						return;
					} catch (IndexerException e) {
						// Fall through
					}					
				}
				Message message = new Message(dest, src, ops);
				if (log.isTraceEnabled()) {
					log.trace("Sending {} to {}", ops.toString(), dest.toString());
				}
				try {
					messageDispatcher.sendMessage(message, requestOptions);
					// No exception was raised, stop the executor
					executor.shutdown();
				} catch (Exception e) {
					++retryCount;
					if(log.isDebugEnabled()) {
						log.debug("Error while sending {} to {}", ops.toString(), dest.toString());
					}
					if(retryCount<maxRetries) {
						log.warn("Sending operations to {} failed, try #{}", dest.toString(), retryCount);
					} else {
						log.error("Could not send operations to "+dest.toString()+" after "+maxRetries + "tries, giving up", e);
					}
				}
				
			}}, 0, minTimeout, TimeUnit.MILLISECONDS);
	}
}
