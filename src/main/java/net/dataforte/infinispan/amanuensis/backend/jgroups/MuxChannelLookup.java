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

import java.net.URL;
import java.util.Properties;

import org.infinispan.CacheException;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.FileLookup;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuxChannelLookup implements JGroupsChannelLookup {	
	private static final Logger log = LoggerFactory.getLogger(MuxChannelLookup.class);
	private static MuxChannel muxedChannel;

	@Override
	public Channel getJGroupsChannel(Properties p) {

		String cfg = p.getProperty(JGroupsTransport.CONFIGURATION_FILE);
		try {
			URL cfgUrl = new FileLookup().lookupFileLocation(cfg);
			JChannel channel = new JChannel(cfgUrl);
			muxedChannel = new MuxChannel(channel);
			log.info("MuxChannel created");
			return muxedChannel;
		} catch (Exception e) {
			log.error("Error while trying to create a channel using config files: " + cfg);
			throw new CacheException(e);
		}
	}

	public static MuxChannel getChannel() {
		return muxedChannel;
	}

	@Override
	public boolean shouldStartAndConnect() {
		return true;
	}

	@Override
	public boolean shouldStopAndDisconnect() {
		return true;
	}	

}
