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

import java.io.File;
import java.net.URL;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.UpHandler;
import org.jgroups.blocks.mux.MuxUpHandler;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.w3c.dom.Element;

public class MuxChannel extends JChannel {
	public MuxChannel() throws ChannelException {
		super();
		init();
	}

	public MuxChannel(Element properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(File properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(JChannel ch) throws ChannelException {
		super(ch);
		init();
	}

	public MuxChannel(ProtocolStackConfigurator configurator) throws ChannelException {
		super(configurator);
		init();
	}

	public MuxChannel(String properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(URL properties) throws ChannelException {
		super(properties);
		init();
	}

	private void init() {
		super.setUpHandler(new MuxUpHandler());
	}

	@Override
	public void setUpHandler(UpHandler upHandler) {
		if(upHandler instanceof MuxUpHandler) {
			super.setUpHandler(upHandler);
		} else {
			super.setUpHandler(new MuxUpHandler(upHandler));
		}
	}
}
