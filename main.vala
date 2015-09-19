/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2014-2015 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
 *
 *  Netsukuku is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Netsukuku is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Netsukuku.  If not, see <http://www.gnu.org/licenses/>.
 */

using Gee;
using Netsukuku;
using Netsukuku.ModRpc;
using Tasklets;

/* This "holder" class is needed because the PeersManagerRemote class provided by
 * the ZCD framework is owned (and tied to) by the AddressManagerXxxxRootStub.
 */
private class PeersManagerStubHolder : Object, IPeersManagerStub
{
    public PeersManagerStubHolder(IAddressManagerStub addr)
    {
        this.addr = addr;
    }
    private IAddressManagerStub addr;

	public void forward_peer_message
	(IPeerMessage peer_message)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.forward_peer_message(peer_message);
	}

	public IPeerParticipantSet get_participant_set
	(int lvl)
	throws PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    return addr.peers_manager.get_participant_set(lvl);
	}

	public IPeersRequest get_request
	(int msg_id, IPeerTupleNode respondant)
	throws PeersUnknownMessageError, PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    return addr.peers_manager.get_request(msg_id, respondant);
	}

	public void set_failure
	(int msg_id, IPeerTupleGNode tuple)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.set_failure(msg_id, tuple);
	}

	public void set_next_destination
	(int msg_id, IPeerTupleGNode tuple)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.set_next_destination(msg_id, tuple);
	}

	public void set_non_participant
	(int msg_id, IPeerTupleGNode tuple)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.set_non_participant(msg_id, tuple);
	}

	public void set_participant
	(int p_id, IPeerTupleGNode tuple)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.set_participant(p_id, tuple);
	}

	public void set_response
	(int msg_id, IPeersResponse response)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.set_response(msg_id, response);
	}

	public void set_refuse_message
	(int msg_id, string refuse_message)
	throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
	{
	    addr.peers_manager.set_refuse_message(msg_id, refuse_message);
	}
}

class MyMapPaths : Object, IPeersMapPaths
{
    public int i_peers_get_levels()
    {
        error("not implemented yet");
    }

    public int i_peers_get_gsize(int level)
    {
        error("not implemented yet");
    }

    public int i_peers_get_nodes_in_my_group(int level)
    {
        error("not implemented yet");
    }

    public int i_peers_get_my_pos(int level)
    {
        error("not implemented yet");
    }

    public bool i_peers_exists(int level, int pos)
    {
        error("not implemented yet");
    }

    public IPeersManagerStub i_peers_gateway
        (int level, int pos,
         zcd.ModRpc.CallerInfo? received_from=null,
         IPeersManagerStub? failed=null)
        throws PeersNonexistentDestinationError
    {
        error("not implemented yet");
    }

    public IPeersManagerStub i_peers_fellow(int level)
        throws PeersNonexistentFellowError
    {
        error("not implemented yet");
    }
}

class MyBackFactory : Object, IPeersBackStubFactory
{
    public IPeersManagerStub i_peers_get_tcp_inside
        (Gee.List<int> positions)
    {
        error("not implemented yet");
    }
}

class MyNeighborsFactory : Object, IPeersNeighborsFactory
{
    public IPeersManagerStub i_peers_get_broadcast
        (IPeersMissingArcHandler missing_handler)
    {
        error("not implemented yet");
    }

    public IPeersManagerStub i_peers_get_tcp
        (IPeersArc arc)
    {
        error("not implemented yet");
    }
}

zcd.IZcdTasklet zcd_tasklet;
Netsukuku.INtkdTasklet ntkd_tasklet;

int main(string[] args)
{
    // Initialize tasklet system
    MyTaskletSystem.init();
    zcd_tasklet = MyTaskletSystem.get_zcd();
    ntkd_tasklet = MyTaskletSystem.get_ntkd();

    // Initialize known serializable classes
    // ... TODO
    // Pass tasklet system to ModRpc (and ZCD)
    zcd.ModRpc.init_tasklet_system(zcd_tasklet);
    // Pass tasklet system to module peerservices
    PeersManager.init(ntkd_tasklet);

    var m = new MyMapPaths();
    var bf = new MyBackFactory();
    var nf = new MyNeighborsFactory();
    PeersManager peers = new PeersManager(m, 4/*levels*/, bf, nf);
    //peers.register(p);

    MyTaskletSystem.kill();
    print("\nExiting.\n");
    return 0;
}
