/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2015 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
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

using Netsukuku;
using Netsukuku.ModRpc;

public class FakeAddressManagerSkeleton : Object,
                                  IAddressManagerSkeleton,
                                  IPeersManagerSkeleton
{
	public virtual unowned INeighborhoodManagerSkeleton
	neighborhood_manager_getter()
	{
	    error("FakeAddressManagerSkeleton: this test should not use method neighborhood_manager_getter.");
	}

	public virtual unowned IQspnManagerSkeleton
	qspn_manager_getter()
	{
	    error("FakeAddressManagerSkeleton: this test should not use method qspn_manager_getter.");
	}

	public virtual unowned IPeersManagerSkeleton
	peers_manager_getter()
	{
	    return this;
	}

	public virtual unowned ICoordinatorManagerSkeleton
	coordinator_manager_getter()
	{
	    error("FakeAddressManagerSkeleton: this test should not use method coordinator_manager_getter.");
	}

    public virtual void forward_peer_message
    (Netsukuku.IPeerMessage peer_message, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method forward_peer_message.");
    }

    public virtual Netsukuku.IPeerParticipantSet get_participant_set
    (int lvl, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method get_participant_set.");
    }

    public virtual Netsukuku.IPeersRequest get_request
    (int msg_id, Netsukuku.IPeerTupleNode respondant, zcd.ModRpc.CallerInfo? caller = null)
    throws Netsukuku.PeersUnknownMessageError
    {
        error("FakeAddressManagerSkeleton: you must override method get_request.");
    }

    public virtual void set_failure
    (int msg_id, Netsukuku.IPeerTupleGNode tuple, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method set_failure.");
    }

    public virtual void set_next_destination
    (int msg_id, Netsukuku.IPeerTupleGNode tuple, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method set_next_destination.");
    }

    public virtual void set_non_participant
    (int msg_id, Netsukuku.IPeerTupleGNode tuple, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method set_non_participant.");
    }

    public virtual void set_participant
    (int p_id, Netsukuku.IPeerTupleGNode tuple, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method set_participant.");
    }

    public virtual void set_response
    (int msg_id, Netsukuku.IPeersResponse response, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method set_response.");
    }

    public virtual void set_refuse_message
    (int msg_id, string refuse_message, zcd.ModRpc.CallerInfo? caller = null)
    {
        error("FakeAddressManagerSkeleton: you must override method set_refuse_message.");
    }
}

public class FakeAddressManagerStub : Object,
                                  IAddressManagerStub,
                                  IPeersManagerStub
{
	public virtual unowned INeighborhoodManagerStub
	neighborhood_manager_getter()
	{
	    error("FakeAddressManagerSkeleton: this test should not use method neighborhood_manager_getter.");
	}

	public virtual unowned IQspnManagerStub
	qspn_manager_getter()
	{
	    error("FakeAddressManagerSkeleton: this test should not use method qspn_manager_getter.");
	}

	public virtual unowned IPeersManagerStub
	peers_manager_getter()
	{
	    return this;
	}

	public virtual unowned ICoordinatorManagerStub
	coordinator_manager_getter()
	{
	    error("FakeAddressManagerSkeleton: this test should not use method coordinator_manager_getter.");
	}

    public virtual void forward_peer_message
    (Netsukuku.IPeerMessage peer_message)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method forward_peer_message.");
    }

    public virtual Netsukuku.IPeerParticipantSet get_participant_set
    (int lvl)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method get_participant_set.");
    }

    public virtual Netsukuku.IPeersRequest get_request
    (int msg_id, Netsukuku.IPeerTupleNode respondant)
    throws Netsukuku.PeersUnknownMessageError, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method get_request.");
    }

    public virtual void set_failure
    (int msg_id, Netsukuku.IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method set_failure.");
    }

    public virtual void set_next_destination
    (int msg_id, Netsukuku.IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method set_next_destination.");
    }

    public virtual void set_non_participant
    (int msg_id, Netsukuku.IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method set_non_participant.");
    }

    public virtual void set_participant
    (int p_id, Netsukuku.IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method set_participant.");
    }

    public virtual void set_response
    (int msg_id, Netsukuku.IPeersResponse response)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method set_response.");
    }

    public virtual void set_refuse_message
    (int msg_id, string refuse_message)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("FakeAddressManagerStub: you must override method set_refuse_message.");
    }
}

