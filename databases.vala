/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2014-2017 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
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
using TaskletSystem;
using Netsukuku.PeerServices;

namespace Netsukuku.PeerServices
{
    public interface IPeersContinuation : Object
    {
    }

    public class DatabaseHandler : Object
    {
        internal DatabaseHandler()
        {
            // ...
        }
        internal int p_id;
        internal HashMap<Object,IChannel> retrieving_keys;
        // for TTL-based services
        internal HashMap<Object,Databases.Timer> not_exhaustive_keys;
        internal ArrayList<Object> not_found_keys;
        internal Databases.Timer? timer_default_not_exhaustive;
        // for few-fixed-keys services
        internal ArrayList<Object> not_completed_keys;
    }

    public interface IDatabaseDescriptor : Object
    {
        public abstract bool is_valid_key(Object k);
        public abstract Gee.List<int> evaluate_hash_node(Object k);
        public abstract bool key_equal_data(Object k1, Object k2);
        public abstract uint key_hash_data(Object k);
        public abstract bool is_valid_record(Object k, Object rec);
        public abstract bool my_records_contains(Object k);
        public abstract Object get_record_for_key(Object k);
        public abstract void set_record_for_key(Object k, Object rec);

        public abstract Object get_key_from_request(IPeersRequest r);
        public abstract int get_timeout_exec(IPeersRequest r);
        public abstract bool is_insert_request(IPeersRequest r);
        public abstract bool is_read_only_request(IPeersRequest r);
        public abstract bool is_update_request(IPeersRequest r);
        public abstract bool is_replica_value_request(IPeersRequest r);
        public abstract bool is_replica_delete_request(IPeersRequest r);
        public abstract IPeersResponse prepare_response_not_found(IPeersRequest r);
        public abstract IPeersResponse prepare_response_not_free(IPeersRequest r, Object rec);
        public abstract IPeersResponse execute(IPeersRequest r) throws PeersRefuseExecutionError, PeersRedoFromStartError;

        public DatabaseHandler dh {get {return dh_getter();} set {dh_setter(value);}}
        public abstract unowned DatabaseHandler dh_getter();
        public abstract void dh_setter(DatabaseHandler x);
    }

    public interface ITemporalDatabaseDescriptor : Object, IDatabaseDescriptor
    {
        public int ttl_db_max_records {get {return ttl_db_max_records_getter();}}
        public abstract int ttl_db_max_records_getter();
        public abstract int ttl_db_my_records_size();
        public int ttl_db_max_keys {get {return ttl_db_max_keys_getter();}}
        public abstract int ttl_db_max_keys_getter();
        public int ttl_db_msec_ttl {get {return ttl_db_msec_ttl_getter();}}
        public abstract int ttl_db_msec_ttl_getter();
        public abstract Gee.List<Object> ttl_db_get_all_keys();
        public int ttl_db_timeout_exec_send_keys {get {return ttl_db_timeout_exec_send_keys_getter();}}
        public abstract int ttl_db_timeout_exec_send_keys_getter();
    }

    public interface IFixedKeysDatabaseDescriptor : Object, IDatabaseDescriptor
    {
        public abstract Gee.List<Object> get_full_key_domain();
        public abstract Object get_default_record_for_key(Object k);
    }
}

namespace Netsukuku.PeerServices.Databases
{
    /* ContactPeer: Start message to servant.
     */
    internal delegate IPeersResponse ContactPeer
         (int p_id,
          PeerTupleNode x_macron,
          IPeersRequest request,
          int timeout_exec,
          bool exclude_myself,
          out PeerTupleNode? respondant,
          PeerTupleGNodeContainer? exclude_tuple_list=null)
         throws PeersNoParticipantsInNetworkError, PeersDatabaseError;

    /* AssertServiceRegistered: Assert that a service p_id is registered.
     */
    internal delegate void AssertServiceRegistered(int p_id);

    /* IsServiceOptional: Gets service's optionality.
     */
    internal delegate bool IsServiceOptional(int p_id);

    /* WaitParticipationMaps: Wait until the participation maps are retrieved below target_levels.
     */
    internal delegate void WaitParticipationMaps(int target_levels);

    /* ComputeDist: Compute distance between 2 PeerTupleNode.
     */
    internal delegate int ComputeDist(PeerTupleNode x_macron, PeerTupleNode x);

    /* GetNodesInMyGroup: Determine the number of nodes in my g-node of a given level.
     */
    internal delegate int GetNodesInMyGroup(int lvl);

    internal class Timer : Object
    {
        private TimeVal start;
        private long msec_ttl;
        public Timer(long msec_ttl)
        {
            start = TimeVal();
            start.get_current_time();
            this.msec_ttl = msec_ttl;
        }

        private long get_lap()
        {
            TimeVal lap = TimeVal();
            lap.get_current_time();
            long sec = lap.tv_sec - start.tv_sec;
            long usec = lap.tv_usec - start.tv_usec;
            if (usec < 0)
            {
                usec += 1000000;
                sec--;
            }
            return sec*1000000 + usec;
        }

        public bool is_expired()
        {
            return get_lap() > msec_ttl*1000;
        }
    }

    internal class Databases : Object
    {
        private int levels;
        private ArrayList<int> pos;
        private ArrayList<int> gsizes;
        private ContactPeer contact_peer;
        private AssertServiceRegistered assert_service_registered;
        private IsServiceOptional is_service_optional;
        private WaitParticipationMaps wait_participation_maps;
        private ComputeDist compute_dist;
        private GetNodesInMyGroup get_nodes_in_my_group;

        public Databases
        (Gee.List<int> pos,
         Gee.List<int> gsizes,
         owned ContactPeer contact_peer,
         owned AssertServiceRegistered assert_service_registered,
         owned IsServiceOptional is_service_optional,
         owned WaitParticipationMaps wait_participation_maps,
         owned ComputeDist compute_dist,
         owned GetNodesInMyGroup get_nodes_in_my_group
         )
        {
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            assert(gsizes.size == pos.size);
            this.levels = pos.size;
            this.contact_peer = (owned) contact_peer;
            this.assert_service_registered = (owned) assert_service_registered;
            this.is_service_optional = (owned) is_service_optional;
            this.wait_participation_maps = (owned) wait_participation_maps;
            this.compute_dist = (owned) compute_dist;
            this.get_nodes_in_my_group = (owned) get_nodes_in_my_group;
        }

        /* Algorithms to maintain a robust distributed database */

        private class ReplicaContinuation : Object, IPeersContinuation
        {
            public int q;
            public int p_id;
            public PeerTupleNode x_macron;
            public IPeersRequest r;
            public int timeout_exec;
            public ArrayList<PeerTupleNode> replicas;
            public PeerTupleGNodeContainer exclude_tuple_list;
        }

        public bool begin_replica
                (int q,
                 int p_id,
                 Gee.List<int> perfect_tuple,
                 IPeersRequest r,
                 int timeout_exec,
                 out IPeersResponse? resp,
                 out IPeersContinuation cont)
        {
            ReplicaContinuation _cont = new ReplicaContinuation();
            _cont.q = q;
            _cont.p_id = p_id;
            _cont.x_macron = new PeerTupleNode(perfect_tuple);
            _cont.r = r;
            _cont.timeout_exec = timeout_exec;
            _cont.replicas = new ArrayList<PeerTupleNode>();
            _cont.exclude_tuple_list = new PeerTupleGNodeContainer(_cont.x_macron.tuple.size);
            cont = _cont;
            return next_replica(cont, out resp);
        }

        public bool next_replica(IPeersContinuation cont, out IPeersResponse? resp)
        {
            ReplicaContinuation _cont = (ReplicaContinuation)cont;
            resp = null;
            if (_cont.replicas.size >= _cont.q) return false;
            PeerTupleNode respondant;
            try {
                resp = contact_peer
                    (_cont.p_id, _cont.x_macron, _cont.r,
                     _cont.timeout_exec, true,
                     out respondant, _cont.exclude_tuple_list);
            } catch (PeersNoParticipantsInNetworkError e) {
                return false;
            } catch (PeersDatabaseError e) {
                return false;
            }
            _cont.replicas.add(respondant);
            PeerTupleGNode respondant_as_gnode = new PeerTupleGNode(respondant.tuple, respondant.tuple.size);
            _cont.exclude_tuple_list.add(respondant_as_gnode);
            return _cont.replicas.size < _cont.q;
        }

        internal bool ttl_db_is_exhaustive(ITemporalDatabaseDescriptor tdd, Object k)
        {
            if (tdd.dh.not_exhaustive_keys.has_key(k))
            {
                assert(! (k in tdd.dh.not_found_keys));
                if (tdd.dh.not_exhaustive_keys[k].is_expired())
                    tdd.dh.not_exhaustive_keys.unset(k);
                else return false;
            }
            if (k in tdd.dh.not_found_keys)
            {
                assert(! (tdd.dh.not_exhaustive_keys.has_key(k)));
                return true;
            }
            if (tdd.dh.timer_default_not_exhaustive == null)
                return true;
            if (tdd.dh.timer_default_not_exhaustive.is_expired())
            {
                tdd.dh.timer_default_not_exhaustive = null;
                debug("TemporalDatabase becomes default_exhaustive\n");
                return true;
            }
            return false;
        }

        internal bool ttl_db_is_out_of_memory(ITemporalDatabaseDescriptor tdd)
        {
            if (tdd.ttl_db_my_records_size() + tdd.dh.retrieving_keys.size >= tdd.ttl_db_max_records)
                return true;
            return false;
        }

        internal void ttl_db_add_not_exhaustive(ITemporalDatabaseDescriptor tdd, Object k)
        {
            int max_not_exhaustive_keys = tdd.ttl_db_max_keys / 2;
            assert(! (k in tdd.dh.not_found_keys));
            if (tdd.dh.not_exhaustive_keys.has_key(k))
            {
                tdd.dh.not_exhaustive_keys[k] = new Timer(tdd.ttl_db_msec_ttl);
                return;
            }
            if (tdd.dh.not_exhaustive_keys.size < max_not_exhaustive_keys)
            {
                tdd.dh.not_exhaustive_keys[k] = new Timer(tdd.ttl_db_msec_ttl);
                return;
            }
            tdd.dh.timer_default_not_exhaustive = new Timer(tdd.ttl_db_msec_ttl);
            debug("TemporalDatabase becomes default_not_exhaustive\n");
            tdd.dh.not_exhaustive_keys.clear();
        }

        internal void ttl_db_add_not_found(ITemporalDatabaseDescriptor tdd, Object k)
        {
            int max_not_found_keys = tdd.ttl_db_max_keys / 2;
            assert(! (tdd.dh.not_exhaustive_keys.has_key(k)));
            if (k in tdd.dh.not_found_keys)
                tdd.dh.not_found_keys.remove(k);
            if (tdd.dh.not_found_keys.size >= max_not_found_keys)
                tdd.dh.not_found_keys.remove_at(0);
            tdd.dh.not_found_keys.add(k);
        }

        internal void ttl_db_remove_not_exhaustive(ITemporalDatabaseDescriptor tdd, Object k)
        {
            if (tdd.dh.not_exhaustive_keys.has_key(k))
                tdd.dh.not_exhaustive_keys.unset(k);
        }

        internal void ttl_db_remove_not_found(ITemporalDatabaseDescriptor tdd, Object k)
        {
            if (k in tdd.dh.not_found_keys)
                tdd.dh.not_found_keys.remove(k);
        }
    }
}