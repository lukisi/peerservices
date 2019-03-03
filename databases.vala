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
using Netsukuku.PeerServices.Utils;

namespace Netsukuku.PeerServices
{
    public interface IReplicaContinuation : Object
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
        public abstract IPeersResponse execute(IPeersRequest r, Gee.List<int> client_tuple) throws PeersRefuseExecutionError, PeersRedoFromStartError;

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
        public long ttl_db_msec_ttl {get {return ttl_db_msec_ttl_getter();}}
        public abstract long ttl_db_msec_ttl_getter();
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
    /* ContactPeer: Start message to servant. This will wait, if needed,
     * for participation maps. See difference with the method `contact_peer`
     * of class MessageRouting.
     */
    internal delegate IPeersResponse ContactPeer
         (int p_id,
          PeerTupleNode x_macron,
          IPeersRequest request,
          int timeout_exec,
          int exclude_my_gnode,
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
        public Timer.chrono()
        {
            start = TimeVal();
            start.get_current_time();
            this.msec_ttl = 0;
        }

        public long get_lap()
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

    internal int eval_coherence_delta(int num_nodes)
    {
        if (num_nodes < 50) return 2000;
        if (num_nodes < 500) return 20000;
        if (num_nodes < 5000) return 30000;
        return 50000;
    }

    internal class Databases : Object
    {
        private int levels;
        private ArrayList<int> pos;
        private ArrayList<int> gsizes;
        private int guest_gnode_level;
        private int new_gnode_level;
        private bool first_identity;
        private Timer timer_first_entry;
        private ContactPeer contact_peer;
        private AssertServiceRegistered assert_service_registered;
        private IsServiceOptional is_service_optional;
        private WaitParticipationMaps wait_participation_maps;
        private ComputeDist compute_dist;
        private GetNodesInMyGroup get_nodes_in_my_group;

        public Databases
        (Gee.List<int> pos,
         Gee.List<int> gsizes,
         int guest_gnode_level,
         int new_gnode_level,
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
            levels = pos.size;
            this.guest_gnode_level = guest_gnode_level;
            this.new_gnode_level = new_gnode_level;
            if (guest_gnode_level == -1)
            {
                assert(new_gnode_level == levels);
                first_identity = true;
            }
            else
            {
                assert(new_gnode_level < levels);
                assert(guest_gnode_level <= new_gnode_level);
                assert(0 <= guest_gnode_level);
                first_identity = false;
            }
            timer_first_entry = new Timer.chrono();
            this.contact_peer = (owned) contact_peer;
            this.assert_service_registered = (owned) assert_service_registered;
            this.is_service_optional = (owned) is_service_optional;
            this.wait_participation_maps = (owned) wait_participation_maps;
            this.compute_dist = (owned) compute_dist;
            this.get_nodes_in_my_group = (owned) get_nodes_in_my_group;
        }

        /* Algorithms to maintain a robust distributed database */

        private class ReplicaContinuation : Object, IReplicaContinuation
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
                 out IReplicaContinuation cont)
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

        public bool next_replica(IReplicaContinuation cont, out IPeersResponse? resp)
        {
            ReplicaContinuation _cont = (ReplicaContinuation)cont;
            resp = null;
            if (_cont.replicas.size >= _cont.q) return false;
            PeerTupleNode respondant;
            try {
                resp = contact_peer
                    (_cont.p_id, _cont.x_macron, _cont.r,
                     _cont.timeout_exec, 0,
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

        public void ttl_db_on_startup
        (ITemporalDatabaseDescriptor tdd, int p_id,
         ITemporalDatabaseDescriptor? prev_id_tdd)
        {
            assert_service_registered(p_id);
            TtlDbOnStartupTasklet ts = new TtlDbOnStartupTasklet();
            ts.t = this;
            ts.tdd = tdd;
            ts.p_id = p_id;
            ts.prev_id_tdd = prev_id_tdd;
            tasklet.spawn(ts);
        }
        private class TtlDbOnStartupTasklet : Object, ITaskletSpawnable
        {
            public Databases t;
            public ITemporalDatabaseDescriptor tdd;
            public int p_id;
            public ITemporalDatabaseDescriptor? prev_id_tdd;
            public void * func()
            {
                debug("starting tasklet_ttl_db_on_startup.\n");
                t.tasklet_ttl_db_on_startup
                    (tdd, p_id, prev_id_tdd);
                debug("ending tasklet_ttl_db_on_startup.\n");
                return null;
            }
        }
        private void tasklet_ttl_db_on_startup
        (ITemporalDatabaseDescriptor tdd, int p_id,
         ITemporalDatabaseDescriptor? prev_id_tdd)
        {
            tdd.dh = new DatabaseHandler();
            tdd.dh.p_id = p_id;
            tdd.dh.timer_default_not_exhaustive = new Timer(tdd.ttl_db_msec_ttl);
            tdd.dh.not_found_keys = new ArrayList<Object>(tdd.key_equal_data);
            tdd.dh.not_exhaustive_keys = new HashMap<Object, Timer>(tdd.key_hash_data, tdd.key_equal_data);
            tdd.dh.retrieving_keys = new HashMap<Object, IChannel>(tdd.key_hash_data, tdd.key_equal_data);
            if (prev_id_tdd == null)
            {
                // we're exhaustive because it's a new network.
                tdd.dh.timer_default_not_exhaustive = null;
                return;
            }
            foreach (Object k in prev_id_tdd.ttl_db_get_all_keys())
            {
                var h_p_k = tdd.evaluate_hash_node(k);
                int l = h_p_k.size;
                if (guest_gnode_level >= l)
                {
                    if (prev_id_tdd.my_records_contains(k))
                    {
                        tdd.set_record_for_key(k, dup_object(prev_id_tdd.get_record_for_key(k)));
                    }
                    // else I am not exhaustive for key `k`
                }
                else if (new_gnode_level >= l)
                {
                    tdd.dh.not_found_keys.add(k);
                }
                // else I am not exhaustive for key `k`
            }
            tasklet.ms_wait(2000);
            IPeersRequest r = new RequestSendKeys(tdd.ttl_db_max_records);
            PeerTupleNode tuple_n;
            PeerTupleNode respondant;
            IPeersResponse _ret;
            try
            {
                tuple_n = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), levels);
                debug("starting contact_peer for a request of send_keys.\n");
                _ret = contact_peer(p_id, tuple_n, r, tdd.ttl_db_timeout_exec_send_keys, guest_gnode_level, out respondant);
                debug("returned from contact_peer for a request of send_keys.\n");
            } catch (PeersNoParticipantsInNetworkError e) {
                tdd.dh.timer_default_not_exhaustive = null;
                debug("we're exhaustive because nobody participates.\n");
                return;
            } catch (PeersDatabaseError e) {
                tdd.dh.timer_default_not_exhaustive = null;
                debug("we're exhaustive because nobody participates.\n");
                return;
            }
            Timer timer_startup = new Timer(tdd.ttl_db_msec_ttl / 10);
            try
            {
                if (_ret is RequestSendKeysResponse)
                {
                    RequestSendKeysResponse ret = (RequestSendKeysResponse)_ret;
                    debug(@"it is a valid response with $(ret.keys.size) keys.\n");
                    foreach (Object k in ret.keys)
                    {
                        if (! ttl_db_is_out_of_memory(tdd))
                        {
                            if (tdd.is_valid_key(k))
                            {
                                if ((! tdd.my_records_contains(k)) &&
                                    (! ttl_db_is_exhaustive(tdd, k)) &&
                                    (! tdd.dh.retrieving_keys.has_key(k)))
                                {
                                    PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k));
                                    int l = h_p_k.tuple.size;
                                    int @case;
                                    HCoord gn;
                                    Utils.convert_tuple_gnode(pos, Utils.tuple_node_to_tuple_gnode(respondant), out @case, out gn);
                                    if (gn.lvl <= l)
                                    {
                                        PeerTupleNode tuple_n_inside_l = Utils.rebase_tuple_node(pos, tuple_n, l);
                                        PeerTupleNode respondant_inside_l = Utils.rebase_tuple_node(pos, respondant, l);
                                        if (compute_dist(h_p_k, tuple_n_inside_l) < compute_dist(h_p_k, respondant_inside_l))
                                        {
                                            ttl_db_start_retrieve(tdd, k);
                                            tasklet.ms_wait(2000);
                                            if (timer_startup.is_expired())
                                            {
                                                debug("we're not exhaustive, but the time is over.\n");
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                PeerTupleGNodeContainer exclude_tuple_list = new PeerTupleGNodeContainer(levels);
                PeerTupleGNode t_respondant = new PeerTupleGNode(respondant.tuple, levels);
                exclude_tuple_list.add(t_respondant);
                while (true)
                {
                    if (ttl_db_is_out_of_memory(tdd))
                    {
                        debug("we're not exhaustive, but the memory is over.\n");
                        return;
                    }
                    tasklet.ms_wait(2000);
                    if (timer_startup.is_expired())
                    {
                        debug("we're not exhaustive, but the time is over.\n");
                        return;
                    }
                    respondant = null;
                    debug("starting contact_peer for another request of send_keys.\n");
                    _ret = contact_peer(p_id, tuple_n, r, tdd.ttl_db_timeout_exec_send_keys, guest_gnode_level, out respondant, exclude_tuple_list);
                    if (_ret is RequestSendKeysResponse)
                    {
                        RequestSendKeysResponse ret = (RequestSendKeysResponse)_ret;
                        foreach (Object k in ret.keys)
                        {
                            if (tdd.is_valid_key(k))
                            {
                                if ((! tdd.my_records_contains(k)) &&
                                    (! ttl_db_is_exhaustive(tdd, k)) &&
                                    (! tdd.dh.retrieving_keys.has_key(k)))
                                {
                                    PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k));
                                    int l = h_p_k.tuple.size;
                                    int @case;
                                    HCoord gn;
                                    Utils.convert_tuple_gnode(pos, Utils.tuple_node_to_tuple_gnode(respondant), out @case, out gn);
                                    if (gn.lvl <= l)
                                    {
                                        PeerTupleNode tuple_n_inside_l = Utils.rebase_tuple_node(pos, tuple_n, l);
                                        PeerTupleNode respondant_inside_l = Utils.rebase_tuple_node(pos, respondant, l);
                                        if (compute_dist(h_p_k, tuple_n_inside_l) < compute_dist(h_p_k, respondant_inside_l))
                                        {
                                            ttl_db_start_retrieve(tdd, k);
                                            if (ttl_db_is_out_of_memory(tdd))
                                            {
                                                debug("we're not exhaustive, but the memory is over.\n");
                                                return;
                                            }
                                            tasklet.ms_wait(2000);
                                            if (timer_startup.is_expired())
                                            {
                                                debug("we're not exhaustive, but the time is over.\n");
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (ttl_db_is_out_of_memory(tdd))
                        {
                            debug("we're not exhaustive, but the memory is over.\n");
                            return;
                        }
                    }
                    t_respondant = Utils.tuple_node_to_tuple_gnode(respondant);
                    exclude_tuple_list.add(t_respondant);
                }
            } catch (PeersNoParticipantsInNetworkError e) {
                tdd.dh.timer_default_not_exhaustive = null;
                debug("we're exhaustive because we got answers from every participant.\n");
                return;
            } catch (PeersDatabaseError e) {
                tdd.dh.timer_default_not_exhaustive = null;
                debug("we're exhaustive because we got answers from every participant.\n");
                return;
            }
        }

        public IPeersResponse
        ttl_db_on_request(ITemporalDatabaseDescriptor tdd, IPeersRequest r, Gee.List<int> client_tuple)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            while (tdd.dh == null) tasklet.ms_wait(10);
            if (tdd.dh == null)
            {
                throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not even started. level=$(guest_gnode_level)");
            }
            if (r is RequestSendKeys)
            {
                RequestSendKeys _r = (RequestSendKeys)r;
                RequestSendKeysResponse ret = new RequestSendKeysResponse();
                foreach (Object k in tdd.ttl_db_get_all_keys())
                {
                    ret.keys.add(k);
                    if (ret.keys.size >= _r.max_count) break;
                }
                return ret;
            }
            if (r is RequestWaitThenSendRecord)
            {
                Object k = ((RequestWaitThenSendRecord)r).k;
                if ((! tdd.my_records_contains(k)) && (! ttl_db_is_exhaustive(tdd, k)))
                {
                    int exclude_my_gnode = first_identity || (timer_first_entry.get_lap() > tdd.ttl_db_msec_ttl) ? 0 : guest_gnode_level;
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not exhaustive. level=$(exclude_my_gnode)");
                }
                int common_lvl = client_tuple.size;
                int delta = eval_coherence_delta(get_nodes_in_my_group(common_lvl));
                if (delta > RequestWaitThenSendRecord.timeout_exec - 1000)
                    delta = RequestWaitThenSendRecord.timeout_exec - 1000;
                tasklet.ms_wait(delta);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return new RequestWaitThenSendRecordResponse(tdd.get_record_for_key(k));
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return new RequestWaitThenSendRecordNotFound();
                }
                else
                {
                    throw new PeersRedoFromStartError.GENERIC("");
                }
            }
            if (tdd.is_insert_request(r))
            {
                debug("ttl_db_on_request: insert request.\n");
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.prepare_response_not_free(r, tdd.get_record_for_key(k));
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    debug("ttl_db_on_request: insert request: exhaustive for k.\n");
                    if (ttl_db_is_out_of_memory(tdd))
                    {
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        throw new PeersRefuseExecutionError.WRITE_OUT_OF_MEMORY("my node is out of memory");
                    }
                    else
                    {
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_remove_not_exhaustive(tdd, k);
                        IPeersResponse res = tdd.execute(r, client_tuple);
                        if (! tdd.my_records_contains(k))
                            ttl_db_add_not_found(tdd, k);
                        return res;
                    }
                }
                else
                {
                    debug("ttl_db_on_request: insert request: not exhaustive for k.\n");
                    if (tdd.dh.retrieving_keys.has_key(k))
                    {
                        IChannel ch = tdd.dh.retrieving_keys[k];
                        try {
                            ch.recv_with_timeout(tdd.get_timeout_exec(r) - 1000);
                        } catch (ChannelError e) {
                        }
                        throw new PeersRedoFromStartError.GENERIC("");
                    }
                    else
                    {
                        if (! ttl_db_is_out_of_memory(tdd))
                        {
                            ttl_db_start_retrieve(tdd, k);
                            tasklet.ms_wait(100);
                            throw new PeersRedoFromStartError.GENERIC("");
                        }
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        int exclude_my_gnode = 0;
                        throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not exhaustive. level=$(exclude_my_gnode)");
                    }
                }
            }
            if (tdd.is_read_only_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.execute(r, client_tuple);
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return tdd.prepare_response_not_found(r);
                }
                else
                {
                    int exclude_my_gnode = first_identity || (timer_first_entry.get_lap() > tdd.ttl_db_msec_ttl) ? 0 : guest_gnode_level;
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not exhaustive. level=$(exclude_my_gnode)");
                }
            }
            if (tdd.is_update_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.execute(r, client_tuple);
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return tdd.prepare_response_not_found(r);
                }
                else
                {
                    if (tdd.dh.retrieving_keys.has_key(k))
                    {
                        IChannel ch = tdd.dh.retrieving_keys[k];
                        try {
                            ch.recv_with_timeout(tdd.get_timeout_exec(r) - 1000);
                        } catch (ChannelError e) {
                        }
                        throw new PeersRedoFromStartError.GENERIC("");
                    }
                    else
                    {
                        if (! ttl_db_is_out_of_memory(tdd))
                        {
                            ttl_db_start_retrieve(tdd, k);
                            tasklet.ms_wait(100);
                            throw new PeersRedoFromStartError.GENERIC("");
                        }
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        int exclude_my_gnode = 0;
                        throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not exhaustive. level=$(exclude_my_gnode)");
                    }
                }
            }
            if (tdd.is_replica_value_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    var res = tdd.execute(r, client_tuple);
                    assert(tdd.my_records_contains(k));
                    return res;
                }
                if (! ttl_db_is_out_of_memory(tdd))
                {
                    ttl_db_remove_not_found(tdd, k);
                    ttl_db_remove_not_exhaustive(tdd, k);
                    var res = tdd.execute(r, client_tuple);
                    assert(tdd.my_records_contains(k));
                    return res;
                }
                else
                {
                    ttl_db_remove_not_found(tdd, k);
                    ttl_db_add_not_exhaustive(tdd, k);
                    throw new PeersRefuseExecutionError.WRITE_OUT_OF_MEMORY("my node is out of memory");
                }
            }
            if (tdd.is_replica_delete_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                var res = tdd.execute(r, client_tuple);
                assert(! tdd.my_records_contains(k));
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_add_not_found(tdd, k);
                return res;
            }
            // none of previous cases
            return tdd.execute(r, client_tuple);
        }

        internal void ttl_db_start_retrieve(ITemporalDatabaseDescriptor tdd, Object k)
        {
            IChannel ch = tasklet.get_channel();
            tdd.dh.retrieving_keys[k] = ch;
            TtlDbStartRetrieveTasklet ts = new TtlDbStartRetrieveTasklet();
            ts.t = this;
            ts.tdd = tdd;
            ts.k = k;
            ts.ch = ch;
            tasklet.spawn(ts);
        }
        private class TtlDbStartRetrieveTasklet : Object, ITaskletSpawnable
        {
            public Databases t;
            public ITemporalDatabaseDescriptor tdd;
            public Object k;
            public IChannel ch;
            public void * func()
            {
                t.tasklet_ttl_db_start_retrieve(tdd, k, ch);
                return null;
            }
        }
        private void tasklet_ttl_db_start_retrieve(ITemporalDatabaseDescriptor tdd, Object k, IChannel ch)
        {
            bool optional = is_service_optional(tdd.dh.p_id);
            if (optional) wait_participation_maps(tdd.evaluate_hash_node(k).size);
            Object? record = null;
            IPeersRequest r = new RequestWaitThenSendRecord(k);
            try {
                PeerTupleNode respondant;
                PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k));
                debug(@"starting contact_peer for a request of wait_then_send_record (Key is a $(k.get_type().name())).\n");
                IPeersResponse res = contact_peer(tdd.dh.p_id, h_p_k, r, RequestWaitThenSendRecord.timeout_exec, 0, out respondant);
                if (res is RequestWaitThenSendRecordResponse)
                {
                    debug(@"the request of wait_then_send_record returned a record.\n");
                    record = ((RequestWaitThenSendRecordResponse)res).record;
                }
            } catch (PeersNoParticipantsInNetworkError e) {
                debug(@"the request of wait_then_send_record threw a PeersNoParticipantsInNetworkError.\n");
                // nop
            } catch (PeersDatabaseError e) {
                debug(@"the request of wait_then_send_record threw a PeersDatabaseError.\n");
                // nop
            }
            if (record != null && tdd.is_valid_record(k, record))
            {
                debug(@"putting the record in my memory.\n");
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_remove_not_found(tdd, k);
                tdd.set_record_for_key(k, record);
            }
            else
            {
                debug(@"the request of wait_then_send_record returned a not_found.\n");
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_add_not_found(tdd, k);
            }
            IChannel temp_ch = tdd.dh.retrieving_keys[k];
            tdd.dh.retrieving_keys.unset(k);
            while (temp_ch.get_balance() < 0) temp_ch.send_async(0);
        }

        public void fixed_keys_db_on_startup
        (IFixedKeysDatabaseDescriptor fkdd, int p_id,
         IFixedKeysDatabaseDescriptor? prev_id_fkdd)
        {
            assert_service_registered(p_id);
            FixedKeysDbOnStartupTasklet ts = new FixedKeysDbOnStartupTasklet();
            ts.t = this;
            ts.fkdd = fkdd;
            ts.p_id = p_id;
            ts.prev_id_fkdd = prev_id_fkdd;
            tasklet.spawn(ts);
        }
        private class FixedKeysDbOnStartupTasklet : Object, ITaskletSpawnable
        {
            public Databases t;
            public IFixedKeysDatabaseDescriptor fkdd;
            public int p_id;
            public IFixedKeysDatabaseDescriptor? prev_id_fkdd;
            public void * func()
            {
                t.tasklet_fixed_keys_db_on_startup
                    (fkdd, p_id, prev_id_fkdd);
                return null;
            }
        }
        private void tasklet_fixed_keys_db_on_startup
        (IFixedKeysDatabaseDescriptor fkdd, int p_id,
         IFixedKeysDatabaseDescriptor? prev_id_fkdd)
        {
            fkdd.dh = new DatabaseHandler();
            fkdd.dh.p_id = p_id;
            fkdd.dh.not_completed_keys = new ArrayList<Object>(fkdd.key_equal_data);
            fkdd.dh.retrieving_keys = new HashMap<Object, IChannel>(fkdd.key_hash_data, fkdd.key_equal_data);
            Gee.List<Object> k_set = fkdd.get_full_key_domain();
            fkdd.dh.not_completed_keys.add_all(k_set);
            foreach (Object k in k_set)
            {
                int l = fkdd.evaluate_hash_node(k).size;
                if (guest_gnode_level >= l)
                {
                    assert(prev_id_fkdd != null);
                    fkdd.set_record_for_key(k, prev_id_fkdd.get_record_for_key(k));
                    fkdd.dh.not_completed_keys.remove(k);
                }
                else if (new_gnode_level >= l)
                {
                    fkdd.set_record_for_key(k, fkdd.get_default_record_for_key(k));
                    fkdd.dh.not_completed_keys.remove(k);
                }
            }
            tasklet.ms_wait(2000);
            bool wait_before_network_activity = false;
            foreach (Object k in k_set)
            {
                int l = fkdd.evaluate_hash_node(k).size;
                if (guest_gnode_level >= l) continue;
                if (new_gnode_level >= l) continue;
                if (wait_before_network_activity) tasklet.ms_wait(200);
                fixed_keys_db_start_retrieve(fkdd, k);
                wait_before_network_activity = true;
            }
        }

        public IPeersResponse
        fixed_keys_db_on_request(IFixedKeysDatabaseDescriptor fkdd, IPeersRequest r, Gee.List<int> client_tuple)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            while (fkdd.dh == null) tasklet.ms_wait(10);
            if (fkdd.dh == null)
                throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not even started. level=$(guest_gnode_level)");
            if (r is RequestWaitThenSendRecord)
            {
                Object k = ((RequestWaitThenSendRecord)r).k;
                if (k in fkdd.dh.not_completed_keys)
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not exhaustive. level=$(guest_gnode_level)");
                assert(fkdd.my_records_contains(k));
                int common_lvl = client_tuple.size;
                int delta = eval_coherence_delta(get_nodes_in_my_group(common_lvl));
                if (delta > RequestWaitThenSendRecord.timeout_exec - 1000)
                    delta = RequestWaitThenSendRecord.timeout_exec - 1000;
                tasklet.ms_wait(delta);
                assert(fkdd.my_records_contains(k));
                return new RequestWaitThenSendRecordResponse(fkdd.get_record_for_key(k));
            }
            if (fkdd.is_read_only_request(r))
            {
                Object k = fkdd.get_key_from_request(r);
                if (! (k in fkdd.dh.not_completed_keys))
                {
                    assert(fkdd.my_records_contains(k));
                    return fkdd.execute(r, client_tuple);
                }
                else
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE(@"not exhaustive. level=$(guest_gnode_level)");
                }
            }
            if (fkdd.is_update_request(r))
            {
                Object k = fkdd.get_key_from_request(r);
                if (! (k in fkdd.dh.not_completed_keys))
                {
                    assert(fkdd.my_records_contains(k));
                    return fkdd.execute(r, client_tuple);
                }
                else
                {
                    while (! fkdd.dh.retrieving_keys.has_key(k)) tasklet.ms_wait(200);
                    IChannel ch = fkdd.dh.retrieving_keys[k];
                    try {
                        ch.recv_with_timeout(fkdd.get_timeout_exec(r) - 1000);
                    } catch (ChannelError e) {
                    }
                    throw new PeersRedoFromStartError.GENERIC("");
                }
            }
            if (fkdd.is_replica_value_request(r))
            {
                return fkdd.execute(r, client_tuple);
            }
            // none of previous cases
            return fkdd.execute(r, client_tuple);
        }

        internal void fixed_keys_db_start_retrieve(IFixedKeysDatabaseDescriptor fkdd, Object k)
        {
            IChannel ch = tasklet.get_channel();
            fkdd.dh.retrieving_keys[k] = ch;
            FixedKeysDbStartRetrieveTasklet ts = new FixedKeysDbStartRetrieveTasklet();
            ts.t = this;
            ts.fkdd = fkdd;
            ts.k = k;
            ts.ch = ch;
            tasklet.spawn(ts);
        }
        private class FixedKeysDbStartRetrieveTasklet : Object, ITaskletSpawnable
        {
            public Databases t;
            public IFixedKeysDatabaseDescriptor fkdd;
            public Object k;
            public IChannel ch;
            public void * func()
            {
                t.tasklet_fixed_keys_db_start_retrieve(fkdd, k, ch); 
                return null;
            }
        }
        private void tasklet_fixed_keys_db_start_retrieve(IFixedKeysDatabaseDescriptor fkdd, Object k, IChannel ch)
        {
            bool optional = is_service_optional(fkdd.dh.p_id);
            if (optional) wait_participation_maps(fkdd.evaluate_hash_node(k).size);
            Object? record = null;
            IPeersRequest r = new RequestWaitThenSendRecord(k);
            int timeout_exec = RequestWaitThenSendRecord.timeout_exec;
            PeerTupleNode h_p_k;
            while (true)
            {
                try {
                    PeerTupleNode respondant;
                    h_p_k = new PeerTupleNode(fkdd.evaluate_hash_node(k));
                    debug(@"RequestWaitThenSendRecord: Starting contact_peer from $(this.get_type().name()):");
                    debug(@"RequestWaitThenSendRecord: p_id=$(fkdd.dh.p_id), k is a $(k.get_type().name())), h_p(k)=$(h_p_k).");
                    IPeersResponse res = contact_peer(fkdd.dh.p_id, h_p_k, r, timeout_exec, 0, out respondant);
                    if (res is RequestWaitThenSendRecordResponse)
                        record = ((RequestWaitThenSendRecordResponse)res).record;
                    debug(@"RequestWaitThenSendRecord($(fkdd.dh.p_id)) to $(h_p_k): got record from $(respondant). Is it valid?");
                    break;
                } catch (PeersNoParticipantsInNetworkError e) {
                    debug(@"RequestWaitThenSendRecord($(fkdd.dh.p_id)) to $(h_p_k): got PeersNoParticipantsInNetworkError: abort.");
                    break;
                } catch (PeersDatabaseError e) {
                    debug(@"RequestWaitThenSendRecord($(fkdd.dh.p_id)) to $(h_p_k): got PeersDatabaseError: try again.");
                    tasklet.ms_wait(200);
                }
            }
            if (record != null && fkdd.is_valid_record(k, record))
            {
                debug(@"RequestWaitThenSendRecord($(fkdd.dh.p_id)) to $(h_p_k): record is valid.");
                fkdd.set_record_for_key(k, record);
            }
            else
            {
                debug(@"RequestWaitThenSendRecord($(fkdd.dh.p_id)) to $(h_p_k): use default record.");
                fkdd.set_record_for_key(k, fkdd.get_default_record_for_key(k));
            }
            IChannel temp_ch = fkdd.dh.retrieving_keys[k];
            fkdd.dh.retrieving_keys.unset(k);
            while (temp_ch.get_balance() < 0) temp_ch.send_async(0);
            fkdd.dh.not_completed_keys.remove(k);
        }
    }
}
