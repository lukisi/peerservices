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

namespace Netsukuku.PeerServices
{
    internal string json_string_object(Object obj)
    {
        Json.Node n = Json.gobject_serialize(obj);
        Json.Generator g = new Json.Generator();
        g.root = n;
        string ret = g.to_data(null);
        return ret;
    }

    internal Object dup_object(Object obj)
    {
        Type type = obj.get_type();
        string t = json_string_object(obj);
        Json.Parser p = new Json.Parser();
        try {
            assert(p.load_from_data(t));
        } catch (Error e) {assert_not_reached();}
        Object ret = Json.gobject_deserialize(type, p.get_root());
        return ret;
    }

    public errordomain PeersNonexistentDestinationError {
        GENERIC
    }

    public errordomain PeersNonexistentFellowError {
        GENERIC
    }

    public errordomain PeersNoParticipantsInNetworkError {
        GENERIC
    }

    public errordomain PeersRefuseExecutionError {
        WRITE_OUT_OF_MEMORY,
        READ_NOT_FOUND_NOT_EXHAUSTIVE,
        GENERIC
    }

    public errordomain PeersDatabaseError {
        GENERIC
    }

    public errordomain PeersRedoFromStartError {
        GENERIC
    }

    public interface IPeersMapPaths : Object
    {
        public abstract int i_peers_get_levels();
        public abstract int i_peers_get_gsize(int level);
        public abstract int i_peers_get_my_pos(int level);
        public abstract int i_peers_get_nodes_in_my_group(int level);
        public abstract bool i_peers_exists(int level, int pos);
        public abstract IPeersManagerStub i_peers_gateway
            (int level, int pos,
             CallerInfo? received_from=null,
             IPeersManagerStub? failed=null)
            throws PeersNonexistentDestinationError;
        public abstract IPeersManagerStub? i_peers_neighbor_at_level
            (int level,
             IPeersManagerStub? failed=null);
    }

    public interface IPeersBackStubFactory : Object
    {
        // positions[0] is pos[0] of the node to contact inside our gnode
        // of level positions.size
        public abstract IPeersManagerStub i_peers_get_tcp_inside
            (Gee.List<int> positions);
    }

    public interface IPeersNeighborsFactory : Object
    {
        public abstract IPeersManagerStub i_peers_get_broadcast(
                            IPeersMissingArcHandler missing_handler);
        public abstract IPeersManagerStub i_peers_get_tcp(
                            IPeersArc arc);
    }

    public interface IPeersMissingArcHandler : Object
    {
        public abstract void i_peers_missing(IPeersArc missing_arc);
    }

    public interface IPeersArc : Object
    {
    }

    public interface IPeersContinuation : Object
    {
    }

    internal class WaitingAnswer : Object
    {
        public IChannel ch;
        public IPeersRequest? request;
        public PeerTupleGNode min_target;
        public PeerTupleGNode? exclude_gnode;
        public PeerTupleGNode? non_participant_gnode;
        public PeerTupleNode? respondant_node;
        public IPeersResponse? response;
        public string? refuse_message;
        public bool redo_from_start;
        public WaitingAnswer(IPeersRequest? request, PeerTupleGNode min_target)
        {
            ch = tasklet.get_channel();
            this.request = request;
            this.min_target = min_target;
            exclude_gnode = null;
            non_participant_gnode = null;
            respondant_node = null;
            response = null;
            refuse_message = null;
            redo_from_start = false;
        }
    }

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

    public class DatabaseHandler : Object
    {
        internal DatabaseHandler()
        {
            // ...
        }
        internal int p_id;
        internal bool ready;
        internal HashMap<Object,IChannel> retrieving_keys;
        // for TTL-based services
        internal HashMap<Object,Timer> not_exhaustive_keys;
        internal ArrayList<Object> not_found_keys;
        internal Timer? timer_default_not_exhaustive;
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

    internal ITasklet tasklet;
    public class PeersManager : Object,
                                IPeersManagerSkeleton
    {
        public static void init(ITasklet _tasklet)
        {
            // Register serializable types
            typeof(PeerTupleNode).class_peek();
            typeof(PeerTupleGNode).class_peek();
            typeof(PeerMessageForwarder).class_peek();
            typeof(PeerParticipantMap).class_peek();
            typeof(PeerParticipantSet).class_peek();
            tasklet = _tasklet;
        }

        private IPeersMapPaths map_paths;
        private int levels;
        private int[] gsizes;
        private int[] pos;
        private IPeersBackStubFactory back_stub_factory;
        private IPeersNeighborsFactory neighbors_factory;
        private HashMap<int, PeerService> services;
        private HashMap<int, WaitingAnswer> waiting_answer_map;
        private HashMap<int, PeerParticipantMap> participant_maps;
        private ArrayList<HCoord> recent_published_list;
        public int guest_gnode_level {get; private set;}
        public int host_gnode_level {get; private set;}
        public int maps_retrieved_below_level {
            get {
                return map_handler.maps_retrieved_below_level;
            }
        }
        private MapHandler.MapHandler map_handler;
        private PeerParticipantSet map;
        private Gee.List<int> my_services;
        private MessageRouting.MessageRouting message_routing;

        public PeersManager
        (PeersManager? old_identity,
        int guest_gnode_level,
        int host_gnode_level,
        IPeersMapPaths map_paths,
        IPeersBackStubFactory back_stub_factory,
        IPeersNeighborsFactory neighbors_factory)
        {
            this.map_paths = map_paths;
            levels = map_paths.i_peers_get_levels();
            gsizes = new int[levels];
            pos = new int[levels];
            for (int i = 0; i < levels; i++)
            {
                gsizes[i] = map_paths.i_peers_get_gsize(i);
                pos[i] = map_paths.i_peers_get_my_pos(i);
            }
            this.back_stub_factory = back_stub_factory;
            this.neighbors_factory = neighbors_factory;
            services = new HashMap<int, PeerService>();
            waiting_answer_map = new HashMap<int, WaitingAnswer>();
            participant_maps = new HashMap<int, PeerParticipantMap>();
            recent_published_list = new ArrayList<HCoord>((a, b) => a.equals(b));
            this.guest_gnode_level = guest_gnode_level;
            this.host_gnode_level = host_gnode_level;

            map = new PeerParticipantSet(new ArrayList<int>.wrap(pos));
            my_services = new ArrayList<int>();
            map_handler = new MapHandler.MapHandler
                (new ArrayList<int>.wrap(pos),
                 /*ClearMapsAtLevel*/ (lvl) => {
                     clear_maps_at_level(lvl);
                 },
                 /*AddParticipant*/ (p_id, h) => {
                     add_participant(p_id, h);
                 },
                 /*RemoveParticipant*/ (p_id, h) => {
                     remove_participant(p_id, h);
                 },
                 /*ProduceMapsCopy*/ () => {
                     return produce_maps_copy();
                 },
                 /*GetNeighborAtLevel*/ (lvl, failing_stub) => {
                     return get_neighbor_at_level(lvl, failing_stub);
                 },
                 /*GetBroadcastNeighbors*/ (fn_mah) => {
                     return get_broadcast_neighbors((owned) fn_mah);
                 },
                 /*GetUnicastNeighbor*/ (missing_arc) => {
                     return get_unicast_neighbor(missing_arc);
                 });
            if (old_identity == null)
            {
                map_handler.create_net();
            }
            else
            {
                map_handler.enter_net(old_identity.map_handler, guest_gnode_level, host_gnode_level);
            }

            message_routing = new MessageRouting.MessageRouting
                (new ArrayList<int>.wrap(pos), new ArrayList<int>.wrap(gsizes),
                 /* gnode_exists                  = */  (/*int*/ lvl, /*int*/ pos) => {
                     return map_paths.i_peers_exists(lvl, pos);
                 },
                 /* get_gateway                   = */  (/*int*/ level, /*int*/ pos,
                                                         /*CallerInfo?*/ received_from,
                                                         /*IPeersManagerStub?*/ failed) => {
                     IPeersManagerStub? ret = null;
                     try {
                        ret = map_paths.i_peers_gateway(level, pos, received_from, failed);
                     } catch (PeersNonexistentDestinationError e) {
                     }
                     return ret;
                 },
                 /* get_client_internally         = */  (/*PeerTupleNode*/ n) => {
                     return back_stub_factory.i_peers_get_tcp_inside(n.tuple);
                 },
                 /* get_nodes_in_my_group         = */  (/*int*/ lvl) => {
                     return map_paths.i_peers_get_nodes_in_my_group(lvl);
                 },
                 /* my_gnode_participates         = */  (/*int*/ p_id, /*int*/ lvl) => {
                     return my_gnode_participates(p_id, lvl);
                 },
                 /* get_non_participant_gnodes    = */  (/*int*/ p_id, /*int*/ target_levels) => {
                     return get_non_participant_gnodes(p_id, target_levels);
                 },
                 /* exec_service                  = */  (/*int*/ p_id, /*IPeersRequest*/ req,
                                                         /*Gee.List<int>*/ client_tuple) => {
                     assert(services.has_key(p_id));
                     return services[p_id].exec(req, client_tuple);
                 });
        }

        private void participate(int p_id)
        {
            if (! (p_id in my_services)) my_services.add(p_id);
            map_handler.participate(p_id);
        }

        private void dont_participate(int p_id)
        {
            if (p_id in my_services) my_services.remove(p_id);
            map_handler.dont_participate(p_id);
        }

        private void clear_maps_at_level(int lvl)
        {
            foreach (int p_id in map.participant_set.keys)
            {
                PeerParticipantMap m = map.participant_set[p_id];
                ArrayList<HCoord> to_del = new ArrayList<HCoord>();
                foreach (HCoord h in m.participant_list) if (h.lvl == lvl) to_del.add(h);
                m.participant_list.remove_all(to_del);
            }
        }
        private void add_participant(int p_id, HCoord h)
        {
            if (h.pos == pos[h.lvl]) return; // ignore myself
            if (! map.participant_set.has_key(p_id))
                map.participant_set[p_id] = new PeerParticipantMap();
            var the_list = map.participant_set[p_id].participant_list;
            if (! (h in the_list)) the_list.add(h);
        }
        private void remove_participant(int p_id, HCoord h)
        {
            if (h.pos == pos[h.lvl]) return; // ignore myself
            if (map.participant_set.has_key(p_id))
            {
                var the_list = map.participant_set[p_id].participant_list;
                if (h in the_list) the_list.remove(h);
                if (the_list.is_empty) map.participant_set.unset(p_id);
            }
        }
        private PeerParticipantSet produce_maps_copy()
        {
            var ret = new PeerParticipantSet(new ArrayList<int>.wrap(pos));
            foreach (int p_id in map.participant_set.keys)
            {
                ret.participant_set[p_id] = new PeerParticipantMap();
                ret.participant_set[p_id].participant_list.add_all
                    (map.participant_set[p_id].participant_list);
            }
            foreach (int p_id in my_services)
            {
                if (! ret.participant_set.has_key(p_id))
                    ret.participant_set[p_id] = new PeerParticipantMap();
                ret.participant_set[p_id].participant_list.add(new HCoord(0, pos[0]));
            }
            return ret;
        }
        private IPeersManagerStub? get_neighbor_at_level(int lvl, IPeersManagerStub? failing_stub)
        {
            return map_paths.i_peers_neighbor_at_level(lvl, failing_stub);
        }
        private IPeersManagerStub get_broadcast_neighbors(owned MapHandler.MissingArcHandler fn_mah)
        {
            MissingArcDelegation missing_handler = new MissingArcDelegation(this, (owned) fn_mah);
            return neighbors_factory.i_peers_get_broadcast(missing_handler);
        }
        class MissingArcDelegation : Object, IPeersMissingArcHandler
        {
            public MissingArcDelegation(PeersManager mgr, owned MapHandler.MissingArcHandler fn_mah)
            {
                this.mgr = mgr;
                this.fn_mah = (owned) fn_mah;
            }
            public MapHandler.MissingArcHandler fn_mah;
            private PeersManager mgr;
            public void i_peers_missing(IPeersArc missing_arc)
            {
                fn_mah(new MissingArcImpl(missing_arc));
            }
        }
        class MissingArcImpl : Object, MapHandler.IMissingArc
        {
            public IPeersArc arc;
            public MissingArcImpl(IPeersArc arc)
            {
                this.arc = arc;
            }
        }
        private IPeersManagerStub get_unicast_neighbor(MapHandler.IMissingArc missing_arc)
        {
            IPeersArc arc = ((MissingArcImpl)missing_arc).arc;
            return neighbors_factory.i_peers_get_tcp(arc);
        }

        public void register(PeerService p)
        {
            if (services.has_key(p.p_id))
            {
                critical("PeersManager.register: Two services with same ID?");
                return;
            }
            services[p.p_id] = p;
            if (p.p_is_optional) participate(p.p_id);
        }

        /* Helpers */

        private bool check_valid_message(PeerMessageForwarder mf)
        {
            return mf.check_valid(levels, gsizes);
        }

        private bool check_valid_tuple_node(PeerTupleNode n)
        {
            return n.check_valid(levels, gsizes);
        }

        private bool check_valid_tuple_gnode(PeerTupleGNode gn)
        {
            return gn.check_valid(levels, gsizes);
        }

        private bool check_valid_participant_map(PeerParticipantMap m)
        {
            return m.check_valid(levels, gsizes);
        }

        private bool check_valid_participant_set(PeerParticipantSet p)
        {
            return p.check_valid(levels, gsizes);
        }

        private bool my_gnode_participates(int p_id, int lvl)
        {
            // Tell whether my g-node at level lvl participates to service p_id.
            if (services.has_key(p_id))
                return true;
            if (! participant_maps.has_key(p_id))
                return false;
            PeerParticipantMap map = participant_maps[p_id];
            foreach (HCoord g in map.participant_list)
            {
                if (g.lvl < lvl)
                    return true;
            }
            return false;
        }

        private Gee.List<HCoord> get_non_participant_gnodes(int p_id, int target_levels=-1)
        {
            // Returns a list of HCoord representing each gnode visible in my topology which, to my
            //  knowledge, do not participate to service p_id
            if (target_levels == -1) target_levels = levels;
            ArrayList<HCoord> ret = new ArrayList<HCoord>();
            bool optional = false;
            if (services.has_key(p_id))
                optional = services[p_id].p_is_optional;
            else
                optional = true;
            if (optional)
            {
                PeerParticipantMap? map = null;
                if (participant_maps.has_key(p_id))
                    map = participant_maps[p_id];
                foreach (HCoord lp in get_all_gnodes_up_to_lvl(target_levels))
                {
                    if (map == null)
                        ret.add(lp);
                    else
                        if (! (lp in map.participant_list))
                            ret.add(lp);
                }
            }
            return ret;
        }

        private Gee.List<HCoord> get_all_gnodes_up_to_lvl(int lvl)
        {
            // Returns a list of HCoord representing each gnode visible in my topology which are inside
            //  my g-node at level lvl. Including single nodes, including myself as single node (0, pos[0]).
            ArrayList<HCoord> ret = new ArrayList<HCoord>();
            for (int l = 0; l < lvl; l++)
            {
                for (int p = 0; p < gsizes[l]; p++)
                {
                    if (pos[l] != p)
                        ret.add(new HCoord(l, p));
                }
            }
            ret.add(new HCoord(0, pos[0]));
            return ret;
        }

        private bool is_service_optional(int p_id)
        {
            bool ret = false;
            if (services.has_key(p_id))
                ret = services[p_id].p_is_optional;
            else
                ret = true;
            return ret;
        }

        private void wait_participation_maps(int target_levels)
        {
            while (maps_retrieved_below_level < target_levels)
                tasklet.ms_wait(10);
        }

        private void convert_tuple_gnode(PeerTupleGNode t, out int @case, out HCoord ret)
        {
            Utils.convert_tuple_gnode(new ArrayList<int>.wrap(pos), t, out @case, out ret);
        }

        private PeerTupleGNode make_tuple_gnode(HCoord h, int top)
        {
            return Utils.make_tuple_gnode(new ArrayList<int>.wrap(pos), h, top);
        }

        private PeerTupleNode make_tuple_node(HCoord h, int top)
        {
            return Utils.make_tuple_node(new ArrayList<int>.wrap(pos), h, top);
        }

        private PeerTupleGNode tuple_node_to_tuple_gnode(PeerTupleNode t)
        {
            return Utils.tuple_node_to_tuple_gnode(t);
        }

        private PeerTupleGNode rebase_tuple_gnode(PeerTupleGNode t, int new_top)
        {
            return Utils.rebase_tuple_gnode(new ArrayList<int>.wrap(pos), t, new_top);
        }

        private PeerTupleNode rebase_tuple_node(PeerTupleNode t, int new_top)
        {
            return Utils.rebase_tuple_node(new ArrayList<int>.wrap(pos), t, new_top);
        }

        private bool visible_by_someone_inside_my_gnode(PeerTupleGNode t, int lvl)
        {
            return Utils.visible_by_someone_inside_my_gnode(new ArrayList<int>.wrap(pos), t, lvl);
        }

        /* Routing algorithm */

        internal IPeersResponse contact_peer
        (int p_id,
         PeerTupleNode x_macron,
         IPeersRequest request,
         int timeout_exec,
         bool exclude_myself,
         out PeerTupleNode? respondant,
         PeerTupleGNodeContainer? _exclude_tuple_list=null)
        throws PeersNoParticipantsInNetworkError, PeersDatabaseError
        {
            bool optional = is_service_optional(p_id);
            if (optional) wait_participation_maps(x_macron.tuple.size);
            return message_routing.contact_peer
                (p_id, optional, x_macron, request, timeout_exec,
                 exclude_myself, out respondant, _exclude_tuple_list);
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
                debug("starting contact_peer for a request of replica.\n");
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

        public void ttl_db_on_startup(ITemporalDatabaseDescriptor tdd, int p_id, bool new_network)
        {
            assert(services.has_key(p_id));
            TtlDbOnStartupTasklet ts = new TtlDbOnStartupTasklet();
            ts.t = this;
            ts.tdd = tdd;
            ts.p_id = p_id;
            ts.new_network = new_network;
            tasklet.spawn(ts);
        }
        private class TtlDbOnStartupTasklet : Object, ITaskletSpawnable
        {
            public PeersManager t;
            public ITemporalDatabaseDescriptor tdd;
            public int p_id;
            public bool new_network;
            public void * func()
            {
                debug("starting tasklet_ttl_db_on_startup.\n");
                t.tasklet_ttl_db_on_startup(tdd, p_id, new_network);
                debug("ending tasklet_ttl_db_on_startup.\n");
                return null;
            }
        }
        private void tasklet_ttl_db_on_startup(ITemporalDatabaseDescriptor tdd, int p_id, bool new_network)
        {
            PeerService srv = services[p_id];
            tdd.dh = new DatabaseHandler();
            tdd.dh.p_id = p_id;
            tdd.dh.ready = false;
            if (srv.p_is_optional)
            {
                // TODO search a mechanism to validate the situation
                //   where maps_retrieved_below_level < levels.
                if (maps_retrieved_below_level < levels)
                {
                    error("not implemented yet");
                }
            }
            tdd.dh.timer_default_not_exhaustive = new Timer(tdd.ttl_db_msec_ttl);
            tdd.dh.not_found_keys = new ArrayList<Object>(tdd.key_equal_data);
            tdd.dh.not_exhaustive_keys = new HashMap<Object, Timer>(tdd.key_hash_data, tdd.key_equal_data);
            tdd.dh.retrieving_keys = new HashMap<Object, IChannel>(tdd.key_hash_data, tdd.key_equal_data);
            tdd.dh.ready = true;
            debug("database handler is ready.\n");
            if (new_network)
            {
                tdd.dh.timer_default_not_exhaustive = null;
                debug("we're exhaustive because it's a new network.\n");
                return;
            }
            IPeersRequest r = new RequestSendKeys(tdd.ttl_db_max_records);
            PeerTupleNode tuple_n;
            PeerTupleNode respondant;
            IPeersResponse _ret;
            try
            {
                tuple_n = make_tuple_node(new HCoord(0, pos[0]), levels);
                debug("starting contact_peer for a request of send_keys.\n");
                _ret = contact_peer(p_id, tuple_n, r, tdd.ttl_db_timeout_exec_send_keys, true, out respondant);
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
                                    convert_tuple_gnode(tuple_node_to_tuple_gnode(respondant), out @case, out gn);
                                    if (gn.lvl <= l)
                                    {
                                        PeerTupleNode tuple_n_inside_l = rebase_tuple_node(tuple_n, l);
                                        PeerTupleNode respondant_inside_l = rebase_tuple_node(respondant, l);
                                        if (message_routing.dist(h_p_k, tuple_n_inside_l) < message_routing.dist(h_p_k, respondant_inside_l))
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
                    _ret = contact_peer(p_id, tuple_n, r, tdd.ttl_db_timeout_exec_send_keys, true, out respondant, exclude_tuple_list);
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
                                    convert_tuple_gnode(tuple_node_to_tuple_gnode(respondant), out @case, out gn);
                                    if (gn.lvl <= l)
                                    {
                                        PeerTupleNode tuple_n_inside_l = rebase_tuple_node(tuple_n, l);
                                        PeerTupleNode respondant_inside_l = rebase_tuple_node(respondant, l);
                                        if (message_routing.dist(h_p_k, tuple_n_inside_l) < message_routing.dist(h_p_k, respondant_inside_l))
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
                    t_respondant = tuple_node_to_tuple_gnode(respondant);
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
        ttl_db_on_request(ITemporalDatabaseDescriptor tdd, IPeersRequest r, int common_lvl)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            if (tdd.dh == null || ! tdd.dh.ready)
                throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not even started");
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
                        IPeersResponse res = tdd.execute(r);
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
                            ttl_db_start_retrieve(tdd, k);
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
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
                    return tdd.execute(r);
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return tdd.prepare_response_not_found(r);
                }
                else
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                }
            }
            if (r is RequestWaitThenSendRecord)
            {
                Object k = ((RequestWaitThenSendRecord)r).k;
                if ((! tdd.my_records_contains(k)) && (! ttl_db_is_exhaustive(tdd, k)))
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                }
                int delta = eval_coherence_delta(map_paths.i_peers_get_nodes_in_my_group(common_lvl));
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
            if (tdd.is_update_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.execute(r);
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
                            ttl_db_start_retrieve(tdd, k);
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
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
                    var res = tdd.execute(r);
                    assert(tdd.my_records_contains(k));
                    return res;
                }
                if (! ttl_db_is_out_of_memory(tdd))
                {
                    ttl_db_remove_not_found(tdd, k);
                    ttl_db_remove_not_exhaustive(tdd, k);
                    var res = tdd.execute(r);
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
                var res = tdd.execute(r);
                assert(! tdd.my_records_contains(k));
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_add_not_found(tdd, k);
                return res;
            }
            // none of previous cases
            return tdd.execute(r);
        }
        private int eval_coherence_delta(int num_nodes)
        {
            if (num_nodes < 50) return 2000;
            if (num_nodes < 500) return 20000;
            if (num_nodes < 5000) return 30000;
            return 50000;
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
            public PeersManager t;
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
            Object? record = null;
            IPeersRequest r = new RequestWaitThenSendRecord(k);
            try {
                PeerTupleNode respondant;
                PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k));
                debug(@"starting contact_peer for a request of wait_then_send_record (Key is a $(k.get_type().name())).\n");
                IPeersResponse res = contact_peer(tdd.dh.p_id, h_p_k, r, RequestWaitThenSendRecord.timeout_exec, true, out respondant);
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

        public void fixed_keys_db_on_startup(IFixedKeysDatabaseDescriptor fkdd, int p_id, int level_new_gnode)
        {
            assert(services.has_key(p_id));
            FixedKeysDbOnStartupTasklet ts = new FixedKeysDbOnStartupTasklet();
            ts.t = this;
            ts.fkdd = fkdd;
            ts.p_id = p_id;
            ts.level_new_gnode = level_new_gnode;
            tasklet.spawn(ts);
        }
        private class FixedKeysDbOnStartupTasklet : Object, ITaskletSpawnable
        {
            public PeersManager t;
            public IFixedKeysDatabaseDescriptor fkdd;
            public int p_id;
            public int level_new_gnode;
            public void * func()
            {
                debug("starting tasklet_fixed_keys_db_on_startup.\n");
                t.tasklet_fixed_keys_db_on_startup(fkdd, p_id, level_new_gnode); 
                debug("ending tasklet_fixed_keys_db_on_startup.\n");
                return null;
            }
        }
        private void tasklet_fixed_keys_db_on_startup(IFixedKeysDatabaseDescriptor fkdd, int p_id, int level_new_gnode)
        {
            PeerService srv = services[p_id];
            fkdd.dh = new DatabaseHandler();
            fkdd.dh.p_id = p_id;
            fkdd.dh.ready = false;
            if (srv.p_is_optional)
            {
                // TODO search a mechanism to validate the situation
                //   where maps_retrieved_below_level < levels.
                if (maps_retrieved_below_level < levels)
                {
                    error("not implemented yet");
                }
            }
            fkdd.dh.not_completed_keys = new ArrayList<Object>(fkdd.key_equal_data);
            fkdd.dh.retrieving_keys = new HashMap<Object, IChannel>(fkdd.key_hash_data, fkdd.key_equal_data);
            Gee.List<Object> k_set = fkdd.get_full_key_domain();
            fkdd.dh.not_completed_keys.add_all(k_set);
            fkdd.dh.ready = true;
            bool wait_before_network_activity = false;
            debug("database handler is ready.\n");
            foreach (Object k in k_set)
            {
                int l = fkdd.evaluate_hash_node(k).size;
                if (level_new_gnode >= l)
                {
                    fkdd.set_record_for_key(k, fkdd.get_default_record_for_key(k));
                    fkdd.dh.not_completed_keys.remove(k);
                }
                else
                {
                    if (wait_before_network_activity) tasklet.ms_wait(200);
                    fixed_keys_db_start_retrieve(fkdd, k);
                    wait_before_network_activity = true;
                }
            }
        }

        public IPeersResponse
        fixed_keys_db_on_request(IFixedKeysDatabaseDescriptor fkdd, IPeersRequest r, int common_lvl)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            if (fkdd.dh == null || ! fkdd.dh.ready)
                throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not even started");
            if (fkdd.is_read_only_request(r))
            {
                Object k = fkdd.get_key_from_request(r);
                if (! (k in fkdd.dh.not_completed_keys))
                {
                    assert(fkdd.my_records_contains(k));
                    return fkdd.execute(r);
                }
                else
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                }
            }
            if (r is RequestWaitThenSendRecord)
            {
                Object k = ((RequestWaitThenSendRecord)r).k;
                if (k in fkdd.dh.not_completed_keys)
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                assert(fkdd.my_records_contains(k));
                int delta = eval_coherence_delta(map_paths.i_peers_get_nodes_in_my_group(common_lvl));
                if (delta > RequestWaitThenSendRecord.timeout_exec - 1000)
                    delta = RequestWaitThenSendRecord.timeout_exec - 1000;
                tasklet.ms_wait(delta);
                assert(fkdd.my_records_contains(k));
                return new RequestWaitThenSendRecordResponse(fkdd.get_record_for_key(k));
            }
            if (fkdd.is_update_request(r))
            {
                Object k = fkdd.get_key_from_request(r);
                if (! (k in fkdd.dh.not_completed_keys))
                {
                    assert(fkdd.my_records_contains(k));
                    return fkdd.execute(r);
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
                return fkdd.execute(r);
            }
            // none of previous cases
            return fkdd.execute(r);
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
            public PeersManager t;
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
            Object? record = null;
            IPeersRequest r = new RequestWaitThenSendRecord(k);
            int timeout_exec = RequestWaitThenSendRecord.timeout_exec;
            while (true)
            {
                try {
                    PeerTupleNode respondant;
                    PeerTupleNode h_p_k = new PeerTupleNode(fkdd.evaluate_hash_node(k));
                    debug(@"starting contact_peer for a request of wait_then_send_record (Key is a $(k.get_type().name())).\n");
                    IPeersResponse res = contact_peer(fkdd.dh.p_id, h_p_k, r, timeout_exec, true, out respondant);
                    if (res is RequestWaitThenSendRecordResponse)
                        record = ((RequestWaitThenSendRecordResponse)res).record;
                    break;
                } catch (PeersNoParticipantsInNetworkError e) {
                    break;
                } catch (PeersDatabaseError e) {
                    tasklet.ms_wait(200);
                }
            }
            if (record != null && fkdd.is_valid_record(k, record))
            {
                fkdd.set_record_for_key(k, record);
            }
            else
            {
                fkdd.set_record_for_key(k, fkdd.get_default_record_for_key(k));
            }
            IChannel temp_ch = fkdd.dh.retrieving_keys[k];
            fkdd.dh.retrieving_keys.unset(k);
            while (temp_ch.get_balance() < 0) temp_ch.send_async(0);
            fkdd.dh.not_completed_keys.remove(k);
        }

        /* Remotable methods */

        public IPeerParticipantSet get_participant_set
        (int lvl, CallerInfo? _rpc_caller=null)
        throws PeersInvalidRequest
        {
            error("deprecated");
            /*
            // check payload
            if (lvl <= 0 || lvl > levels) throw new PeersInvalidRequest.GENERIC("level out of range");
            // begin
            PeerParticipantSet ret = new PeerParticipantSet();
            foreach (int p_id in participant_maps.keys)
            {
                PeerParticipantMap map = new PeerParticipantMap();
                PeerParticipantMap my_map = participant_maps[p_id];
                bool participation_at_lvl = false;
                foreach (HCoord hc in my_map.participant_list)
                {
                    if (hc.lvl >= lvl) map.participant_list.add(hc);
                    else participation_at_lvl = true;
                }
                if (participation_at_lvl) map.participant_list.add(new HCoord(lvl, pos[lvl]));
                ret.participant_set[p_id] = map;
            }
            return ret;
            */
        }

        public IPeerParticipantSet ask_participant_maps
        (CallerInfo? _rpc_caller=null)
        {
            error("not yet implemented");
        }

        public void give_participant_maps
        (IPeerParticipantSet maps, CallerInfo? _rpc_caller=null)
        {
            error("not yet implemented");
        }

        public void forward_peer_message
        (IPeerMessage peer_message, CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (peer_message is PeerMessageForwarder)) return;
            PeerMessageForwarder mf = (PeerMessageForwarder) peer_message;
            if (! check_valid_message(mf)) return;
            if (_rpc_caller == null) return;
            // prepare
            bool optional = false;
            if (services.has_key(mf.p_id))
                optional = services[mf.p_id].p_is_optional;
            else
                optional = true;
            // call message_routing
            message_routing.forward_msg(mf, optional, maps_retrieved_below_level, _rpc_caller);
            // if optional, check non_participant_tuple_list
            if (optional && participant_maps.has_key(mf.p_id))
            {
                foreach (PeerTupleGNode t in mf.non_participant_tuple_list)
                {
                    int @case;
                    HCoord ret;
                    convert_tuple_gnode(t, out @case, out ret);
                    if (@case == 2)
                    {
                        if (ret in participant_maps[mf.p_id].participant_list)
                        {
                            CheckNonParticipationTasklet ts = new CheckNonParticipationTasklet();
                            ts.t = this;
                            ts.ret = ret;
                            ts.p_id = mf.p_id;
                            tasklet.spawn(ts);
                        }
                    }
                }
            }
        }
        private class CheckNonParticipationTasklet : Object, ITaskletSpawnable
        {
            public PeersManager t;
            public HCoord ret;
            public int p_id;
            public void * func()
            {
                t.check_non_participation_tasklet(ret, p_id);
                return null;
            }
        }
        internal void check_non_participation_tasklet(HCoord ret, int p_id)
        {
            if (message_routing.check_non_participation(ret, p_id))
                if (participant_maps.has_key(p_id))
                    participant_maps[p_id].participant_list.remove(ret);
        }

        public IPeersRequest get_request
        (int msg_id, IPeerTupleNode _respondant, CallerInfo? _rpc_caller=null)
        throws PeersUnknownMessageError, PeersInvalidRequest
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.get_request: ignored because unknown msg_id");
                throw new PeersUnknownMessageError.GENERIC("unknown msg_id");
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.get_request: ignored because unknown class as IPeerTupleNode");
                throw new PeersInvalidRequest.GENERIC("unknown class as IPeerTupleNode");
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must be inside my search g-node
            if (wa.min_target.top != respondant.top)
            {
                debug("PeersManager.get_request: ignored because not same g-node of research");
                throw new PeersInvalidRequest.GENERIC("not same g-node of research");
            }
            // ok
            wa.respondant_node = respondant;
            wa.ch.send_async(0);
            // might be a fake request
            if (wa.request == null) throw new PeersUnknownMessageError.GENERIC("was a fake request");
            else return wa.request;
        }

        public void set_response
        (int msg_id, IPeersResponse response, IPeerTupleNode _respondant, CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_response: ignored because unknown msg_id");
                return;
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.set_response: ignored because unknown class as IPeerTupleNode");
                return;
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            bool mismatch = false;
            if (wa.respondant_node == null) mismatch = true;
            else if (wa.respondant_node.tuple.size != respondant.tuple.size) mismatch = true;
            else
            {
                for (int j = 0; j < respondant.tuple.size; j++)
                {
                    if (respondant.tuple[j] != wa.respondant_node.tuple[j])
                    {
                        mismatch = true;
                        break;
                    }
                }
            }
            if (mismatch)
            {
                debug("PeersManager.set_response: ignored because did not send request to that node");
                return;
            }
            wa.response = response;
            wa.ch.send_async(0);
        }

        public void set_refuse_message
        (int msg_id, string refuse_message, IPeerTupleNode _respondant, CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_refuse_message: ignored because unknown msg_id");
                return;
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.set_refuse_message: ignored because unknown class as IPeerTupleNode");
                return;
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            bool mismatch = false;
            if (wa.respondant_node == null) mismatch = true;
            else if (wa.respondant_node.tuple.size != respondant.tuple.size) mismatch = true;
            else
            {
                for (int j = 0; j < respondant.tuple.size; j++)
                {
                    if (respondant.tuple[j] != wa.respondant_node.tuple[j])
                    {
                        mismatch = true;
                        break;
                    }
                }
            }
            if (mismatch)
            {
                debug("PeersManager.set_refuse_message: ignored because did not send request to that node");
                return;
            }
            wa.refuse_message = refuse_message;
            debug(@"PeersManager.set_refuse_message: $(refuse_message)");
            wa.ch.send_async(0);
        }

        public void set_redo_from_start
        (int msg_id, IPeerTupleNode _respondant, CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_redo_from_start: ignored because unknown msg_id");
                return;
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.set_redo_from_start: ignored because unknown class as IPeerTupleNode");
                return;
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            bool mismatch = false;
            if (wa.respondant_node == null) mismatch = true;
            else if (wa.respondant_node.tuple.size != respondant.tuple.size) mismatch = true;
            else
            {
                for (int j = 0; j < respondant.tuple.size; j++)
                {
                    if (respondant.tuple[j] != wa.respondant_node.tuple[j])
                    {
                        mismatch = true;
                        break;
                    }
                }
            }
            if (mismatch)
            {
                debug("PeersManager.set_redo_from_start: ignored because did not send request to that node");
                return;
            }
            wa.redo_from_start = true;
            wa.ch.send_async(0);
        }

        public void set_next_destination
        (int msg_id, IPeerTupleGNode _tuple, CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_next_destination: ignored because unknown msg_id");
                return;
            }
            if (! (_tuple is PeerTupleGNode))
            {
                debug("PeersManager.set_next_destination: ignored because unknown class as IPeerTupleGNode");
                return;
            }
            PeerTupleGNode tuple = (PeerTupleGNode)_tuple;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must maintain the smallest value k
            if (wa.min_target.top != tuple.top)
            {
                debug("PeersManager.set_next_destination: ignored because not same g-node of research");
                return;
            }
            int old_k = wa.min_target.top - wa.min_target.tuple.size;
            if (tuple.top - tuple.tuple.size >= old_k)
            {
                debug("PeersManager.set_next_destination: ignored because already reached a lower level");
                return;
            }
            wa.min_target = tuple;
            wa.ch.send_async(0);
        }

        public void set_failure
        (int msg_id, IPeerTupleGNode _tuple, CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_failure: ignored because unknown msg_id");
                return;
            }
            if (! (_tuple is PeerTupleGNode))
            {
                debug("PeersManager.set_failure: ignored because unknown class as IPeerTupleGNode");
                return;
            }
            PeerTupleGNode tuple = (PeerTupleGNode)_tuple;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must be lower than the smallest value k
            if (wa.min_target.top != tuple.top)
            {
                debug("PeersManager.set_failure: ignored because not same g-node of research");
                return;
            }
            int old_k = wa.min_target.top - wa.min_target.tuple.size;
            if (tuple.top - tuple.tuple.size > old_k)
            {
                debug("PeersManager.set_failure: ignored because already reached a lower level");
                return;
            }
            wa.exclude_gnode = tuple;
            wa.ch.send_async(0);
        }

        public void set_non_participant
        (int msg_id, IPeerTupleGNode _tuple, CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_non_participant: ignored because unknown msg_id");
                return;
            }
            if (! (_tuple is PeerTupleGNode))
            {
                debug("PeersManager.set_non_participant: ignored because unknown class as IPeerTupleGNode");
                return;
            }
            PeerTupleGNode tuple = (PeerTupleGNode)_tuple;
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must be lower than the smallest value k
            if (wa.min_target.top != tuple.top)
            {
                debug("PeersManager.set_non_participant: ignored because not same g-node of research");
                return;
            }
            int old_k = wa.min_target.top - wa.min_target.tuple.size;
            if (tuple.top - tuple.tuple.size > old_k)
            {
                debug("PeersManager.set_non_participant: ignored because already reached a lower level");
                return;
            }
            wa.non_participant_gnode = tuple;
            wa.ch.send_async(0);
        }

        public void set_missing_optional_maps
        (int msg_id, CallerInfo? _rpc_caller=null)
        {
            error("not yet implemented");
        }

        public void set_participant
        (int p_id, IPeerTupleGNode tuple, CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (tuple is PeerTupleGNode)) return;
            PeerTupleGNode gn = (PeerTupleGNode) tuple;
            if (! check_valid_tuple_gnode(gn)) return;
            // begin
            if (services.has_key(p_id) && ! services[p_id].p_is_optional) return;
            map_handler.set_participant(p_id, gn);
        }
    }

    public abstract class PeerService : Object
    {
        public int p_id {get; private set;}
        public bool p_is_optional {get; private set;}
        public PeerService(int p_id, bool p_is_optional)
        {
            this.p_id = p_id;
            this.p_is_optional = p_is_optional;
        }

        public abstract IPeersResponse
        exec(IPeersRequest req, Gee.List<int> client_tuple)
        throws PeersRefuseExecutionError, PeersRedoFromStartError;
    }

    public abstract class PeerClient : Object
    {
        protected ArrayList<int> gsizes;
        private int p_id;
        private PeersManager peers_manager;
        public PeerClient(int p_id, Gee.List<int> gsizes, PeersManager peers_manager)
        {
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            this.p_id = p_id;
            this.peers_manager = peers_manager;
        }

        protected abstract uint64 hash_from_key(Object k, uint64 top);

        public virtual Gee.List<int> perfect_tuple(Object k)
        {
            uint64 top = 1;
            foreach (int gsize in gsizes) top *= gsize;
            uint64 hash = hash_from_key(k, top-1);
            assert(hash < top);
            ArrayList<int> tuple = new ArrayList<int>();
            foreach (int gsize in gsizes)
            {
                uint64 pos = hash % gsize;
                tuple.add((int)pos);
                hash /= gsize;
            }
            return tuple;
        }

        protected IPeersResponse call(Object k, IPeersRequest request, int timeout_exec)
        throws PeersNoParticipantsInNetworkError, PeersDatabaseError
        {
            PeerTupleNode respondant;
            debug(@"starting contact_peer for a specific request from $(get_type().name()) (Key is a $(k.get_type().name())).\n");
            return
            peers_manager.contact_peer
                (p_id,
                 new PeerTupleNode(perfect_tuple(k)),
                 request,
                 timeout_exec,
                 false,
                 out respondant);
        }
    }
}
