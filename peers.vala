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
        private Databases.Databases databases;

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
            message_routing.gnode_is_not_participating.connect((/*HCoord*/ g, /*int*/ p_id) => {
                if (participant_maps.has_key(p_id))
                    participant_maps[p_id].participant_list.remove(g);
            });

            databases = new Databases.Databases
                (new ArrayList<int>.wrap(pos), new ArrayList<int>.wrap(gsizes),
                 /* contact_peer     = */  (/*int*/ p_id,
                                            /*PeerTupleNode*/ x_macron,
                                            /*IPeersRequest*/ request,
                                            /*int*/ timeout_exec,
                                            /*bool*/ exclude_myself,
                                            out /*PeerTupleNode?*/ respondant,
                                            /*PeerTupleGNodeContainer?*/ exclude_tuple_list) => {
                     // Call method of message_routing.
                     bool optional = is_service_optional(p_id);
                     if (optional) wait_participation_maps(x_macron.tuple.size);
                     return message_routing.contact_peer
                         (p_id,
                          optional,
                          x_macron,
                          request,
                          timeout_exec,
                          exclude_myself,
                          out respondant,
                          exclude_tuple_list);
                     // Done.
                 },
                 /* assert_service_registered = */  (/*int*/ p_id) => {
                     assert(services.has_key(p_id));
                 },
                 /* is_service_optional       = */  (/*int*/ p_id) => {
                     return is_service_optional(p_id);
                 },
                 /* wait_participation_maps   = */  (/*int*/ target_levels) => {
                     wait_participation_maps(target_levels);
                 },
                 /* compute_dist              = */  (/*PeerTupleNode*/ x_macron,
                                                      /*PeerTupleNode*/ x) => {
                     return message_routing.dist(x_macron, x);
                 },
                 /* get_nodes_in_my_group     = */  (/*int*/ lvl) => {
                     return map_paths.i_peers_get_nodes_in_my_group(lvl);
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

        public bool begin_replica
                (int q,
                 int p_id,
                 Gee.List<int> perfect_tuple,
                 IPeersRequest r,
                 int timeout_exec,
                 out IPeersResponse? resp,
                 out IPeersContinuation cont)
        {
            return databases.begin_replica
                (q, p_id, perfect_tuple, r, timeout_exec, out resp, out cont);
        }

        public bool next_replica(IPeersContinuation cont, out IPeersResponse? resp)
        {
            return databases.next_replica
                (cont, out resp);
        }

        public void ttl_db_on_startup
        (ITemporalDatabaseDescriptor tdd, int p_id,
         int guest_gnode_level, int new_gnode_level, ITemporalDatabaseDescriptor? prev_id_tdd)
        {
            databases.ttl_db_on_startup
                (tdd, p_id, guest_gnode_level, new_gnode_level, prev_id_tdd);
        }

        public IPeersResponse
        ttl_db_on_request(ITemporalDatabaseDescriptor tdd, IPeersRequest r, int common_lvl)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            return databases.ttl_db_on_request(tdd, r, common_lvl);
        }

        public void fixed_keys_db_on_startup
        (IFixedKeysDatabaseDescriptor fkdd, int p_id,
         int guest_gnode_level, int new_gnode_level, IFixedKeysDatabaseDescriptor? prev_id_fkdd)
        {
            databases.fixed_keys_db_on_startup
                (fkdd, p_id, guest_gnode_level, new_gnode_level, prev_id_fkdd);
        }

        public IPeersResponse
        fixed_keys_db_on_request(IFixedKeysDatabaseDescriptor fkdd, IPeersRequest r, int common_lvl)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            return databases.fixed_keys_db_on_request(fkdd, r, common_lvl);
        }

        /* Remotable methods */

        public IPeerParticipantSet ask_participant_maps
        (CallerInfo? _rpc_caller=null)
        {
            return map_handler.produce_maps_below_level(maps_retrieved_below_level);
        }

        public void give_participant_maps
        (IPeerParticipantSet maps, CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (maps is PeerParticipantSet)) tasklet.exit_tasklet();
            PeerParticipantSet _maps = (PeerParticipantSet) maps;
            // begin
            map_handler.give_participant_maps(_maps);
        }

        public void forward_peer_message
        (IPeerMessage peer_message, CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (peer_message is PeerMessageForwarder)) tasklet.exit_tasklet();
            PeerMessageForwarder mf = (PeerMessageForwarder) peer_message;
            if (! check_valid_message(mf)) tasklet.exit_tasklet();
            if (_rpc_caller == null) tasklet.exit_tasklet();
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
        (int msg_id, IPeerTupleNode respondant, CallerInfo? _rpc_caller=null)
        throws PeersUnknownMessageError, PeersInvalidRequest
        {
            // check that interfaces are ok
            if (!(respondant is PeerTupleNode))
            {
                warning("bad request rpc: get_request, invalid respondant.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            return
                message_routing.get_request
                (msg_id, (PeerTupleNode)respondant);
            // Done.
        }

        public void set_response
        (int msg_id, IPeersResponse response, IPeerTupleNode respondant, CallerInfo? _rpc_caller=null)
        {
            // check that interfaces are ok
            if (!(respondant is PeerTupleNode))
            {
                warning("bad request rpc: set_response, invalid respondant.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_response(msg_id, response, (PeerTupleNode)respondant);
            // Done.
        }

        public void set_refuse_message
        (int msg_id, string refuse_message, IPeerTupleNode respondant, CallerInfo? _rpc_caller=null)
        {
            // check that interfaces are ok
            if (!(respondant is PeerTupleNode))
            {
                warning("bad request rpc: set_refuse_message, invalid respondant.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_refuse_message(msg_id, refuse_message, (PeerTupleNode)respondant);
            // Done.
        }

        public void set_redo_from_start
        (int msg_id, IPeerTupleNode respondant, CallerInfo? _rpc_caller=null)
        {
            // check that interfaces are ok
            if (!(respondant is PeerTupleNode))
            {
                warning("bad request rpc: set_redo_from_start, invalid respondant.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_redo_from_start(msg_id, (PeerTupleNode)respondant);
            // Done.
        }

        public void set_next_destination
        (int msg_id, IPeerTupleGNode tuple, CallerInfo? _rpc_caller=null)
        {
            // check that interfaces are ok
            if (! (tuple is PeerTupleGNode))
            {
                warning("bad request rpc: set_next_destination, invalid tuple.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_next_destination(msg_id, (PeerTupleGNode)tuple);
            // Done.
        }

        public void set_failure
        (int msg_id, IPeerTupleGNode tuple, CallerInfo? _rpc_caller=null)
        {
            // check that interfaces are ok
            if (! (tuple is PeerTupleGNode))
            {
                warning("bad request rpc: set_failure, invalid tuple.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_failure(msg_id, (PeerTupleGNode)tuple);
            // Done.
        }

        public void set_non_participant
        (int msg_id, IPeerTupleGNode tuple, CallerInfo? _rpc_caller=null)
        {
            // check that interfaces are ok
            if (! (tuple is PeerTupleGNode))
            {
                warning("bad request rpc: set_non_participant, invalid tuple.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_non_participant(msg_id, (PeerTupleGNode)tuple);
            // Done.
        }

        public void set_missing_optional_maps
        (int msg_id, CallerInfo? _rpc_caller=null)
        {
            // Call method of message_routing.
            message_routing.set_missing_optional_maps(msg_id);
            // Done.
        }

        public void set_participant
        (int p_id, IPeerTupleGNode tuple, CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (tuple is PeerTupleGNode)) tasklet.exit_tasklet();
            PeerTupleGNode gn = (PeerTupleGNode) tuple;
            if (! check_valid_tuple_gnode(gn)) tasklet.exit_tasklet();
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
