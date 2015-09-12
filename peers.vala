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
using Netsukuku.ModRpc;
using zcd.ModRpc;
using LibPeersInternals;

namespace Netsukuku
{
    public errordomain PeersNonexistentDestinationError {
        GENERIC
    }

    public errordomain PeersNonexistentFellowError {
        GENERIC
    }

    public errordomain PeersNoParticipantsInNetworkError {
        GENERIC
    }

    public interface IPeersMapPaths : Object
    {
        public abstract int i_peers_get_levels();
        public abstract int i_peers_get_gsize(int level);
        public abstract int i_peers_get_nodes_in_my_group(int level);
        public abstract int i_peers_get_my_pos(int level);
        public abstract bool i_peers_exists(int level, int pos);
        public abstract IPeersManagerStub i_peers_gateway
            (int level, int pos,
             zcd.ModRpc.CallerInfo? received_from=null,
             IPeersManagerStub? failed=null)
            throws PeersNonexistentDestinationError;
        public abstract IPeersManagerStub i_peers_fellow(int level)
            throws PeersNonexistentFellowError;
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

    internal class PeerTupleNode : Object, Json.Serializable, IPeerTupleNode
    {
        public Gee.List<int> tuple {get; set;}
        public int top {get {return tuple.size;}}
        public PeerTupleNode(Gee.List<int> tuple)
        {
            this.tuple = new ArrayList<int>();
            this.tuple.add_all(tuple);
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "top":
                // get-only dynamic property
                return false;
            case "tuple":
                try {
                    @value = deserialize_list_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "top":
                return serialize_int(0); // get-only dynamic property
            case "tuple":
                return serialize_list_int((Gee.List<int>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class PeerTupleGNode : Object, Json.Serializable, IPeerTupleGNode
    {
        public Gee.List<int> tuple {get; set;}
        public int top {get; set;}
        public PeerTupleGNode(Gee.List<int> tuple, int top)
        {
            this._tuple = new ArrayList<int>();
            this._tuple.add_all(tuple);
            this.top = top;
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "tuple":
                try {
                    @value = deserialize_list_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "top":
                try {
                    @value = deserialize_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "tuple":
                return serialize_list_int((Gee.List<int>)@value);
            case "top":
                return serialize_int((int)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class PeerTupleGNodeContainer : Object
    {
        private ArrayList<PeerTupleGNode> _list;
        public Gee.List<PeerTupleGNode> list {
            owned get {
                return _list.read_only_view;
            }
        }
        public int top {get; private set;}

        public PeerTupleGNodeContainer(int top)
        {
            this.top = top;
            _list = new ArrayList<PeerTupleGNode>();
        }

        private int get_rev_pos(PeerTupleGNode g, int j)
        {
            // g.tuple has positions for levels from ε >= 0 to g.top-1.
            //  e.g.:
            //     g.top = 5
            //       that is, the g-node h that we live inside is at level 5.
            //     g.tuple.size = 3
            //     g.tuple = [2,1,3]
            //       that is g is g-node 3.1.2 inside h
            //       epsilon is 2, position for 2 is 2, position for 3 is 1, position for 4 is 3
            // get position for top-1-j.
            return g.tuple[g.tuple.size-1 - j];
        }

        public void add(PeerTupleGNode g)
        {
            assert(g.top == top);
            // If g is already contained in the list, return.
            if (contains(g)) return;
            // Cycle the list:
            int i = 0;
            while (i < _list.size)
            {
                PeerTupleGNode e = _list[i];
                assert(e.top == g.top);
                if (e.tuple.size > g.tuple.size)
                {
                    // If a gnode which is inside g is in the list, remove it.
                    bool mismatch = false;
                    for (int j = 0; j < g.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(g,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        _list.remove_at(i);
                        i--;
                    }
                }
                i++;
            }
            // Then add g.
            _list.add(g);
        }

        private bool contains(PeerTupleGNode g)
        {
            // Cycle the list:
            foreach (PeerTupleGNode e in _list)
            {
                assert(e.top == g.top);
                if (e.tuple.size <= g.tuple.size)
                {
                    // If g is already in the list, return true.
                    bool mismatch = false;
                    for (int j = 0; j < e.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(g,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    internal class PeerMessageForwarder : Object, Json.Serializable, IPeerMessage
    {
        public PeerTupleNode n {get; set;}
        public PeerTupleNode? x_macron {get; set;}
        public int lvl {get; set;}
        public int pos {get; set;}
        public int p_id {get; set;}
        public int msg_id {get; set;}
        public Gee.List<PeerTupleGNode> exclude_tuple_list {get; set;}
        public Gee.List<PeerTupleGNode> non_participant_tuple_list {get; set;}
        public bool reverse {get; set;}

        public PeerMessageForwarder()
        {
            exclude_tuple_list = new ArrayList<PeerTupleGNode>();
            non_participant_tuple_list = new ArrayList<PeerTupleGNode>();
            reverse = false;
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "n":
                try {
                    @value = deserialize_peer_tuple_node(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "x-macron":
            case "x_macron":
                try {
                    @value = deserialize_nullable_peer_tuple_node(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "lvl":
            case "pos":
            case "p-id":
            case "p_id":
            case "msg-id":
            case "msg_id":
                try {
                    @value = deserialize_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "exclude-tuple-list":
            case "exclude_tuple_list":
                try {
                    @value = deserialize_list_peer_tuple_gnode(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "non_participant_tuple_list":
            case "non-participant-tuple-list":
                try {
                    @value = deserialize_list_peer_tuple_gnode(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "reverse":
                try {
                    @value = deserialize_bool(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "n":
                return serialize_peer_tuple_node((PeerTupleNode)@value);
            case "x-macron":
            case "x_macron":
                return serialize_nullable_peer_tuple_node((PeerTupleNode?)@value);
            case "lvl":
            case "pos":
            case "p-id":
            case "p_id":
            case "msg-id":
            case "msg_id":
                return serialize_int((int)@value);
            case "exclude-tuple-list":
            case "exclude_tuple_list":
                return serialize_list_peer_tuple_gnode((Gee.List<PeerTupleGNode>)@value);
            case "non_participant_tuple_list":
            case "non-participant-tuple-list":
                return serialize_list_peer_tuple_gnode((Gee.List<PeerTupleGNode>)@value);
            case "reverse":
                return serialize_bool((bool)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class WaitingAnswer : Object
    {
        public INtkdChannel ch;
        public IPeersRequest? request;
        public PeerTupleGNode min_target;
        public PeerTupleGNode? exclude_gnode;
        public PeerTupleGNode? non_participant_gnode;
        public PeerTupleNode? respondant_node;
        public IPeersResponse? response;
        public WaitingAnswer(IPeersRequest? request, PeerTupleGNode min_target)
        {
            ch = tasklet.get_channel();
            this.request = request;
            this.min_target = min_target;
            exclude_gnode = null;
            non_participant_gnode = null;
            respondant_node = null;
            response = null;
        }
    }

    internal class PeerParticipantMap : Object, Json.Serializable
    {
        public Gee.List<HCoord> participant_list {get; set;}

        public PeerParticipantMap()
        {
            participant_list = new ArrayList<HCoord>((a,b) => a.equals(b));
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "participant-list":
            case "participant_list":
                try {
                    @value = deserialize_list_hcoord(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "participant-list":
            case "participant_list":
                return serialize_list_hcoord((Gee.List<HCoord>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class PeerParticipantSet : Object, Json.Serializable, IPeerParticipantSet
    {
        public HashMap<int, PeerParticipantMap> participant_set {get; set;}

        public PeerParticipantSet()
        {
            participant_set = new HashMap<int, PeerParticipantMap>();
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "participant-set":
            case "participant_set":
                try {
                    @value = deserialize_map_int_peer_participant_map(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "participant_set":
            case "participant-set":
                return serialize_map_int_peer_participant_map((HashMap<int, PeerParticipantMap>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal INtkdTasklet tasklet;
    public class PeersManager : Object,
                                IPeersManagerSkeleton
    {
        public static void init(INtkdTasklet _tasklet)
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

        // The maps of participants are now ready.
        public signal void participant_maps_ready();

        public PeersManager
            (IPeersMapPaths map_paths,
             int level_new_gnode,
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
            if (level_new_gnode < levels)
            {
                RetrieveParticipantSetTasklet ts = new RetrieveParticipantSetTasklet();
                ts.t = this;
                ts.lvl = level_new_gnode + 1;
                tasklet.spawn(ts);
            }
        }

        private class RetrieveParticipantSetTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public int lvl;
            public void * func()
            {
                t.retrieve_participant_set(lvl);
                t.participant_maps_ready();
                return null;
            }
        }
        private void retrieve_participant_set(int lvl)
        {
            IPeersManagerStub stub;
            try {
                stub = map_paths.i_peers_fellow(lvl);
            } catch (PeersNonexistentFellowError e) {
                debug(@"retrieve_participant_set: Failed to get because PeersNonexistentFellowError");
                return;
            }
            IPeerParticipantSet ret;
            try {
                ret = stub.get_participant_set(lvl);
            } catch (PeersInvalidRequest e) {
                debug(@"retrieve_participant_set: Failed to get because PeersInvalidRequest $(e.message)");
                return;
            } catch (zcd.ModRpc.StubError e) {
                debug(@"retrieve_participant_set: Failed to get because StubError $(e.message)");
                return;
            } catch (zcd.ModRpc.DeserializeError e) {
                debug(@"retrieve_participant_set: Failed to get because DeserializeError $(e.message)");
                return;
            }
            if (! (ret is PeerParticipantSet)) {
                debug("retrieve_participant_set: Failed to get because unknown class");
                return;
            }
            PeerParticipantSet participant_set = (PeerParticipantSet)ret;
            // copy
            participant_maps = new HashMap<int, PeerParticipantMap>();
            foreach (int p_id in participant_set.participant_set.keys)
            {
                PeerParticipantMap my_map = new PeerParticipantMap();
                participant_maps[p_id] = my_map;
                PeerParticipantMap map = participant_set.participant_set[p_id];
                foreach (HCoord hc in map.participant_list)
                    my_map.participant_list.add(hc);
            }
        }

        public void register(PeerService p)
        {
            if (services.has_key(p.p_id))
            {
                critical("PeersManager.register: Two services with same ID?");
                return;
            }
            services[p.p_id] = p;
            if (p.p_is_optional)
            {
                PublishMyParticipationTasklet ts = new PublishMyParticipationTasklet();
                ts.t = this;
                ts.p_id = p.p_id;
                tasklet.spawn(ts);
                if (!participant_maps.has_key(p.p_id))
                    participant_maps[p.p_id] = new PeerParticipantMap();
                // save my position
                PeerParticipantMap map = participant_maps[p.p_id];
                map.participant_list.add(new HCoord(0, pos[0]));
            }
            // Each specific service must handle, after registering, the
            //  retrieve of cache by using begin_retrieve and next_retrieve
            //  functions. If is needed it can use implementation of is_ready
            //  to wait for the cache before answering requests.
        }

        /* Helpers */

        private bool check_valid_message(PeerMessageForwarder mf)
        {
            if (! check_valid_tuple_node(mf.n)) return false;
            if (mf.lvl < 0) return false;
            if (mf.lvl >= levels) return false;
            if (mf.pos < 0) return false;
            if (mf.pos >= gsizes[mf.lvl]) return false;
            if (mf.n.tuple.size <= mf.lvl) return false;
            if (mf.x_macron != null)
            {
                if (! check_valid_tuple_node(mf.x_macron)) return false;
                if (mf.x_macron.tuple.size != mf.lvl) return false;
            }
            if (! mf.exclude_tuple_list.is_empty)
            {
                foreach (PeerTupleGNode gn in mf.exclude_tuple_list)
                {
                    if (! check_valid_tuple_gnode(gn)) return false;
                    if (gn.top != mf.lvl) return false;
                }
            }
            if (! mf.non_participant_tuple_list.is_empty)
            {
                PeerTupleGNode gn;
                gn = mf.non_participant_tuple_list[0];
                if (! check_valid_tuple_gnode(gn)) return false;
                if (gn.top <= mf.lvl) return false;
                int first_top = gn.top;
                for (int i = 1; i < mf.non_participant_tuple_list.size; i++)
                {
                    gn = mf.non_participant_tuple_list[i];
                    if (! check_valid_tuple_gnode(gn)) return false;
                    if (gn.top != first_top) return false;
                }
            }
            return true;
        }

        private bool check_valid_tuple_node(PeerTupleNode n)
        {
            if (n.tuple.size == 0) return false;
            if (n.tuple.size > levels) return false;
            for (int i = 0; i < n.tuple.size; i++)
            {
                if (n.tuple[i] < 0) return false;
                if (n.tuple[i] >= gsizes[i]) return false;
            }
            return true;
        }

        private bool check_valid_tuple_gnode(PeerTupleGNode gn)
        {
            if (gn.tuple.size == 0) return false;
            if (gn.top > levels) return false;
            if (gn.tuple.size > gn.top) return false;
            for (int i = 0; i < gn.tuple.size; i++)
            {
                int eps = gn.top - gn.tuple.size;
                if (gn.tuple[i] < 0) return false;
                if (gn.tuple[i] >= gsizes[eps+i]) return false;
            }
            return true;
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

        private Gee.List<HCoord> get_non_participant_gnodes(int p_id)
        {
            // Returns a list of HCoord representing each gnode visible in my topology which, to my
            //  knowledge, do not participate to service p_id
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
                foreach (HCoord lp in get_all_gnodes_up_to_lvl(levels))
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

        private void convert_tuple_gnode(PeerTupleGNode t, out int @case, out HCoord ret)
        {
            /*
            Given t which represents a g-node h of level ε which lives inside one of my g-nodes,
            where ε = t.top - t.tuple.size,
            this methods returns the following informations:

            * int @case
               * Is 1 iff t represents one of my g-nodes.
               * Is 2 iff t represents a g-node visible in my topology.
               * Is 3 iff t represents a g-node not visible in my topology.
            * HCoord ret
               * The g-node that I have in common with h.
               * In case 1  ret.lvl = ε. Also, pos[ret.lvl] = ret.pos.
               * In case 2  ret.lvl = ε. Also, pos[ret.lvl] ≠ ret.pos.
               * In case 3  ret.lvl > ε.
            */
            int lvl = t.top;
            int i = t.tuple.size;
            assert(i > 0);
            assert(i <= lvl);
            bool trovato = false;
            while (true)
            {
                lvl--;
                i--;
                if (pos[lvl] != t.tuple[i])
                {
                    ret = new HCoord(lvl, t.tuple[i]);
                    trovato = true;
                    if (i == 0)
                        @case = 2;
                    else
                        @case = 3;
                    break;
                }
                if (i == 0)
                {
                    ret = new HCoord(lvl, t.tuple[i]);
                    @case = 1;
                    break;
                }
            }
        }

        private PeerTupleGNode make_tuple_gnode(HCoord h, int top)
        {
            // Returns a PeerTupleGNode that represents h inside our g-node of level top. 
            assert(top > h.lvl);
            ArrayList<int> tuple = new ArrayList<int>();
            int i = top;
            while (true)
            {
                i--;
                if (i == h.lvl)
                {
                    tuple.insert(0, h.pos);
                    break;
                }
                else
                {
                    tuple.insert(0, pos[i]);
                }
            }
            return new PeerTupleGNode(tuple, top);
        }

        private PeerTupleNode make_tuple_node(HCoord h, int top)
        {
            // Returns a PeerTupleNode that represents h inside our g-node of level top. Actually h could be a g-node
            //  but the resulting PeerTupleNode is to be used in method 'dist'. Values of positions for indexes less than
            //  h.lvl are not important, they just have to be in ranges, so we set to 0. 
            assert(top > h.lvl);
            ArrayList<int> tuple = new ArrayList<int>();
            int i = top;
            while (i > 0)
            {
                i--;
                if (i > h.lvl)
                    tuple.insert(0, pos[i]);
                else if (i == h.lvl)
                    tuple.insert(0, h.pos);
                else
                    tuple.insert(0, 0);
            }
            return new PeerTupleNode(tuple);
        }

        private PeerTupleGNode rebase_tuple_gnode(PeerTupleGNode t, int new_top)
        {
            // Given t that represents a g-node inside my g-node at level t.top, returns
            //  a PeerTupleGNode with top=new_top that represents the same g-node.
            // Assert that t.top <= new_top.
            assert(t.top <= new_top);
            ArrayList<int> tuple = new ArrayList<int>();
            ArrayList<int> tuple0 = new ArrayList<int>();
            int i = new_top;
            while (true)
            {
                i--;
                if (i >= t.top)
                    tuple.insert(0, pos[i]);
                else
                {
                    tuple0.add_all(t.tuple);
                    tuple0.add_all(tuple);
                    break;
                }
            }
            return new PeerTupleGNode(tuple0, new_top);
        }

        private PeerTupleNode rebase_tuple_node(PeerTupleNode t, int new_top)
        {
            // Given t that represents a node inside my g-node at level t.top, returns
            //  a PeerTupleNode with top=new_top that represents the same node.
            // Assert that t.top <= new_top.
            assert(t.top <= new_top);
            ArrayList<int> tuple = new ArrayList<int>();
            ArrayList<int> tuple0 = new ArrayList<int>();
            int i = new_top;
            while (true)
            {
                i--;
                if (i >= t.top)
                    tuple.insert(0, pos[i]);
                else
                {
                    tuple0.add_all(t.tuple);
                    tuple0.add_all(tuple);
                    break;
                }
            }
            return new PeerTupleNode(tuple0);
        }

        private bool visible_by_someone_inside_my_gnode(PeerTupleGNode t, int lvl)
        {
            // Given a g-node decides if some node inside my g-node of level lvl has
            //  visibility of it.
            //  e.g. I am node 1.2.3.4.5.6
            //       if t represents 1.2.2.0
            //           could be tuple=0,2 top=4
            //           could be tuple=0,2,2 top=5
            //           could be tuple=0,2,2,1 top=6
            //       if lvl=6 answer is true
            //       if lvl=5 answer is true
            //       if lvl=4 answer is true
            //       if lvl=3 answer is false
            //  e.g. I am node 1.2.3.4.5.6
            //       if t represents 1.2.3.0
            //           could be tuple=0 top=3
            //           could be tuple=0,3 top=4
            //           could be tuple=0,3,2 top=5
            //           could be tuple=0,3,2,1 top=6
            //       for any lvl answer would be true
            int eps = t.top - t.tuple.size;
            int l = eps;
            if (l >= lvl-1)
                l = l + 1;
            else
                l = lvl;
            if (t.top <= l)
                return true;
            PeerTupleGNode h = new PeerTupleGNode(t.tuple.slice(l-eps, t.tuple.size), t.top);
            int @case;
            HCoord ret;
            convert_tuple_gnode(h, out @case, out ret);
            if (@case == 1)
                return true;
            return false;
        }

        /* Routing algorithm */

        private int dist(PeerTupleNode x_macron, PeerTupleNode x, bool reverse=false)
        {
            if (reverse) return dist(x, x_macron);
            assert(x_macron.tuple.size == x.tuple.size);
            int distance = 0;
            for (int j = x.tuple.size-1; j >= 0; j--)
            {
                distance *= gsizes[j];
                distance += x.tuple[j] - x_macron.tuple[j];
                if (x_macron.tuple[j] > x.tuple[j])
                    distance += gsizes[j];
            }
            return distance;
        }

        internal HCoord? approximate(PeerTupleNode? x_macron,
                                     Gee.List<HCoord> _exclude_list,
                                     bool reverse=false)
        {
            // Make sure that exclude_list is searchable
            Gee.List<HCoord> exclude_list = new ArrayList<HCoord>((a, b) => {return a.equals(b);});
            exclude_list.add_all(_exclude_list);
            // This function is x=Ht(x̄)
            if (x_macron == null)
            {
                // It's me or nobody
                HCoord x = new HCoord(0, pos[0]);
                if (!(x in exclude_list))
                    return x;
                else
                    return null;
            }
            // The search can be restricted to a certain g-node
            int valid_levels = x_macron.tuple.size;
            assert (valid_levels <= levels);
            HCoord? ret = null;
            int min_distance = -1;
            // for each known g-node other than me
            for (int l = 0; l < valid_levels; l++)
                for (int p = 0; p < gsizes[l]; p++)
                if (pos[l] != p)
                if (map_paths.i_peers_exists(l, p))
            {
                HCoord x = new HCoord(l, p);
                if (!(x in exclude_list))
                {
                    PeerTupleNode tuple_x = make_tuple_node(x, valid_levels);
                    int distance = dist(x_macron, tuple_x, reverse);
                    if (min_distance == -1 || distance < min_distance)
                    {
                        ret = x;
                        min_distance = distance;
                    }
                }
            }
            // and then for me
            HCoord x = new HCoord(0, pos[0]);
            if (!(x in exclude_list))
            {
                PeerTupleNode tuple_x = make_tuple_node(x, valid_levels);
                int distance = dist(x_macron, tuple_x, reverse);
                if (min_distance == -1 || distance < min_distance)
                {
                    ret = x;
                    min_distance = distance;
                }
            }
            // If null yet, then nobody participates.
            return ret;
        }

        private int find_timeout_routing(int nodes)
        {
            // number of msec to wait for a routing between a group of $(nodes) nodes.
            int ret = 20;
            if (nodes > 100) ret = 200;
            if (nodes > 1000) ret = 2000;
            // TODO explore values
            return ret;
        }

        internal IPeersResponse contact_peer
        (int p_id,
         PeerTupleNode x_macron,
         IPeersRequest request,
         int timeout_exec,
         bool exclude_myself,
         out PeerTupleNode? respondant,
         PeerTupleGNodeContainer? _exclude_tuple_list=null,
         bool reverse=false)
        throws PeersNoParticipantsInNetworkError
        {
            respondant = null;
            int target_levels = x_macron.tuple.size;
            var exclude_gnode_list = new ArrayList<HCoord>();
            bool optional = false;
            if (services.has_key(p_id))
            {
                optional = services[p_id].p_is_optional;
                if (! services[p_id].is_ready())
                    exclude_myself = true;
            }
            else
                optional = true;
            exclude_gnode_list.add_all(get_non_participant_gnodes(p_id));
            if (exclude_myself)
                exclude_gnode_list.add(new HCoord(0, pos[0]));
            PeerTupleGNodeContainer exclude_tuple_list;
            if (_exclude_tuple_list != null)
                exclude_tuple_list = _exclude_tuple_list;
            else
                exclude_tuple_list = new PeerTupleGNodeContainer(target_levels);
            assert(exclude_tuple_list.top == target_levels);
            foreach (PeerTupleGNode gn in exclude_tuple_list.list)
            {
                int @case;
                HCoord ret;
                convert_tuple_gnode(gn, out @case, out ret);
                if (@case == 2)
                    exclude_gnode_list.add(ret);
            }
            PeerTupleGNodeContainer non_participant_tuple_list = new PeerTupleGNodeContainer(target_levels);
            IPeersResponse? response = null;
            while (true)
            {
                HCoord? x = approximate(x_macron, exclude_gnode_list, reverse);
                if (x == null)
                    throw new PeersNoParticipantsInNetworkError.GENERIC("");
                if (x.lvl == 0 && x.pos == pos[0])
                    return services[p_id].exec(request);
                PeerMessageForwarder mf = new PeerMessageForwarder();
                mf.n = make_tuple_node(new HCoord(0, pos[0]), x.lvl+1);
                // That is n0·n1·...·nj, where j = x.lvl
                if (x.lvl == 0)
                    mf.x_macron = null;
                else
                    mf.x_macron = new PeerTupleNode(x_macron.tuple.slice(0, x.lvl));
                    // That is x̄0·x̄1·...·x̄j-1.
                mf.lvl = x.lvl;
                mf.pos = x.pos;
                mf.p_id = p_id;
                mf.msg_id = Random.int_range(0, int.MAX);
                mf.reverse = reverse;
                foreach (PeerTupleGNode t in exclude_tuple_list.list)
                {
                    int @case;
                    HCoord ret;
                    convert_tuple_gnode(t, out @case, out ret);
                    if (@case == 3)
                    {
                        if (ret.equals(x))
                        {
                            int eps = t.top - t.tuple.size;
                            PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-eps), x.lvl);
                            mf.exclude_tuple_list.add(_t);
                        }
                    }
                }
                foreach (PeerTupleGNode t in non_participant_tuple_list.list)
                {
                    if (visible_by_someone_inside_my_gnode(t, x.lvl+1))
                        mf.non_participant_tuple_list.add(t);
                }
                int timeout_routing = find_timeout_routing(map_paths.i_peers_get_nodes_in_my_group(x.lvl+1));
                WaitingAnswer waiting_answer = new WaitingAnswer(request, make_tuple_gnode(x, x.lvl+1));
                waiting_answer_map[mf.msg_id] = waiting_answer;
                IPeersManagerStub gwstub;
                IPeersManagerStub? failed = null;
                bool redo_approximate = false;
                while (true)
                {
                    try {
                        gwstub = map_paths.i_peers_gateway(x.lvl, x.pos, null, failed);
                    } catch (PeersNonexistentDestinationError e) {
                        redo_approximate = true;
                        break;
                    }
                    try {
                        gwstub.forward_peer_message(mf);
                    } catch (zcd.ModRpc.StubError e) {
                        failed = gwstub;
                        continue;
                    } catch (zcd.ModRpc.DeserializeError e) {
                        failed = gwstub;
                        continue;
                    }
                    break;
                }
                if (redo_approximate)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    tasklet.ms_wait(2);
                    continue;
                }
                int timeout = timeout_routing;
                while (true)
                {
                    try {
                        waiting_answer.ch.recv_with_timeout(timeout);
                        if (waiting_answer.exclude_gnode != null)
                        {
                            PeerTupleGNode t = rebase_tuple_gnode(waiting_answer.exclude_gnode, target_levels);
                            // t represents the same g-node of waiting_answer.exclude_gnode, but with top=target_levels
                            int @case;
                            HCoord ret;
                            convert_tuple_gnode(t, out @case, out ret);
                            if (@case == 2)
                            {
                                exclude_gnode_list.add(ret);
                            }
                            exclude_tuple_list.add(t);
                            waiting_answer_map.unset(mf.msg_id);
                            break;
                        }
                        else if (waiting_answer.non_participant_gnode != null)
                        {
                            PeerTupleGNode t = rebase_tuple_gnode(waiting_answer.non_participant_gnode, target_levels);
                            // t represents the same g-node of waiting_answer.non_participant_gnode, but with top=target_levels
                            int @case;
                            HCoord ret;
                            convert_tuple_gnode(t, out @case, out ret);
                            if (@case == 2)
                            {
                                if (optional)
                                {
                                    if (! participant_maps.has_key(p_id))
                                        throw new PeersNoParticipantsInNetworkError.GENERIC("");
                                    PeerParticipantMap map = participant_maps[p_id];
                                    map.participant_list.remove(ret);
                                }
                                exclude_gnode_list.add(ret);
                            }
                            exclude_tuple_list.add(t);
                            non_participant_tuple_list.add(t);
                            waiting_answer_map.unset(mf.msg_id);
                            break;
                        }
                        else if (respondant == null && waiting_answer.respondant_node != null)
                        {
                            respondant = rebase_tuple_node(waiting_answer.respondant_node, target_levels);
                            // respondant represents the same node of waiting_answer.respondant_node, but with top=target_levels
                            timeout = timeout_exec;
                        }
                        else if (waiting_answer.response != null)
                        {
                            response = waiting_answer.response;
                            waiting_answer_map.unset(mf.msg_id);
                            break;
                        }
                        else
                        {
                            // A new destination (min_target) is found, nothing to do.
                        }
                    } catch (NtkdChannelError e) {
                        // TIMEOUT_EXPIRED
                        PeerTupleGNode t = rebase_tuple_gnode(waiting_answer.min_target, target_levels);
                        // t represents the same g-node of waiting_answer.min_target, but with top=target_levels
                        int @case;
                        HCoord ret;
                        convert_tuple_gnode(t, out @case, out ret);
                        if (@case == 2)
                        {
                            exclude_gnode_list.add(ret);
                        }
                        exclude_tuple_list.add(t);
                        waiting_answer_map.unset(mf.msg_id);
                        break;
                    }
                }
                if (response != null)
                    break;
            }
            return response;
        }

        /* Participation publish algorithms */

        private bool check_non_participation(int p_id, int lvl, int _pos)
        {
            // Decide if it's secure to state that lvl,_pos does not participate to service p_id.
            PeerTupleNode? x_macron = null;
            if (lvl > 0)
            {
                ArrayList<int> tuple = new ArrayList<int>();
                for (int i = 0; i < lvl; i++) tuple.add(0);
                x_macron = new PeerTupleNode(tuple);
            }
            PeerTupleNode n = make_tuple_node(new HCoord(0, pos[0]), lvl+1);
            PeerMessageForwarder mf = new PeerMessageForwarder();
            mf.n = n;
            mf.x_macron = x_macron;
            mf.lvl = lvl;
            mf.pos = _pos;
            mf.p_id = p_id;
            mf.msg_id = Random.int_range(0, int.MAX);
            int timeout_routing = find_timeout_routing(map_paths.i_peers_get_nodes_in_my_group(lvl+1));
            WaitingAnswer waiting_answer = new WaitingAnswer(null, make_tuple_gnode(new HCoord(lvl, _pos), lvl+1));
            IPeersManagerStub gwstub;
            IPeersManagerStub? failed = null;
            while (true)
            {
                try {
                    gwstub = map_paths.i_peers_gateway(lvl, _pos, null, failed);
                } catch (PeersNonexistentDestinationError e) {
                    waiting_answer_map.unset(mf.msg_id);
                    return true;
                }
                try {
                    gwstub.forward_peer_message(mf);
                } catch (zcd.ModRpc.StubError e) {
                    failed = gwstub;
                    continue;
                } catch (zcd.ModRpc.DeserializeError e) {
                    failed = gwstub;
                    continue;
                }
                break;
            }
            try {
                waiting_answer.ch.recv_with_timeout(timeout_routing);
                if (waiting_answer.exclude_gnode != null)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
                else if (waiting_answer.non_participant_gnode != null)
                {
                    int @case;
                    HCoord ret;
                    convert_tuple_gnode(waiting_answer.non_participant_gnode, out @case, out ret);
                    if (@case == 2)
                    {
                        waiting_answer_map.unset(mf.msg_id);
                        return true;
                    }
                    else
                    {
                        waiting_answer_map.unset(mf.msg_id);
                        return false;
                    }
                }
                else if (waiting_answer.response != null)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
                else
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
            } catch (NtkdChannelError e) {
                // TIMEOUT_EXPIRED
                waiting_answer_map.unset(mf.msg_id);
                return false;
            }
        }

        class MissingArcSetParticipant : Object, IPeersMissingArcHandler
        {
            public MissingArcSetParticipant(PeersManager mgr, int p_id, PeerTupleGNode tuple)
            {
                this.mgr = mgr;
                this.p_id = p_id;
                this.tuple = tuple;
            }
            private PeersManager mgr;
            private int p_id;
            private PeerTupleGNode tuple;
            public void i_peers_missing(IPeersArc missing_arc)
            {
                IPeersManagerStub stub =
                    mgr.neighbors_factory.i_peers_get_tcp(missing_arc);
                try {
                    stub.set_participant(p_id, tuple);
                } catch (zcd.ModRpc.StubError e) {
                    // ignore
                } catch (zcd.ModRpc.DeserializeError e) {
                    // ignore
                }
            }
        }

        private class PublishMyParticipationTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public int p_id;
            public void * func()
            {
                t.publish_my_participation(p_id);
                return null;
            }
        }
        private void publish_my_participation(int p_id)
        {
            PeerTupleGNode gn = make_tuple_gnode(new HCoord(0, pos[0]), levels);
            int timeout = 300000; // 5 min
            int iterations = 5;
            while (true)
            {
                if (iterations > 0) iterations--;
                else timeout = Random.int_range(24*60*60*1000, 2*24*60*60*1000); // 1 day to 2 days
                MissingArcSetParticipant missing_handler = new MissingArcSetParticipant(this, p_id, gn);
                IPeersManagerStub br_stub = neighbors_factory.i_peers_get_broadcast(missing_handler);
                try {
                    br_stub.set_participant(p_id, gn);
                } catch (zcd.ModRpc.StubError e) {
                    // ignore
                } catch (zcd.ModRpc.DeserializeError e) {
                    // ignore
                }
                tasklet.ms_wait(timeout);
            }
        }

        /* DHT maintainer algorithms */

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
            }
            _cont.replicas.add(respondant);
            PeerTupleGNode respondant_as_gnode = new PeerTupleGNode(respondant.tuple, respondant.tuple.size);
            _cont.exclude_tuple_list.add(respondant_as_gnode);
            return _cont.replicas.size < _cont.q; 
        }

        private class RetrieveCacheContinuation : Object, IPeersContinuation
        {
            public int p_id;
            public IPeersRequest r;
            public int timeout_exec;
            public int j;
            public PeerTupleGNodeContainer exclude_tuple_list;
        }

        public bool begin_retrieve_cache
                (int p_id,
                 IPeersRequest r,
                 int timeout_exec,
                 out IPeersResponse? resp,
                 out IPeersContinuation? cont)
        {
            resp = null;
            cont = null;
            for (int j = 0; j < levels; j++)
            {
                PeerTupleNode x_macron = make_tuple_node(new HCoord(0, pos[0]), j+1);
                PeerTupleGNodeContainer exclude_tuple_list = new PeerTupleGNodeContainer(x_macron.tuple.size);
                PeerTupleNode respondant;
                try {
                    resp = contact_peer
                        (p_id, x_macron, r,
                         timeout_exec, true,
                         out respondant, exclude_tuple_list);
                } catch (PeersNoParticipantsInNetworkError e) {
                    continue;
                }
                PeerTupleGNode respondant_as_gnode = new PeerTupleGNode(respondant.tuple, respondant.tuple.size);
                exclude_tuple_list.add(respondant_as_gnode);
                RetrieveCacheContinuation _cont = new RetrieveCacheContinuation();
                _cont.p_id = p_id;
                _cont.r = r;
                _cont.timeout_exec = timeout_exec;
                _cont.j = j;
                _cont.exclude_tuple_list = exclude_tuple_list;
                cont = _cont;
                return true;
            }
            return false;
        }

        public bool next_retrieve_cache(IPeersContinuation cont, out IPeersResponse? resp)
        {
            resp = null;
            RetrieveCacheContinuation _cont = (RetrieveCacheContinuation)cont;
            PeerTupleNode x_macron = make_tuple_node(new HCoord(0, pos[0]), _cont.j+1);
            PeerTupleNode respondant;
            try {
                resp = contact_peer
                    (_cont.p_id, x_macron, _cont.r,
                     _cont.timeout_exec, true,
                     out respondant, _cont.exclude_tuple_list, true);
            } catch (PeersNoParticipantsInNetworkError e) {
                return false;
            }
            return false;
        }

        /* Remotable methods */

        public IPeerParticipantSet get_participant_set
        (int lvl, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        throws PeersInvalidRequest
        {
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
        }

        public void forward_peer_message
        (IPeerMessage peer_message, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (peer_message is PeerMessageForwarder)) return;
            PeerMessageForwarder mf = (PeerMessageForwarder) peer_message;
            if (! check_valid_message(mf)) return;
            if (_rpc_caller == null) return;
            // begin
            bool optional = false;
            bool exclude_myself = false;
            if (services.has_key(mf.p_id))
            {
                optional = services[mf.p_id].p_is_optional;
                exclude_myself = ! services[mf.p_id].is_ready();
            }
            else
                optional = true;
            if (pos[mf.lvl] == mf.pos)
            {
                if (! my_gnode_participates(mf.p_id, mf.lvl))
                {
                    IPeersManagerStub nstub
                        = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                    PeerTupleGNode gn = make_tuple_gnode(new HCoord(mf.lvl, mf.pos), mf.n.tuple.size);
                    try {
                        nstub.set_non_participant(mf.msg_id, gn);
                    } catch (zcd.ModRpc.StubError e) {
                        // ignore
                    } catch (zcd.ModRpc.DeserializeError e) {
                        // ignore
                    }
                }
                else
                {
                    ArrayList<HCoord> exclude_gnode_list = new ArrayList<HCoord>();
                    exclude_gnode_list.add_all(get_non_participant_gnodes(mf.p_id));
                    if (exclude_myself)
                        exclude_gnode_list.add(new HCoord(0, pos[0]));
                    foreach (PeerTupleGNode gn in mf.exclude_tuple_list)
                    {
                        int @case;
                        HCoord ret;
                        convert_tuple_gnode(gn, out @case, out ret);
                        if (@case == 1)
                            exclude_gnode_list.add_all(get_all_gnodes_up_to_lvl(ret.lvl));
                        else if (@case == 2)
                            exclude_gnode_list.add(ret);
                    }
                    bool delivered = false;
                    while (! delivered)
                    {
                        HCoord? x = approximate(mf.x_macron, exclude_gnode_list, mf.reverse);
                        if (x == null)
                        {
                            IPeersManagerStub nstub
                                = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                            PeerTupleGNode gn = make_tuple_gnode(new HCoord(mf.lvl, mf.pos), mf.n.tuple.size);
                            try {
                                nstub.set_failure(mf.msg_id, gn);
                            } catch (zcd.ModRpc.StubError e) {
                                // ignore
                            } catch (zcd.ModRpc.DeserializeError e) {
                                // ignore
                            }
                            break;
                        }
                        else if (x.lvl == 0 && x.pos == pos[0])
                        {
                            IPeersManagerStub nstub
                                = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                            PeerTupleNode tuple_respondant = make_tuple_node(new HCoord(0, pos[0]), mf.n.tuple.size);
                            try {
                                IPeersRequest request
                                    = nstub.get_request(mf.msg_id, tuple_respondant);
                                IPeersResponse resp = services[mf.p_id].exec(request);
                                nstub.set_response(mf.msg_id, resp);
                            } catch (PeersUnknownMessageError e) {
                                // ignore
                            } catch (PeersInvalidRequest e) {
                                // ignore
                            } catch (zcd.ModRpc.StubError e) {
                                // ignore
                            } catch (zcd.ModRpc.DeserializeError e) {
                                // ignore
                            }
                            break;
                        }
                        else
                        {
                            PeerMessageForwarder mf2 = (PeerMessageForwarder)Json.gobject_deserialize
                                    (typeof(PeerMessageForwarder), Json.gobject_serialize(mf));
                            mf2.lvl = x.lvl;
                            mf2.pos = x.pos;
                            if (x.lvl == 0)
                                mf2.x_macron = null;
                            else
                                mf2.x_macron = new PeerTupleNode(mf.x_macron.tuple.slice(0, x.lvl));
                            mf2.exclude_tuple_list.clear();
                            mf2.non_participant_tuple_list.clear();
                            foreach (PeerTupleGNode t in mf.exclude_tuple_list)
                            {
                                int @case;
                                HCoord ret;
                                convert_tuple_gnode(t, out @case, out ret);
                                if (@case == 3)
                                {
                                    if (ret.equals(x))
                                    {
                                        int eps = t.top - t.tuple.size;
                                        PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-eps), x.lvl);
                                        mf2.exclude_tuple_list.add(_t);
                                    }
                                }
                            }
                            foreach (PeerTupleGNode t in mf.non_participant_tuple_list)
                            {
                                if (visible_by_someone_inside_my_gnode(t, x.lvl+1))
                                    mf2.non_participant_tuple_list.add(t);
                            }
                            IPeersManagerStub gwstub;
                            IPeersManagerStub? failed = null;
                            while (true)
                            {
                                try {
                                    gwstub = map_paths.i_peers_gateway(mf2.lvl, mf2.pos, _rpc_caller, failed);
                                } catch (PeersNonexistentDestinationError e) {
                                    tasklet.ms_wait(20);
                                    break;
                                }
                                try {
                                    gwstub.forward_peer_message(mf2);
                                } catch (zcd.ModRpc.StubError e) {
                                    failed = gwstub;
                                    continue;
                                } catch (zcd.ModRpc.DeserializeError e) {
                                    failed = gwstub;
                                    continue;
                                }
                                delivered = true;
                                IPeersManagerStub nstub
                                    = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                                PeerTupleGNode gn = make_tuple_gnode(x, mf.n.tuple.size);
                                try {
                                    nstub.set_next_destination(mf.msg_id, gn);
                                } catch (zcd.ModRpc.StubError e) {
                                    // ignore
                                } catch (zcd.ModRpc.DeserializeError e) {
                                    // ignore
                                }
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                IPeersManagerStub gwstub;
                IPeersManagerStub? failed = null;
                while (true)
                {
                    try {
                        gwstub = map_paths.i_peers_gateway(mf.lvl, mf.pos, _rpc_caller, failed);
                    } catch (PeersNonexistentDestinationError e) {
                        // give up routing
                        break;
                    }
                    try {
                        gwstub.forward_peer_message(mf);
                    } catch (zcd.ModRpc.StubError e) {
                        failed = gwstub;
                        continue;
                    } catch (zcd.ModRpc.DeserializeError e) {
                        failed = gwstub;
                        continue;
                    }
                    break;
                }
            }
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
        private class CheckNonParticipationTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public HCoord ret;
            public int p_id;
            public void * func()
            {
                if (t.check_non_participation(p_id, ret.lvl, ret.pos))
                    if (t.participant_maps.has_key(p_id))
                        t.participant_maps[p_id].participant_list.remove(ret); 
                return null;
            }
        }

        public IPeersRequest get_request
        (int msg_id, IPeerTupleNode _respondant, zcd.ModRpc.CallerInfo? _rpc_caller=null)
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
            // might be a fake request
            if (wa.request == null) throw new PeersUnknownMessageError.GENERIC("was a fake request");
            // ok
            wa.respondant_node = respondant;
            wa.ch.send_async(0);
            return wa.request;
        }

        public void set_response
        (int msg_id, IPeersResponse response, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_response: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            if (wa.respondant_node == null)
            {
                debug("PeersManager.set_response: ignored because did not send request");
                return;
            }
            wa.response = response;
            wa.ch.send_async(0);
        }

        public void set_next_destination
        (int msg_id, IPeerTupleGNode _tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
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
        (int msg_id, IPeerTupleGNode _tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
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
            if (tuple.top - tuple.tuple.size >= old_k)
            {
                debug("PeersManager.set_failure: ignored because already reached a lower level");
                return;
            }
            wa.exclude_gnode = tuple;
            wa.ch.send_async(0);
        }

        public void set_non_participant
        (int msg_id, IPeerTupleGNode _tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
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
            if (tuple.top - tuple.tuple.size >= old_k)
            {
                debug("PeersManager.set_non_participant: ignored because already reached a lower level");
                return;
            }
            wa.non_participant_gnode = tuple;
            wa.ch.send_async(0);
        }

        public void set_participant
        (int p_id, IPeerTupleGNode tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (tuple is PeerTupleGNode)) return;
            PeerTupleGNode gn = (PeerTupleGNode) tuple;
            if (! check_valid_tuple_gnode(gn)) return;
            // begin
            if (services.has_key(p_id) && ! services[p_id].p_is_optional) return;
            int @case;
            HCoord ret;
            convert_tuple_gnode(gn, out @case, out @ret);
            if (@case == 1) return;
            if (ret in recent_published_list) return;
            recent_published_list.add(ret);
            if (! participant_maps.has_key(p_id))
                participant_maps[p_id] = new PeerParticipantMap();
            participant_maps[p_id].participant_list.add(ret);
            PeerTupleGNode ret_gn = make_tuple_gnode(ret, levels);
            MissingArcSetParticipant missing_handler = new MissingArcSetParticipant(this, p_id, ret_gn);
            IPeersManagerStub br_stub = neighbors_factory.i_peers_get_broadcast(missing_handler);
            try {
                br_stub.set_participant(p_id, ret_gn);
            } catch (zcd.ModRpc.StubError e) {
                // ignore
            } catch (zcd.ModRpc.DeserializeError e) {
                // ignore
            }
            RecentPublishedListRemoveTasklet ts = new RecentPublishedListRemoveTasklet();
            ts.t = this;
            ts.ret = ret;
            tasklet.spawn(ts);
        }
        private class RecentPublishedListRemoveTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public HCoord ret;
            public void * func()
            {
                tasklet.ms_wait(60000);
                t.recent_published_list.remove(ret);
                return null;
            }
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

        public virtual bool is_ready()
        {
            return true;
        }

        public abstract IPeersResponse exec(IPeersRequest req);
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
        throws PeersNoParticipantsInNetworkError
        {
            PeerTupleNode respondant;
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
