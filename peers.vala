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
using zcd;
using Tasklets;

namespace Netsukuku
{
    public errordomain PeersNonexistentDestinationError {
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
        public abstract IAddressManagerRootDispatcher i_peers_gateway
            (int level, int pos,
             CallerInfo? received_from=null,
             IAddressManagerRootDispatcher? failed=null)
            throws PeersNonexistentDestinationError;
    }

    public interface IPeersBackStubFactory : Object
    {
        // positions[0] is pos[0] of the node to contact inside our gnode
        // of level positions.size
        public abstract IAddressManagerRootDispatcher i_peers_get_tcp_inside
            (Gee.List<int> positions);
    }

    public interface IPeersNeighborsFactory : Object
    {
        public abstract IAddressManagerRootDispatcher i_peers_get_broadcast(
                            IPeersMissingArcHandler missing_handler);
        public abstract IAddressManagerRootDispatcher i_peers_get_tcp(
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

    internal class PeerTupleNode : Object, ISerializable, IPeerTupleNode
    {
        private ArrayList<int> _tuple;
        public Gee.List<int> tuple {
            owned get {
                return _tuple.read_only_view;
            }
        }
        public PeerTupleNode(Gee.List<int> tuple)
        {
            this._tuple = new ArrayList<int>();
            this._tuple.add_all(tuple);
        }

        public Variant serialize_to_variant()
        {
            Variant v0 = Serializer.int_array_to_variant(_tuple.to_array());
            return v0;
        }

        public void deserialize_from_variant(Variant v) throws SerializerError
        {
            int[] my_tuple = Serializer.variant_to_int_array(v);
            _tuple = new ArrayList<int>();
            _tuple.add_all_array(my_tuple);
        }
    }

    internal class PeerTupleGNode : Object, ISerializable, IPeerTupleGNode
    {
        private ArrayList<int> _tuple;
        public Gee.List<int> tuple {
            owned get {
                return _tuple.read_only_view;
            }
        }
        public int top {get; private set;}
        public PeerTupleGNode(Gee.List<int> tuple, int top)
        {
            this._tuple = new ArrayList<int>();
            this._tuple.add_all(tuple);
            this.top = top;
        }

        public Variant serialize_to_variant()
        {
            Variant v0 = Serializer.int_array_to_variant(_tuple.to_array());
            Variant v1 = Serializer.int_to_variant(top);
            Variant vret = Serializer.tuple_to_variant(v0, v1);
            return vret;
        }

        public void deserialize_from_variant(Variant v) throws SerializerError
        {
            Variant v0;
            Variant v1;
            Serializer.variant_to_tuple(v, out v0, out v1);
            int[] my_tuple = Serializer.variant_to_int_array(v0);
            _tuple = new ArrayList<int>();
            _tuple.add_all_array(my_tuple);
            top = Serializer.variant_to_int(v1);
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

        public PeerTupleGNodeContainer()
        {
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

    internal class PeerMessageForwarder : Object, ISerializable, IPeerMessage
    {
        public PeerTupleNode n;
        public PeerTupleNode? x_macron;
        public int lvl;
        public int pos;
        public int p_id;
        public int msg_id;
        public Gee.List<PeerTupleGNode> exclude_tuple_list;
        public Gee.List<PeerTupleGNode> non_participant_tuple_list;
        public bool reverse;

        public PeerMessageForwarder()
        {
            exclude_tuple_list = new ArrayList<PeerTupleGNode>();
            non_participant_tuple_list = new ArrayList<PeerTupleGNode>();
            reverse = false;
        }

        public Variant serialize_to_variant()
        {
            Variant v0 = n.serialize_to_variant();
            Variant v1;
            if (x_macron == null)
            {
                v1 = Serializer.int_to_variant(0);
            }
            else
            {
                v1 = x_macron.serialize_to_variant();
            }
            Variant v2 = Serializer.int_to_variant(lvl);
            Variant v3 = Serializer.int_to_variant(pos);
            Variant v4 = Serializer.int_to_variant(p_id);
            Variant v5 = Serializer.int_to_variant(msg_id);
            Variant v6;
            {
                ListISerializable lst = new ListISerializable();
                foreach (PeerTupleGNode o in exclude_tuple_list) lst.add(o);
                v6 = lst.serialize_to_variant();
            }
            Variant v7;
            {
                ListISerializable lst = new ListISerializable();
                foreach (PeerTupleGNode o in non_participant_tuple_list) lst.add(o);
                v7 = lst.serialize_to_variant();
            }
            Variant v8 = Serializer.int_to_variant(reverse ? 1 : 0);
            Variant vtemp = Serializer.tuple_to_variant_5(v0, v1, v2, v3, v4);
            Variant vret = Serializer.tuple_to_variant_5(vtemp, v5, v6, v7, v8);
            return vret;
        }

        public void deserialize_from_variant(Variant v) throws SerializerError
        {
            Variant v0;
            Variant v1;
            Variant v2;
            Variant v3;
            Variant v4;
            Variant v5;
            Variant v6;
            Variant v7;
            Variant v8;
            Variant vtemp;
            Serializer.variant_to_tuple_5(v, out vtemp, out v5, out v6, out v7, out v8);
            Serializer.variant_to_tuple_5(vtemp, out v0, out v1, out v2, out v3, out v4);
            n = (PeerTupleNode)Object.new(typeof(PeerTupleNode));
            n.deserialize_from_variant(v0);
            if (v1.get_type_string() == "i")
            {
                x_macron = null;
            }
            else
            {
                x_macron = (PeerTupleNode)Object.new(typeof(PeerTupleNode));
                x_macron.deserialize_from_variant(v1);
            }
            lvl = Serializer.variant_to_int(v2);
            pos = Serializer.variant_to_int(v3);
            p_id = Serializer.variant_to_int(v4);
            msg_id = Serializer.variant_to_int(v5);
            exclude_tuple_list = new ArrayList<PeerTupleGNode>();
            {
                ListISerializable lst = (ListISerializable)Object.new(typeof(ListISerializable));
                lst.deserialize_from_variant(v6);
                Gee.List<PeerTupleGNode> typed_lst = (Gee.List<PeerTupleGNode>)lst.backed;
                exclude_tuple_list.add_all(typed_lst);
            }
            non_participant_tuple_list = new ArrayList<PeerTupleGNode>();
            {
                ListISerializable lst = (ListISerializable)Object.new(typeof(ListISerializable));
                lst.deserialize_from_variant(v7);
                Gee.List<PeerTupleGNode> typed_lst = (Gee.List<PeerTupleGNode>)lst.backed;
                non_participant_tuple_list.add_all(typed_lst);
            }
            reverse = Serializer.variant_to_int(v8) == 1;
        }
    }

    internal class WaitingAnswer : Object
    {
        public Channel ch;
        public RemoteCall? request;
        public PeerTupleGNode min_target;
        public PeerTupleGNode? exclude_gnode;
        public PeerTupleGNode? non_participant_gnode;
        public PeerTupleNode? respondant_node;
        public ISerializable? response;
        public WaitingAnswer(RemoteCall? request, PeerTupleGNode min_target)
        {
            ch = new Channel();
            this.request = request;
            this.min_target = min_target;
            exclude_gnode = null;
            non_participant_gnode = null;
            respondant_node = null;
            response = null;
        }
    }

    internal class PeerParticipantMap : Object, ISerializable
    {
        private ArrayList<HCoord> _list;
        public Gee.List<HCoord> participant_list {
            owned get {
                return _list;
            }
        }

        public PeerParticipantMap()
        {
            _list = new ArrayList<HCoord>((a,b) => {return a.equals(b);});
        }

        public Variant serialize_to_variant()
        {
            Variant v;
            {
                ListISerializable lst = new ListISerializable();
                foreach (HCoord o in participant_list) lst.add(o);
                v = lst.serialize_to_variant();
            }
            return v;
        }

        public void deserialize_from_variant(Variant v) throws SerializerError
        {
            _list = new ArrayList<HCoord>((a,b) => {return a.equals(b);});
            {
                ListISerializable lst = (ListISerializable)Object.new(typeof(ListISerializable));
                lst.deserialize_from_variant(v);
                Gee.List<HCoord> typed_lst = (Gee.List<HCoord>)lst.backed;
                participant_list.add_all(typed_lst);
            }
        }
    }

    internal class PeerParticipantSet : Object, ISerializable, IPeerParticipantSet
    {
        private HashMap<int, PeerParticipantMap> _set;
        public HashMap<int, PeerParticipantMap> participant_set {
            owned get {
                return _set;
            }
        }

        public PeerParticipantSet()
        {
            _set = new HashMap<int, PeerParticipantMap>();
        }

        public Variant serialize_to_variant()
        {
            Variant v0 = Serializer.int_array_to_variant(participant_set.keys.to_array());
            Variant v1;
            {
                ListISerializable lv = new ListISerializable();
                foreach (int k in participant_set.keys) lv.add(participant_set[k]);
                v1 = lv.serialize_to_variant();
            }
            Variant vret = Serializer.tuple_to_variant(v0, v1);
            return vret;
        }

        public void deserialize_from_variant(Variant v) throws SerializerError
        {
            Variant v0;
            Variant v1;
            Serializer.variant_to_tuple(v, out v0, out v1);
            {
                int[] keys = Serializer.variant_to_int_array(v0);
                ListISerializable lv = (ListISerializable)Object.new(typeof(ListISerializable));
                lv.deserialize_from_variant(v1);
                Gee.List<PeerParticipantMap> typed_lv = (Gee.List<PeerParticipantMap>)lv.backed;
                if (keys.length != typed_lv.size)
                    throw new SerializerError.GENERIC("Mismatch in hashmap keys and values numbers.");
                _set = new HashMap<int, PeerParticipantMap>();
                for (int i = 0; i < typed_lv.size; i++)
                    _set[keys[i]] = typed_lv[i];
            }
        }
    }

    public class PeersManager : Object,
                                IPeersManager
    {
        public static void init()
        {
            // Register serializable types
            typeof(PeerTupleNode).class_peek();
            typeof(PeerTupleGNode).class_peek();
            typeof(PeerMessageForwarder).class_peek();
            typeof(PeerParticipantMap).class_peek();
            typeof(PeerParticipantSet).class_peek();
        }

        private IPeersMapPaths map_paths;
        private int levels;
        private int[] gsizes;
        private int[] pos;
        private IPeersBackStubFactory back_stub_factory;
        private IPeersNeighborsFactory neighbors_factory;
        private HashMap<int, PeerService> services;
        private HashMap<int, PeerParticipantMap> participant_maps;
        public PeersManager
            (IPeersMapPaths map_paths,
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
        }

        public void register(PeerService p)
        {
            // TODO
            if (services.has_key(p.p_id))
            {
                critical("PeersManager.register: Two services with same ID?");
                return;
            }
            services[p.p_id] = p;
            // ...
            if (p.p_is_optional)
            {
                // TODO start tasklet to publish periodically my participation
            }
            // ...
            // TODO start tasklet to retrieve records for my cache of DHT
            // ...
            if (p.p_is_optional)
            {
                if (!participant_maps.has_key(p.p_id))
                    participant_maps[p.p_id] = new PeerParticipantMap();
                // save my position
                PeerParticipantMap map = participant_maps[p.p_id];
                map.participant_list.add(new HCoord(0, pos[0]));
            }
            // ...
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

        private bool visible_by_someone_inside_my_gnode(PeerTupleGNode t, int lvl)
        {
            // TODO
            error("");
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

        internal HCoord? approximate(PeerTupleNode x_macron,
                                     Gee.List<HCoord> _exclude_list,
                                     bool reverse=false)
        {
            // Make sure that exclude_list is searchable
            Gee.List<HCoord> exclude_list = new ArrayList<HCoord>((a, b) => {return a.equals(b);});
            exclude_list.add_all(_exclude_list);
            // This function is x=Ht(x̄)
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

        internal ISerializable contact_peer
        (int p_id,
         PeerTupleNode x_macron,
         RemoteCall request,
         int timeout_exec,
         bool exclude_myself,
         out PeerTupleNode respondant,
         PeerTupleGNodeContainer? _exclude_tuple_list=null,
         bool reverse=false)
        throws PeersNoParticipantsInNetworkError
        {
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
                exclude_tuple_list = new PeerTupleGNodeContainer();
            foreach (PeerTupleGNode gn in exclude_tuple_list.list)
            {
                int @case;
                HCoord ret;
                convert_tuple_gnode(gn, out @case, out ret);
                if (@case == 2)
                    exclude_gnode_list.add(ret);
            }
            PeerTupleGNodeContainer non_participant_tuple_list = new PeerTupleGNodeContainer();
            ISerializable? response = null;
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
                // TODO
                assert_not_reached();
            }
            return response;
        }

        /* Remotable methods
         */

		public IPeerParticipantSet get_participant_set(int lvl)
		{
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

		public void forward_peer_message(IPeerMessage peer_message)
		{
		    // TODO
            assert_not_reached();
		}

		public RemoteCall get_request (int msg_id, IPeerTupleNode respondant) throws PeersUnknownMessageError
        {
		    // TODO
            assert_not_reached();
        }

		public void set_response (int msg_id, ISerializable response)
        {
		    // TODO
            assert_not_reached();
        }

		public void set_next_destination (int msg_id, IPeerTupleGNode tuple)
        {
		    // TODO
            assert_not_reached();
        }

		public void set_failure (int msg_id, IPeerTupleGNode tuple)
        {
		    // TODO
            assert_not_reached();
        }

		public void set_non_participant (int msg_id, IPeerTupleGNode tuple)
        {
		    // TODO
            assert_not_reached();
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

        public abstract ISerializable exec(RemoteCall req);
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

        internal PeerTupleNode internal_perfect_tuple(Object k)
        {
            return new PeerTupleNode(perfect_tuple(k));
        }

        protected ISerializable call(Object k, RemoteCall request, int timeout_exec)
        throws PeersNoParticipantsInNetworkError
        {
            PeerTupleNode respondant;
            return
            peers_manager.contact_peer
                (p_id,
                 internal_perfect_tuple(k),
                 request,
                 timeout_exec,
                 false,
                 out respondant);
        }
    }
}
