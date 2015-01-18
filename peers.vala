using Gee;
using zcd;
using Tasklets;

namespace Netsukuku
{
    public errordomain PeersNonexistentDestinationError {
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
            (int level, int pos, IAddressManagerRootDispatcher? failed=null)
            throws PeersNonexistentDestinationError;
    }

    public interface IPeersBackStubFactory : Object
    {
        // positions[0] is pos[0] of the node to contact inside our gnode
        // of level positions.size
        public abstract IAddressManagerRootDispatcher i_peers_get_tcp_inside
            (Gee.List<int> positions);
    }

    // This delegate is used as a callback to tell whether a certain g-node in my map (or
    //  myself) is valid as a result for Ht(hp(k)). It can be used to exclude some
    //  gnodes because they failed to get reached. It can be used to exclude gnodes that
    //  are not participating.
    internal delegate bool ValidGnode(HCoord gnode);
    internal interface IValidityChecker : Object
    {
        public abstract bool i_validity_check(HCoord gnode);
    }
    internal class NullChecker : Object, IValidityChecker
    {
        private IValidityChecker? parent; // decorator pattern
        public NullChecker(IValidityChecker? parent=null)
        {
            this.parent = parent;
        }

        public bool i_validity_check(HCoord gnode)
        {
            // decorator pattern
            if (parent != null && ! parent.i_validity_check(gnode)) return false;
            // this checker implementation
            return true;
        }
    }

    internal class ExcludedTupleChecker : Object, IValidityChecker
    {
        private int toplevel;
        private IPeersMapPaths map_paths;
        private ExcludedTupleContainer excl;
        private IValidityChecker? parent; // decorator pattern
        public ExcludedTupleChecker
        (int toplevel,
         IPeersMapPaths map_paths,
         ExcludedTupleContainer excl,
         IValidityChecker? parent=null)
        {
            this.toplevel = toplevel;
            this.map_paths = map_paths;
            this.excl = excl;
            this.parent = parent;
        }

        public bool i_validity_check(HCoord gnode)
        {
            // decorator pattern
            if (parent != null && ! parent.i_validity_check(gnode)) return false;
            // this checker implementation
            ArrayList<int> gnode_pos = new ArrayList<int>();
            for (int l = toplevel; l >= gnode.lvl; l--)
            {
                gnode_pos.add(map_paths.i_peers_get_my_pos(l));
            }
            gnode_pos.add(gnode.pos);
            PeerTuple tuple_gnode = new PeerTuple(gnode_pos);
            return ! excl.contains(tuple_gnode);
        }
    }

    internal class ExcludedTupleContainer : Object
    {
        private int toplevel;
        private ArrayList<PeerTuple> e_list;
        public ExcludedTupleContainer(int toplevel)
        {
            this.toplevel = toplevel;
            e_list = new ArrayList<PeerTuple>();
        }
        private int get_rev_pos(PeerTuple t, int j)
        {
            // t has positions for levels from ε >= 0 to toplevel.
            // get position for toplevel-j.
            return t.tuple[t.tuple.size - j];
        }
        public void add_tuple(PeerTuple t)
        {
            // t has positions for levels from ε >= 0 to toplevel.
            // hence ε = toplevel+1 - t.tuple.size
            // t.tuple[0] is for level ε, i-th is for ε+i, ...
            assert(toplevel+1 - t.tuple.size >= 0);
            // If t is already contained in the list, return.
            if (contains(t)) return;
            // Cycle the list:
            int i = 0;
            while (i < e_list.size)
            {
                PeerTuple e = e_list[i];
                if (e.tuple.size > t.tuple.size)
                {
                    // If a gnode which is inside t is in the list, remove it.
                    bool mismatch = false;
                    for (int j = 0; j < t.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(t,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        e_list.remove_at(i);
                        i--;
                    }
                }
                i++;
            }
            // Then add t.
            e_list.add(t);
        }
        public bool contains(PeerTuple t)
        {
            // t has positions for levels from ε >= 0 to toplevel.
            // hence ε = toplevel+1 - t.tuple.size
            // t.tuple[0] is for level ε, i-th is for ε+i, ...
            assert(toplevel+1 - t.tuple.size >= 0);
            // Cycle the list:
            foreach (PeerTuple e in e_list)
            {
                if (e.tuple.size <= t.tuple.size)
                {
                    // If t is already in the list, return true.
                    bool mismatch = false;
                    for (int j = 0; j < e.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(t,j))
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
        public ArrayList<PeerTuple> make_for_gnode(PeerTuple gnode)
        {
            // gnode is a tuple that represents one of our sibling at a level
            // and it must not be in this set.
            assert(! contains(gnode));
            int level_of_gnode = toplevel - gnode.tuple.size + 1;
            assert(level_of_gnode > 0);
            ArrayList<PeerTuple> ret = new ArrayList<PeerTuple>();
            // Cycle my list:
            foreach (PeerTuple e in e_list)
            {
                if (e.tuple.size > gnode.tuple.size)
                {
                    // If e is inside gnode, add e to new container.
                    bool mismatch = false;
                    for (int j = 0; j < gnode.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(gnode,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        PeerTuple e_in_gnode =
                            new PeerTuple(
                            e.tuple.slice(0, e.tuple.size - gnode.tuple.size));
                        ret.add(e_in_gnode);
                    }
                }
            }
            return ret;
        }
    }

    public class PeersManager : Object,
                                IPeersManager
    {
        public static void init()
        {
            // Register serializable types
            typeof(PeerTuple).class_peek();
            typeof(PeerMessageForwarder).class_peek();
        }

        private IPeersMapPaths map_paths;
        private int levels;
        private int[] gsizes;
        private int[] pos;
        private IPeersBackStubFactory back_stub_factory;
        private HashMap<int, PeerService> services;
        public PeersManager
            (IPeersMapPaths map_paths,
             IPeersBackStubFactory back_stub_factory,
             Gee.List<PeerService> services)
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
            this.services = new HashMap<int, PeerService>();
            foreach (PeerService p in services)
                this.services[p.pid] = p;
        }

        internal HCoord? approximate(PeerTuple x_macron,
                                     ValidGnode valid_gnode_callback=(a)=>{return true;},
                                     bool reverse=false)
        {
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
                if (valid_gnode_callback(x))
                {
                    PeerTuple tuple_x = make_tuple(x, valid_levels);
                    int distance = reverse ?
                                   dist_rev(x_macron, tuple_x) :
                                   dist(x_macron, tuple_x);
                    if (min_distance == -1 || distance < min_distance)
                    {
                        ret = x;
                        min_distance = distance;
                    }
                }
            }
            // and then for me
            HCoord x = new HCoord(0, pos[0]);
            if (valid_gnode_callback(x))
            {
                PeerTuple tuple_x = make_tuple(x, valid_levels);
                int distance = reverse ?
                               dist_rev(x_macron, tuple_x) :
                               dist(x_macron, tuple_x);
                if (min_distance == -1 || distance < min_distance)
                {
                    ret = x;
                    min_distance = distance;
                }
            }
            // If null yet, then nobody participates.
            return ret;
        }

        private PeerTuple make_tuple(HCoord y, int valid_levels)
        {
            ArrayList<int> tuple = new ArrayList<int>();
            for (int j = valid_levels-1; j > y.lvl; j--)
            {
                tuple.insert(0, pos[j]);
            }
            tuple.insert(0, y.pos);
            for (int j = y.lvl-1; j >= 0; j--)
            {
                tuple.insert(0, 0);
            }
            return new PeerTuple(tuple);
        }

        private int dist(PeerTuple x_macron, PeerTuple x)
        {
            int valid_levels = x_macron.tuple.size;
            assert (valid_levels == x.tuple.size);
            int distance = 0;
            for (int j = valid_levels-1; j >= 0; j--)
            {
                if (x_macron.tuple[j] == x.tuple[j])
                {
                    distance += 0;
                }
                else if (x.tuple[j] > x_macron.tuple[j])
                {
                    distance += x.tuple[j] - x_macron.tuple[j];
                }
                else
                {
                    distance += x.tuple[j] - x_macron.tuple[j] + gsizes[j];
                }
                if (j > 0)
                {
                    distance *= gsizes[j-1];
                }
            }
            return distance;
        }

        private int dist_rev(PeerTuple x_macron, PeerTuple x)
        {
            return dist(x, x_macron);
        }

        // Helper: given a HCoord (lvl,pos) and a level j:
        //   * a node n that shares with this node the level j+1 has sent a
        //     message to find x
        //   * this node is going to send a message to report failure or
        //     to report next destination
        //   * produce a tuple xε·xε+1·...·xj-1·xj
        private Gee.List<int> make_positions_inside(int j, HCoord gnode)
        {
            assert(j >= gnode.lvl);
            Gee.List<int> ret = new ArrayList<int>();
            ret.add(gnode.pos);
            for (int i = gnode.lvl+1; i <= j ; i++)
                ret.add(map_paths.i_peers_get_my_pos(i));
            return ret;
        }

        private bool check_valid_message(PeerMessageForwarder mf)
        {
            if (mf.lvl < 0) return false;
            if (mf.lvl > levels) return false;
            if (mf.pos < 0) return false;
            if (mf.pos > map_paths.i_peers_get_gsize(mf.lvl)) return false;
            if (mf.x_macron.tuple.size != mf.lvl) return false;
            if (mf.n.tuple.size > levels) return false;
            if (mf.n.tuple.size <= mf.x_macron.tuple.size) return false;
            // TODO finish checking
            return true;
        }

        /* Remotable methods
         */

		public void forward_peer_message(IPeerMessage peer_message)
		{
		    if (! (peer_message is PeerMessageForwarder)) return;
		    PeerMessageForwarder mf = (PeerMessageForwarder)peer_message;
		    if (! check_valid_message(mf)) return; // ignore invalid input from untrusted source.
		    if (pos[mf.lvl] == mf.pos)
		    {
		        // for my g-node
		        if (mf.lvl == 0)
		        {
		            // my final destination
		            // TODO
		        }
		        else
		        {
		            if (! services.has_key(mf.p_id))
		            {
		                // No knowledge yet about this service.
		                Gee.List<int> n_positions = 
		                back_stub_factory.i_peers_get_tcp_inside(
		                        make_positions_inside(
		                        mf.n.tuple.size,
		                        new HCoord()))
		                .peers_manager.set_failure(
		            }
		            PeerService p = services[mf.p_id];
		            IValidityChecker? parent = null;
		            
		            // TODO if service mf.p_id is NOT in my registered services, then do as if
		            //      service is optional and nobody participates inside my g-node.
		            // TODO if service mf.p_id IS in my registered services, then look if it is
		            //      an optional service and in that case obtain from the service handler
		            //      a IValidityChecker parent which will check its own participant_map.

		            ExcludedTupleContainer cont = new ExcludedTupleContainer(mf.lvl-1);
		            foreach (PeerTuple t in mf.exclude_gnode_list) cont.add_tuple(t);
		            IValidityChecker chkr = new ExcludedTupleChecker(mf.lvl-1, map_paths, cont, parent);

		            HCoord? next_dest = approximate(
		                mf.x_macron,
		                (gnode) => {
		                    return chkr.i_validity_check(gnode);
		                },
		                mf.reverse);
		            if (next_dest == null)
		            {
		                // nobody remains
		                // TODO
		            }
		            else if (next_dest.lvl == 0 && next_dest.pos == pos[0])
		            {
		                // I am the destination
		                // TODO
		            }
		            else
		            {
		                // found next destination
		                int k = next_dest.lvl;
		                if (k == 0)
		                {
		                    mf.exclude_gnode_list.clear();
		                }
		                else
		                {
                            ArrayList<int> next_dest_pos = new ArrayList<int>();
                            // from next_dest to current-1
                            next_dest_pos.add(next_dest.pos);
                            for (int l = next_dest.lvl+1; l < mf.lvl; l++)
                            {
                                next_dest_pos.add(map_paths.i_peers_get_my_pos(l));
                            }
                            PeerTuple tuple_next_dest = new PeerTuple(next_dest_pos);
		                    mf.exclude_gnode_list = cont.make_for_gnode(tuple_next_dest);
		                }
		                // TODO
		            }
		        }
		    } 
		    else
		    {
		        IAddressManagerRootDispatcher? failed=null;
		        IAddressManagerRootDispatcher stub;
		        while (true)
		        {
		            try
		            {
		                stub = map_paths.i_peers_gateway(mf.lvl, mf.pos, failed);
		                stub.peers_manager.forward_peer_message(peer_message);
		                return;
		            }
		            catch (RPCError e)
		            {
		                // Since this should be a reliable stub the arc to the gateway should be removed
		                // and a new gateway should be given to us.
		                failed = stub;
		            }
		            catch (PeersNonexistentDestinationError e)
		            {
		                // give up
		                return;
		            }
		        }
		    }
		}

		public RemoteCall get_request (int id_msg) throws PeersUnknownMessage
        {
            assert_not_reached(); // TODO
        }

		public void set_response (int id_msg, ISerializable resp)
        {
            assert_not_reached(); // TODO
        }

		public void set_next_destination (int id_msg, IPeerTuple tuple)
        {
            assert_not_reached(); // TODO
        }

		public void set_failure (int id_msg, IPeerTuple tuple)
        {
            assert_not_reached(); // TODO
        }
    }

    public abstract class PeerService : Object
    {
        protected ArrayList<int> gsizes;
        public int pid {get; private set;}
        public PeerService(int pid, Gee.List<int> gsizes)
        {
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            this.pid = pid;
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

        internal PeerTuple internal_perfect_tuple(Object k)
        {
            return new PeerTuple(perfect_tuple(k));
        }
    }

    public abstract class OptionalPeerService : PeerService
    {
        public OptionalPeerService(int pid, Gee.List<int> gsizes)
        {
            base(pid, gsizes);
        }
    }

    internal class PeerTuple : Object, ISerializable, IPeerTuple
    {
        private ArrayList<int> _tuple;
        public Gee.List<int> tuple {
            owned get {
                return _tuple.read_only_view;
            }
        }
        public PeerTuple(Gee.List<int> tuple)
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

    internal class PeerMessageForwarder : Object, ISerializable, IPeerMessage
    {
        public PeerTuple n;
        public PeerTuple x_macron;
        public int lvl;
        public int pos;
        public int p_id;
        public int msg_id;
        public Gee.List<PeerTuple> exclude_gnode_list;
        public bool reverse;
        public PeerMessageForwarder()
        {
            _reset();
        }

        private void _reset()
        {
            exclude_gnode_list = new ArrayList<PeerTuple>();
            reverse = false;
        }

        public Variant serialize_to_variant()
        {
            Variant v0 = n.serialize_to_variant();
            Variant v1 = x_macron.serialize_to_variant();
            Variant v2 = Serializer.int_to_variant(lvl);
            Variant v3 = Serializer.int_to_variant(pos);
            Variant v4 = Serializer.int_to_variant(p_id);
            Variant v5 = Serializer.int_to_variant(msg_id);
            ListISerializable lst = new ListISerializable.with_backer(exclude_gnode_list);
            Variant v6 = lst.serialize_to_variant();
            Variant v7 = Serializer.int_to_variant(reverse ? 1 : 0);
            Variant vtemp = Serializer.tuple_to_variant_5(v0, v1, v2, v3, v4);
            Variant vret = Serializer.tuple_to_variant_4(vtemp, v5, v6, v7);
            return vret;
        }

        public void deserialize_from_variant(Variant v) throws SerializerError
        {
            _reset();
            Variant v0;
            Variant v1;
            Variant v2;
            Variant v3;
            Variant v4;
            Variant v5;
            Variant v6;
            Variant v7;
            Variant vtemp;
            Serializer.variant_to_tuple_4(v, out vtemp, out v5, out v6, out v7);
            Serializer.variant_to_tuple_5(vtemp, out v0, out v1, out v2, out v3, out v4);
            n = (PeerTuple)Object.new(typeof(PeerTuple));
            n.deserialize_from_variant(v0);
            x_macron = (PeerTuple)Object.new(typeof(PeerTuple));
            x_macron.deserialize_from_variant(v1);
            lvl = Serializer.variant_to_int(v2);
            pos = Serializer.variant_to_int(v3);
            p_id = Serializer.variant_to_int(v4);
            msg_id = Serializer.variant_to_int(v5);
            ListISerializable lst = (ListISerializable)Object.new(typeof(ListISerializable));
            lst.deserialize_from_variant(v6);
            exclude_gnode_list = (Gee.List<PeerTuple>)lst.backed;
            reverse = Serializer.variant_to_int(v7) == 1;
        }
    }
}
