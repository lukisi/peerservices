using Gee;
using zcd;
using Tasklets;

namespace Netsukuku
{
    public interface IPeersMapPaths : Object
    {
        public abstract int i_peers_get_levels();
        public abstract int i_peers_get_gsize(int level);
        public abstract int i_peers_get_nodes_in_my_group(int level);
        public abstract int i_peers_get_my_pos(int level);
        public abstract bool i_peers_exists(int level, int pos);
        public abstract int i_peers_get_nodes_in_group(int level, int pos);
        public abstract IAddressManagerRootDispatcher i_peers_gateway(int level, int pos);
    }

    // This delegate is used as a callback to tell whether a certain g-node in my map (or
    //  myself) is valid as a result for Ht(hp(k)). It can be used to exclude some
    //  gnodes because they failed to get reached. It can be used to exclude gnodes that
    //  are not participating.
    internal delegate bool ValidGnode(HCoord gnode);

    public class PeersManager : Object
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
        public PeersManager(IPeersMapPaths map_paths)
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
    }

    public abstract class PeerService : Object
    {
        private ArrayList<int> gsizes;
        public PeerService(Gee.List<int> gsizes)
        {
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
        }

        protected abstract uint64 hash_from_key(Object k, uint64 top);

        public PeerTuple perfect_tuple(Object k)
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
            return new PeerTuple(tuple);
        }
    }

    public class PeerTuple : Object, ISerializable
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

    public class PeerMessageForwarder : Object, ISerializable
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
