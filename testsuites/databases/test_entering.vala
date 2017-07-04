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
using Netsukuku;
using Netsukuku.PeerServices;
using TaskletSystem;
using EnteringTestcase;

namespace EnteringTestcase
{
    namespace Service01
    {
        class Names : Object
        {
            public static const string Mark = "Mark";
            public static const string John = "John";
            public static const string Luke = "Luke";
            public static const string Stef = "Stef";
            public static const string Sue = "Sue";
            public static const string Bob = "Bob";
            public static const string Clark = "Clark";
            public static PeerTupleNode evaluate_hash_node(string n)
            {
                if (n == Mark)
                    return new PeerTupleNode(new ArrayList<int>.wrap({1,1,0,1}));
                if (n == John)
                    return new PeerTupleNode(new ArrayList<int>.wrap({1,1,0,1}));
                if (n == Luke)
                    return new PeerTupleNode(new ArrayList<int>.wrap({1,0,0,0}));
                if (n == Stef)
                    return new PeerTupleNode(new ArrayList<int>.wrap({0,0,1,1}));
                if (n == Sue)
                    return new PeerTupleNode(new ArrayList<int>.wrap({1,0,1,0}));

                return new PeerTupleNode(new ArrayList<int>.wrap({0,0,0,1}));
            }
        }
    }

    namespace Service00
    {
        /* Service 00
          * keys = 1, ... levels
          *
          * hash_nodes =
          *    k=1       {0} a node in my g-node of level 1
          *    k=2       {0,0}
          *    ....
          *    k=levels  {0,0,0,0}
          *
          * values = state of the g-node (a string which starts with "" and gets appends)
          *
          * Requests: add_segment, replica_value
          *
          * string add_segment(string)
          *   the client pass a segment that is to add to the current string and obtains the whole string.
         */

        class Key : Object
        {
            public int key {public get; public set;}
        }

        class Record : Object
        {
            public int key {public get; public set;}
            public string data {public get; public set;}
        }

        class AddSegmentRequest : Object, IPeersRequest
        {
            public int key {public get; public set;}
            public string segment {public get; public set;}
        }
        class AddSegmentResponse : Object, IPeersResponse
        {
            public string data {public get; public set;}
        }

        class ReplicaRequest : Object, IPeersRequest
        {
            public int key {public get; public set;}
            public string data {public get; public set;}
        }
        class ReplicaOkResponse : Object, IPeersResponse
        {
        }

        class Servant : Object
        {
            public int levels {public get; private set;}
            private int guest_gnode_level;
            private int new_gnode_level;
            private Servant? prev_id;
            public Databases.Databases databases;
            public Client client;
            public HashMap<int,string> database;

            public Servant
            (Client client, Databases.Databases databases, int levels,
             int guest_gnode_level=-1, int new_gnode_level=-1, Servant? prev_id=null)
            {
                if (new_gnode_level == -1) new_gnode_level = levels;
                this.databases = databases;
                this.client = client;
                this.levels = levels;
                this.guest_gnode_level = guest_gnode_level;
                this.new_gnode_level = new_gnode_level;
                this.prev_id = prev_id;
                descriptor = new Descriptor(this);
                database = new HashMap<int,string>();
                on_startup();
            }

            public static string get_default_for_key(int key)
            {
                return "";
            }

            public static const int add_segment_timeout_exec = 2000;
            public string add_segment(int key, string data)
            {
                if (! database.has_key(key))
                    database[key] = Servant.get_default_for_key(key);
                database[key] = database[key] + data;
                return database[key];
            }

            private void replica_value(int key, string data)
            {
                database[key] = data;
            }

            private void on_startup()
            {
                IFixedKeysDatabaseDescriptor? prev_id_fkdd = null;
                if (prev_id != null) prev_id_fkdd = prev_id.descriptor;
                databases.fixed_keys_db_on_startup
                    (/*fkdd               = */ descriptor,
                     /*p_id               = */ 0,
                     /*guest_gnode_level  = */ guest_gnode_level,
                     /*new_gnode_level    = */ new_gnode_level,
                     /*prev_id_fkdd       = */ prev_id_fkdd);
            }

            public IPeersResponse on_request(IPeersRequest r, int common_lvl)
            throws PeersRefuseExecutionError, PeersRedoFromStartError
            {
                return databases.fixed_keys_db_on_request
                    (/*fkdd               = */ descriptor,
                     r, common_lvl);
            }

            private IFixedKeysDatabaseDescriptor descriptor {public get; private set;}
            private class Descriptor : Object, IDatabaseDescriptor, IFixedKeysDatabaseDescriptor
            {
                public Descriptor(Servant t)
                {
                    this.t = t;
                }
                private Servant t;

                public bool is_valid_key(Object k)
                {
                    if (k is Key)
                    {
                        Key _k = (Key)k;
                        if (_k.key <= 0) return false;
                        if (_k.key > t.levels) return false;
                        return true;
                    }
                    return false;
                }

                public Gee.List<int> evaluate_hash_node(Object k)
                {
                    assert(is_valid_key(k));
                    Key _k = (Key)k;
                    return Client.tuple_hash(_k.key);
                }

                public bool key_equal_data(Object k1, Object k2)
                {
                    assert(is_valid_key(k1));
                    Key _k1 = (Key)k1;
                    int k1l = _k1.key;
                    assert(is_valid_key(k2));
                    Key _k2 = (Key)k2;
                    int k2l = _k2.key;
                    return k1l == k2l;
                }

                public uint key_hash_data(Object k)
                {
                    assert(is_valid_key(k));
                    Key _k = (Key)k;
                    int kl = _k.key;
                    return (uint)kl;
                }

                public bool is_valid_record(Object k, Object rec)
                {
                    if (k is Key)
                    {
                        if (rec is Record)
                        {
                            Key _k = (Key)k;
                            Record _rec = (Record)rec;
                            return _k.key == _rec.key;
                        }
                        return false;
                    }
                    return false;
                }

                public bool my_records_contains(Object k)
                {
                    assert(is_valid_key(k));
                    // is a fkdd: always contains
                    Key _k = (Key)k;
                    int kl = _k.key;
                    if (! t.database.has_key(kl))
                        t.database[kl] = Servant.get_default_for_key(kl);
                    return true;
                }

                public Object get_record_for_key(Object k)
                {
                    assert(is_valid_key(k));
                    assert(my_records_contains(k));
                    Key _k = (Key)k;
                    int kl = _k.key;
                    var ret = new Record();
                    ret.key = kl;
                    ret.data = t.database[kl];
                    return ret;
                }

                public void set_record_for_key(Object k, Object rec)
                {
                    assert(is_valid_key(k));
                    Key _k = (Key)k;
                    int kl = _k.key;
                    assert(is_valid_record(k, rec));
                    Record _rec = (Record)rec;
                    t.database[kl] = _rec.data;
                }


                public Object get_key_from_request(IPeersRequest r)
                {
                    if (r is AddSegmentRequest)
                    {
                        AddSegmentRequest _r = (AddSegmentRequest)r;
                        Key ret = new Key();
                        ret.key = _r.key;
                        return ret;
                    }
                    else if (r is ReplicaRequest)
                    {
                        ReplicaRequest _r = (ReplicaRequest)r;
                        Key ret = new Key();
                        ret.key = _r.key;
                        return ret;
                    }
                    else assert_not_reached();
                }

                public int get_timeout_exec(IPeersRequest r)
                {
                    if (r is AddSegmentRequest)
                    {
                        return add_segment_timeout_exec;
                    }
                    else if (r is ReplicaRequest)
                    {
                        // not insert or update => should not request
                        assert_not_reached();
                    }
                    else assert_not_reached();
                }

                public bool is_insert_request(IPeersRequest r)
                {
                    // no requests of this type in this service
                    return false;
                }

                public bool is_read_only_request(IPeersRequest r)
                {
                    // no requests of this type in this service
                    return false;
                }

                public bool is_update_request(IPeersRequest r)
                {
                    if (r is AddSegmentRequest)
                    {
                        AddSegmentRequest _r = (AddSegmentRequest)r;
                        if (_r.key <= 0) return false;
                        if (_r.key > t.levels) return false;
                        return true;
                    }
                    return false;
                }

                public bool is_replica_value_request(IPeersRequest r)
                {
                    if (r is ReplicaRequest)
                    {
                        ReplicaRequest _r = (ReplicaRequest)r;
                        if (_r.key <= 0) return false;
                        if (_r.key > t.levels) return false;
                        return true;
                    }
                    return false;
                }

                public bool is_replica_delete_request(IPeersRequest r)
                {
                    // no requests of this type in this service
                    return false;
                }

                public IPeersResponse prepare_response_not_found(IPeersRequest r)
                {
                    // no requests of this type in this service
                    assert_not_reached();
                }

                public IPeersResponse prepare_response_not_free(IPeersRequest r, Object rec)
                {
                    // no requests of this type in this service
                    assert_not_reached();
                }

                public IPeersResponse execute(IPeersRequest r) throws PeersRefuseExecutionError, PeersRedoFromStartError
                {
                    if (r is AddSegmentRequest)
                    {
                        AddSegmentRequest _r = (AddSegmentRequest)r;
                        AddSegmentResponse ret = new AddSegmentResponse();
                        ret.data = t.add_segment(_r.key, _r.segment);
                        {
                            // launch a tasklet to make replicas
                            MakeReplicasTasklet ts = new MakeReplicasTasklet();
                            ts.t = this;
                            ts.key = _r.key;
                            ts.data = t.database[_r.key];
                            tasklet.spawn(ts);
                        }
                        return ret;
                    }
                    else if (r is ReplicaRequest)
                    {
                        ReplicaRequest _r = (ReplicaRequest)r;
                        t.replica_value(_r.key, _r.data);
                        return new ReplicaOkResponse();
                    }
                    else assert_not_reached();
                }
                private class MakeReplicasTasklet : Object, ITaskletSpawnable
                {
                    public Descriptor t;
                    public int key;
                    public string data;
                    public void * func()
                    {
                        t.make_replicas_tasklet(key, data);
                        return null;
                    }
                }
                private void make_replicas_tasklet(int key, string data)
                {
                    t.client.make_replicas(key, data);
                }


                private DatabaseHandler dh;
                public unowned DatabaseHandler dh_getter()
                {
                    return dh;
                }
                public void dh_setter(DatabaseHandler x)
                {
                    dh = x;
                }


                public Gee.List<Object> get_full_key_domain()
                {
                    var ret = new ArrayList<Object>();
                    for (int i = 1; i <= t.levels; i++)
                    {
                        var k = new Key();
                        k.key = i;
                        ret.add(k);
                    }
                    return ret;
                }

                public Object get_default_record_for_key(Object k)
                {
                    assert(is_valid_key(k));
                    Key _k = (Key)k;
                    int kl = _k.key;
                    var ret = new Record();
                    ret.key = kl;
                    ret.data = Servant.get_default_for_key(kl);
                    return ret;
                }
            }
        }

        class Client : Object
        {
            public Databases.Databases databases;

            public Client
            (Databases.Databases databases)
            {
                this.databases = databases;
            }

            public void make_replicas(int key, string data)
            {
                ReplicaRequest request = new ReplicaRequest();
                request.key = key;
                request.data = data;

                IPeersResponse? resp;
                IReplicaContinuation cont;
                bool ret = databases.begin_replica(9, 0, tuple_hash(key),
                                                   request, 1000, out resp, out cont);
                while (ret)
                {
                    ret = databases.next_replica(cont, out resp);
                }
            }

            public static Gee.List<int> tuple_hash(int kl)
            {
                var ret = new ArrayList<int>();
                for (int i = 0; i < kl; i++) ret.add(0);
                return ret;
            }
        }
    }

    SimNode node_a;
    SimNode node_b;
    SimNode node_c;
    SimNode node_d;
    SimNode node_e;
    SimNode node_f;
    SimNode node_g;
    SimNode node_h;
    Network net1;
    Network net2;

    Gee.List<int> gsizes;
    int levels;

    public void prepare_network_1()
    {
        gsizes = new ArrayList<int>.wrap({2,2,2,2});
        levels = gsizes.size;
        net1 = new Network();
        var h00 = new HCoord(0,0);
        var h01 = new HCoord(0,1);
        var h10 = new HCoord(1,0);
        var h11 = new HCoord(1,1);
        var h20 = new HCoord(2,0);
        var h21 = new HCoord(2,1);
        var h30 = new HCoord(3,0);
        var h31 = new HCoord(3,1);
        // g-node 1:1:0
        node_a = new SimNode("a", new ArrayList<int>.wrap({1,0,1,1}), net1);
        node_b = new SimNode("b", new ArrayList<int>.wrap({0,0,1,1}), net1);
        assert(node_a.exists_gnode(h00));
        node_a.add_gateway_to_gnode(node_b, h00);
        assert(node_b.exists_gnode(h01));
        node_b.add_gateway_to_gnode(node_a, h01);
    }

    public void prepare_network_2()
    {
        net2 = new Network();
        var h00 = new HCoord(0,0);
        var h01 = new HCoord(0,1);
        var h10 = new HCoord(1,0);
        var h11 = new HCoord(1,1);
        var h20 = new HCoord(2,0);
        var h21 = new HCoord(2,1);
        var h30 = new HCoord(3,0);
        var h31 = new HCoord(3,1);
        // g-node 1:1:1, also 1:1
        node_c = new SimNode("c", new ArrayList<int>.wrap({1,1,1,1}), net2);
        // g-node 1:0:1
        node_e = new SimNode("e", new ArrayList<int>.wrap({1,1,0,1}), net2);
        node_h = new SimNode("h", new ArrayList<int>.wrap({0,1,0,1}), net2);
        assert(node_e.exists_gnode(h00));
        node_e.add_gateway_to_gnode(node_h, h00);
        assert(node_h.exists_gnode(h01));
        node_h.add_gateway_to_gnode(node_e, h01);
        // g-node 1:0:0
        node_f = new SimNode("f", new ArrayList<int>.wrap({1,0,0,1}), net2);
        node_g = new SimNode("g", new ArrayList<int>.wrap({0,0,0,1}), net2);
        assert(node_f.exists_gnode(h00));
        node_f.add_gateway_to_gnode(node_g, h00);
        assert(node_g.exists_gnode(h01));
        node_g.add_gateway_to_gnode(node_f, h01);
        // g-node 1:0
        assert(node_h.exists_gnode(h10));
        node_h.add_gateway_to_gnode(node_e, h10);
        assert(node_e.exists_gnode(h10));
        node_e.add_gateway_to_gnode(node_f, h10);
        assert(node_f.exists_gnode(h11));
        node_f.add_gateway_to_gnode(node_e, h11);
        assert(node_g.exists_gnode(h11));
        node_g.add_gateway_to_gnode(node_f, h11);
        // g-node 1
        assert(node_c.exists_gnode(h20));
        node_c.add_gateway_to_gnode(node_e, h20);
        assert(node_e.exists_gnode(h21));
        node_e.add_gateway_to_gnode(node_c, h21);
        assert(node_h.exists_gnode(h21));
        node_h.add_gateway_to_gnode(node_e, h21);
        assert(node_f.exists_gnode(h21));
        node_f.add_gateway_to_gnode(node_e, h21);
        assert(node_g.exists_gnode(h21));
        node_g.add_gateway_to_gnode(node_f, h21);
        // g-node 0
        node_d = new SimNode("d", new ArrayList<int>.wrap({0,1,1,0}), net2);
        // whole net
        assert(node_c.exists_gnode(h30));
        node_c.add_gateway_to_gnode(node_d, h30);
        node_c.add_gateway_to_gnode(node_e, h30);
        assert(node_e.exists_gnode(h30));
        node_e.add_gateway_to_gnode(node_c, h30);
        node_e.add_gateway_to_gnode(node_f, h30);
        assert(node_h.exists_gnode(h30));
        node_h.add_gateway_to_gnode(node_e, h30);
        assert(node_f.exists_gnode(h30));
        node_f.add_gateway_to_gnode(node_g, h30);
        node_f.add_gateway_to_gnode(node_e, h30);
        assert(node_g.exists_gnode(h30));
        node_g.add_gateway_to_gnode(node_d, h30);
        node_g.add_gateway_to_gnode(node_f, h30);
        assert(node_d.exists_gnode(h31));
        node_d.add_gateway_to_gnode(node_c, h31);
        node_d.add_gateway_to_gnode(node_g, h31);
    }

    class Network : Object
    {
        public Network()
        {
            nodes = new HashMap<string,SimNode>();
        }

        public HashMap<string,SimNode> nodes;
    }

    class TupleStub : Object
    {
        public TupleStub(SimNode node, bool inside_min_common_gnode=true)
        {
            this.node = node;
            this.inside_min_common_gnode = inside_min_common_gnode;
        }
        public SimNode node;
        public bool inside_min_common_gnode;
    }

    class SimNode : Object
    {
        public string name;
        public Gee.List<int> pos;
        public HashMap<HCoord, Gee.List<SimNode>> network_by_hcoord;
        public HashMap<string,TupleStub> stub_by_tuple;
        private MessageRouting.MessageRouting message_routing;
        private Databases.Databases databases;
        private Service00.Client s00_client;
        private Service00.Servant s00_servant;
        private int guest_gnode_level;
        private int new_gnode_level;
        private SimNode? prev_id;

        public SimNode
        (string name,
         Gee.List<int> pos, Network net,
         int guest_gnode_level=-1, int new_gnode_level=-1, SimNode? prev_id=null)
        {
            if (new_gnode_level == -1) new_gnode_level = levels;
            assert(pos.size == levels);
            for (int i = 0; i < levels; i++)
            {
                assert(pos[i] >= 0);
                assert(pos[i] < gsizes[i]);
            }

            this.name = name;
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.guest_gnode_level = guest_gnode_level;
            this.new_gnode_level = new_gnode_level;
            this.prev_id = prev_id;
            network_by_hcoord =
                new HashMap<HCoord, Gee.List<SimNode>>
                (/* key_hash_func  = */(a) => @"$(a.lvl)_$(a.pos)".hash(),
                 /* key_equal_func = */(a, b) => a.equals(b));
            stub_by_tuple = new HashMap<string,TupleStub>();

            foreach (SimNode other in net.nodes.values)
            {
                other.add_knowledge_node(this);
                add_knowledge_node(other);
            }

            net.nodes[name] = this;

            message_routing = new MessageRouting.MessageRouting
                (pos, gsizes,
                 /* gnode_exists                  = */  (/*int*/ lvl, /*int*/ pos) => {
                     return exists_gnode(new HCoord(lvl,pos));
                 },
                 /* get_gateway                   = */  (/*int*/ level, /*int*/ pos,
                                                         /*CallerInfo?*/ received_from,
                                                         /*IPeersManagerStub?*/ failed) => {
                     IPeersManagerStub? ret = null;

                     // In this testcase we assume no failures in passing message to a neighbor
                     assert(failed == null);

                     HCoord dest = new HCoord(level,pos);
                     if (exists_gnode(dest))
                     {
                         SimNode? gw = null;
                         SimNode? prev = null;

                         if (received_from != null)
                         {
                             FakeCallerInfo _received_from = (FakeCallerInfo)received_from;
                             prev = _received_from.node;
                         }

                         int i = 0;
                         while (network_by_hcoord[dest].size > i)
                         {
                             SimNode this_gw = network_by_hcoord[dest][i];
                             if (this_gw == prev) i++;
                             else
                             {
                                 gw = this_gw;
                                 break;
                             }
                         }
                         if (gw != null)
                             ret = new FakeUnicastStub.target_by_gateway(gw, this);
                     }

                     return ret;
                 },
                 /* get_client_internally         = */  (/*PeerTupleNode*/ n) => {
                     IPeersManagerStub ret = null;

                     var addr = address(n.tuple);
                     var tstub = stub_by_tuple[addr];
                     ret = new FakeUnicastStub.target_internally(tstub, this);
                     return ret;
                 },
                 /* get_nodes_in_my_group         = */  (/*int*/ lvl) => {
                     return nodes_inside_my_gnode(lvl);
                 },
                 /* my_gnode_participates         = */  (/*int*/ p_id, /*int*/ lvl) => {
                     // All services are `strict` in this testcase
                     return true;
                 },
                 /* get_non_participant_gnodes    = */  (/*int*/ p_id, /*int*/ target_levels) => {
                     // All services are `strict` in this testcase
                     return new ArrayList<HCoord>();
                 },
                 /* exec_service                  = */  (/*int*/ p_id, /*IPeersRequest*/ req,
                                                         /*Gee.List<int>*/ client_tuple) => {
                     IPeersResponse ret;
                     // Could throw PeersRefuseExecutionError, PeersRedoFromStartError.
                     if (p_id == 0)
                     {
                         int common_lvl = client_tuple.size;
                         string classname = req.get_type().name();
                         string client = address(client_tuple);
                         string me = address(pos);
                         debug(@"$(me): executing request $(classname) from client {$(client)}");
                         if (req is Service00.AddSegmentRequest)
                             debug(@"when executing add_segment client was {$(address(client_tuple))}");
                         ret = s00_servant.on_request(req, common_lvl);
                     }
                     else if (p_id == 1)
                     {
                         error("not implemented yet");
                     }
                     else assert_not_reached();
                     return ret;
                 });

            databases = new Databases.Databases
                (pos, gsizes,
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
                     assert(p_id == 1 || p_id == 0); // The only services simulated in this testcase
                     // void
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
                     return nodes_inside_my_gnode(lvl);
                 });

            s00_client = new Service00.Client(databases);
            if (prev_id == null)
                s00_servant = new Service00.Servant
                    (s00_client, databases, levels);
            else
                s00_servant = new Service00.Servant
                    (s00_client, databases, levels,
                     guest_gnode_level, new_gnode_level, prev_id.s00_servant);
        }

        private bool is_service_optional(int p_id)
        {
            assert(p_id == 1 || p_id == 0); // The only services simulated in this testcase
            return false;
        }

        private void wait_participation_maps(int target_levels)
        {
            assert_not_reached(); // The only services simulated in this testcase are not optional.
        }

        public string srv00_add_segment(int key, string segment)
        {
            int p_id = 0;
            PeerTupleNode x_macron =
                new PeerTupleNode(
                Service00.Client.tuple_hash(key));
            bool optional = is_service_optional(p_id);
            if (optional) wait_participation_maps(x_macron.tuple.size);
            var request = new Service00.AddSegmentRequest();
            request.key = key;
            request.segment = segment;
            int timeout_exec = Service00.Servant.add_segment_timeout_exec;
            PeerTupleNode? respondant;
            var iresp = message_routing.contact_peer
                (p_id,
                 optional,
                 x_macron,
                 request,
                 timeout_exec,
                 false,
                 out respondant);
            debug(@"respondant of add_segment was $(address(respondant.tuple))");
            assert(iresp is Service00.AddSegmentResponse);
            Service00.AddSegmentResponse resp = (Service00.AddSegmentResponse)iresp;
            return resp.data;
        }

        private void add_knowledge_node(SimNode other)
        {
            HCoord g = find_hcoord(pos, other.pos);
            if (! (g in network_by_hcoord.keys)) network_by_hcoord[g] = new ArrayList<SimNode>();
            // most internal tuple
            Gee.List<int> internal_tuple = new ArrayList<int>();
            int i = 0;
            for (; i <= g.lvl; i++) internal_tuple.add(other.pos[i]);
            string _address = address(internal_tuple);
            stub_by_tuple[_address] = new TupleStub(other);
            // wider
            for (; i < levels; i++)
            {
                internal_tuple.add(other.pos[i]);
                _address = address(internal_tuple);
                stub_by_tuple[_address] = new TupleStub(other, false);
            }
        }

        public void add_gateway_to_gnode(SimNode gw, HCoord g)
        {
            network_by_hcoord[g].add(gw);
        }

        public bool exists_gnode(HCoord g)
        {
            return network_by_hcoord.has_key(g);
        }

        public int nodes_inside_my_gnode(int level)
        {
            int count = 0;
            foreach (var e in stub_by_tuple.entries)
            {
                string addr = e.key;
                TupleStub stub = e.@value;
                if (stub.inside_min_common_gnode)
                {
                    int s_pos_cur = 0;
                    int lvl = 0;
                    while (true)
                    {
                        int s_pos = addr.index_of(":", s_pos_cur);
                        if (s_pos == -1) break;
                        s_pos_cur = s_pos+1;
                        lvl++;
                    }
                    if (lvl < level) count++;
                }
            }
            return count;
        }

        public void rpc_forward_peer_message(PeerMessageForwarder mf, SimNode caller)
        {
            // check if mf.p_id is optional
            // In this testcase is always false.
            bool optional = is_service_optional(mf.p_id);
            int maps_retrieved_below_level = levels;
            // prepare CallerInfo
            FakeCallerInfo caller_info = new FakeCallerInfo(caller);
            // Call method of message_routing.
            message_routing.forward_msg(mf, optional, maps_retrieved_below_level, caller_info);
            // Done.
            if (optional)
            {
                // not needed in this testcase, we should now
                foreach (PeerTupleGNode t in mf.non_participant_tuple_list)
                {
                    // ... start tasklet and call message_routing.check_non_participation
                    // then update participation maps.
                }
            }
        }

        public IPeersRequest rpc_get_request
        (int msg_id, IPeerTupleNode respondant)
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

        public void rpc_set_response
        (int msg_id, IPeersResponse response, IPeerTupleNode respondant)
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

        public void rpc_set_failure
        (int msg_id, IPeerTupleGNode tuple)
        {
            // check that interfaces are ok
            if (!(tuple is PeerTupleGNode))
            {
                warning("bad request rpc: set_failure, invalid tuple.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_failure(msg_id, (PeerTupleGNode)tuple);
            // Done.
        }

        public void rpc_set_next_destination
        (int msg_id, IPeerTupleGNode tuple)
        {
            // check that interfaces are ok
            if (!(tuple is PeerTupleGNode))
            {
                warning("bad request rpc: set_next_destination, invalid tuple.");
                tasklet.exit_tasklet();
            }
            // Call method of message_routing.
            message_routing.set_next_destination(msg_id, (PeerTupleGNode)tuple);
            // Done.
        }
    }

    class FakeCallerInfo : CallerInfo
    {
        public SimNode node;
        public FakeCallerInfo(SimNode node)
        {
            this.node = node;
        }
    }

    class FakeUnicastStub : Object, IPeersManagerStub
    {
        private SimNode caller;
        private TupleStub? internally;
        public FakeUnicastStub.target_internally(TupleStub internally, SimNode caller)
        {
            this.internally = internally;
            this.by_gateway = null;
            this.caller = caller;
        }

        private SimNode? by_gateway;
        public FakeUnicastStub.target_by_gateway(SimNode by_gateway, SimNode caller)
        {
            this.by_gateway = by_gateway;
            this.caller = caller;
            this.internally = null;
        }

        public IPeerParticipantSet ask_participant_maps () throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void forward_peer_message (IPeerMessage peer_message) throws StubError, DeserializeError
        {
            assert(by_gateway != null);
            assert(internally == null);
            // This is a stub that sends a message in unicast (reliable, no wait).

            // Here we could simulate StubError
            tasklet.ms_wait(2); // simulates network latency
            ForwardPeerMessageTasklet ts = new ForwardPeerMessageTasklet();
            ts.t = this;
            ts.by_gateway = by_gateway;
            ts.peer_message = (PeerMessageForwarder)peer_message;
            ts.caller = caller;
            tasklet.spawn(ts);
        }
        private class ForwardPeerMessageTasklet : Object, ITaskletSpawnable
        {
            public FakeUnicastStub t;
            public SimNode by_gateway;
            public PeerMessageForwarder peer_message;
            public SimNode caller;
            public void * func()
            {
                by_gateway.rpc_forward_peer_message(peer_message, caller);
                return null;
            }
        }

        public IPeersRequest get_request (int msg_id, IPeerTupleNode respondant) throws PeersUnknownMessageError, PeersInvalidRequest, StubError, DeserializeError
        {
            assert(by_gateway == null);
            assert(internally != null);
            // This is a stub that connects via TCP and waits for answer.

            // Here we could simulate StubError
            tasklet.ms_wait(2); // simulates network latency
            if (! internally.inside_min_common_gnode) warning("Tuple in message_forwarder is wider than expected");
            SimNode srv_client = internally.node;
            return srv_client.rpc_get_request(msg_id, respondant);
        }

        public void give_participant_maps (IPeerParticipantSet maps) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_failure (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            assert(by_gateway == null);
            assert(internally != null);
            // This is a stub that connects via TCP and waits for answer.

            // Here we could simulate StubError
            tasklet.ms_wait(2); // simulates network latency
            if (! internally.inside_min_common_gnode) warning("Tuple in message_forwarder is wider than expected");
            SimNode srv_client = internally.node;
            srv_client.rpc_set_failure(msg_id, tuple);
        }

        public void set_missing_optional_maps (int msg_id) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_next_destination (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            assert(by_gateway == null);
            assert(internally != null);
            // This is a stub that connects via TCP and waits for answer.

            // Here we could simulate StubError
            tasklet.ms_wait(2); // simulates network latency
            if (! internally.inside_min_common_gnode) warning("Tuple in message_forwarder is wider than expected");
            SimNode srv_client = internally.node;
            srv_client.rpc_set_next_destination(msg_id, tuple);
        }

        public void set_non_participant (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_participant (int p_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_redo_from_start (int msg_id, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_refuse_message (int msg_id, string refuse_message, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_response (int msg_id, IPeersResponse response, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            assert(by_gateway == null);
            assert(internally != null);
            // This is a stub that connects via TCP and waits for answer.

            // Here we could simulate StubError
            tasklet.ms_wait(2); // simulates network latency
            if (! internally.inside_min_common_gnode) warning("Tuple in message_forwarder is wider than expected");
            SimNode srv_client = internally.node;
            srv_client.rpc_set_response(msg_id, response, respondant);
        }
    }
}

class Entering : Object
{
    public void test_entering()
    {
        string data;
        print("\nPrepare network net1...\n");
        prepare_network_1();
        print("Network net1 ready.\n");
        tasklet.ms_wait(10);

        // node_a makes a request
        data = node_a.srv00_add_segment(1, "abcd");
        print(@"node_a: srv00 key 1: '$(data)'.\n");
        tasklet.ms_wait(10);
        // then node_b makes a request
        data = node_b.srv00_add_segment(1, "efg");
        print(@"node_b: srv00 key 1: '$(data)'.\n");
        tasklet.ms_wait(10);
        // node_a makes a request at level 4
        data = node_a.srv00_add_segment(4, "pippo");
        print(@"node_a: srv00 key 4: '$(data)'.\n");
        tasklet.ms_wait(10);
        // then node_b verifies
        data = node_b.srv00_add_segment(4, "");
        print(@"node_b: srv00 key 4: '$(data)'.\n");
        tasklet.ms_wait(10);

        print("Prepare network net2...\n");
        prepare_network_2();
        print("Network net2 ready.\n");
        tasklet.ms_wait(10);

        // node_c makes a request at level 2
        data = node_c.srv00_add_segment(2, "qqqq");
        print(@"node_c: srv00 key 2: '$(data)'.\n");
        tasklet.ms_wait(10);
        // node_e makes a request at level 2
        data = node_e.srv00_add_segment(2, "wwww");
        print(@"node_e: srv00 key 2: '$(data)'.\n");
        tasklet.ms_wait(10);
        // node_e makes a request at level 4
        data = node_e.srv00_add_segment(4, "zzzz");
        print(@"node_e: srv00 key 4: '$(data)'.\n");
        tasklet.ms_wait(10);

        print("Merge networks: net1 enters net2...\n");
        var node_a_old = node_a;
    }
}