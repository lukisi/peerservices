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

string json_string_object(Object obj)
{
    Json.Node n = Json.gobject_serialize(obj);
    Json.Generator g = new Json.Generator();
    g.root = n;
    g.pretty = true;
    string ret = g.to_data(null);
    return ret;
}

Object dup_object(Object obj)
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

class PeersTester : Object
{
    public void set_up ()
    {
    }

    public void tear_down ()
    {
    }

    public void test_approximate()
    {
        Gee.List<int> gsizes = new ArrayList<int>.wrap({5,5,5});
        int levels = gsizes.size;
        Gee.List<PeerTupleNode> nodes_in_network = new ArrayList<PeerTupleNode>();
        nodes_in_network.add(new PeerTupleNode(new ArrayList<int>.wrap({3,1,0})));
        nodes_in_network.add(new PeerTupleNode(new ArrayList<int>.wrap({2,1,0})));
        nodes_in_network.add(new PeerTupleNode(new ArrayList<int>.wrap({1,1,0})));
        nodes_in_network.add(new PeerTupleNode(new ArrayList<int>.wrap({0,1,3})));
        nodes_in_network.add(new PeerTupleNode(new ArrayList<int>.wrap({1,2,0})));
        nodes_in_network.add(new PeerTupleNode(new ArrayList<int>.wrap({1,3,0})));
        foreach (var node in nodes_in_network)
        {
            MessageRouting.MessageRouting m =
                new MessageRouting.MessageRouting
                (node.tuple, gsizes,
                 /*gnode_exists*/ (lvl, pos) => {
                     var gnode = Utils.make_tuple_gnode(node.tuple, new HCoord(lvl, pos), levels);
                     foreach (var node2 in nodes_in_network) {
                         var gnode2 = new PeerTupleGNode(node2.tuple, levels);
                         if (Utils.contains(gnode, gnode2)) return true;
                     }
                     return false;
                 },
                 (level, pos, received_from, failed) => null, /*get_gateway is not needed in this test*/
                 (n) => null, /*get_client_internally is not needed in this test*/
                 (lvl) => 1, /*get_nodes_in_my_group is not needed in this test*/
                 (p_id, level) => false, /*my_gnode_participates is not needed in this test*/
                 (p_id, target_levels) => null, /*get_non_participant_gnodes is not needed in this test*/
                 (p_id, req, client_tuple) => null /*exec_service is not needed in this test*/
                 );
            var x_macron = new PeerTupleNode(new ArrayList<int>.wrap({1,0,0}));
            var exclude_list = new ArrayList<HCoord>();
            var h = m.approximate(x_macron, exclude_list);
            print(@"($(h.lvl),$(h.pos))\n");
        }
        {
            // target is 4:2:2
            // given the nodes_in_network, the one is 0:2:1
            // how is it represented by 0:1:3?
            var node = new PeerTupleNode(new ArrayList<int>.wrap({3,1,0}));
            MessageRouting.MessageRouting m =
                new MessageRouting.MessageRouting
                (node.tuple, gsizes,
                 /*gnode_exists*/ (lvl, pos) => {
                     var gnode = Utils.make_tuple_gnode(node.tuple, new HCoord(lvl, pos), levels);
                     foreach (var node2 in nodes_in_network) {
                         var gnode2 = new PeerTupleGNode(node2.tuple, levels);
                         if (Utils.contains(gnode, gnode2)) return true;
                     }
                     return false;
                 },
                 (level, pos, received_from, failed) => null, /*get_gateway is not needed in this test*/
                 (n) => null, /*get_client_internally is not needed in this test*/
                 (lvl) => 1, /*get_nodes_in_my_group is not needed in this test*/
                 (p_id, level) => false, /*my_gnode_participates is not needed in this test*/
                 (p_id, target_levels) => null, /*get_non_participant_gnodes is not needed in this test*/
                 (p_id, req, client_tuple) => null /*exec_service is not needed in this test*/
                 );
            var x_macron = new PeerTupleNode(new ArrayList<int>.wrap({2,2,4}));
            var exclude_list = new ArrayList<HCoord>();
            var h = m.approximate(x_macron, exclude_list);
            assert(h.lvl == 1);
            assert(h.pos == 2);
        }
    }

    public void test_dist()
    {
        Gee.List<int> gsizes = new ArrayList<int>.wrap({5,5,5});
        Gee.List<int> my_pos = new ArrayList<int>.wrap({3,1,0});
        Gee.List<int> x_pos = new ArrayList<int>.wrap({2,1,0});
        Gee.List<int> y_pos = new ArrayList<int>.wrap({1,1,0});
        Gee.List<int> z_pos = new ArrayList<int>.wrap({0,1,0});
        Gee.List<int> ga_pos = new ArrayList<int>.wrap({1,2,0});
        Gee.List<int> gb_pos = new ArrayList<int>.wrap({1,0,0});
        MessageRouting.MessageRouting m =
            new MessageRouting.MessageRouting
            (my_pos, gsizes,
             (lvl, pos) => false, /*gnode_exists is not needed in this test*/
             (level, pos, received_from, failed) => null, /*get_gateway is not needed in this test*/
             (n) => null, /*get_client_internally is not needed in this test*/
             (lvl) => 1, /*get_nodes_in_my_group is not needed in this test*/
             (p_id, level) => false, /*my_gnode_participates is not needed in this test*/
             (p_id, target_levels) => null, /*get_non_participant_gnodes is not needed in this test*/
             (p_id, req, client_tuple) => null /*exec_service is not needed in this test*/
             );
        PeerTupleNode x = new PeerTupleNode(x_pos);
        PeerTupleNode y = new PeerTupleNode(y_pos);
        PeerTupleNode z = new PeerTupleNode(z_pos);
        PeerTupleNode ga = new PeerTupleNode(ga_pos);
        PeerTupleNode gb = new PeerTupleNode(gb_pos);
        print("\n");
        print(@"gsizes = $(address(gsizes))\n");
        print(@"my_pos = $(address(my_pos))\n");
        print(@"x = $(address(x.tuple))\n");
        print(@"y = $(address(y.tuple))\n");
        print(@"z = $(address(z.tuple))\n");
        print(@"ga = $(address(ga.tuple))\n");
        print(@"gb = $(address(gb.tuple))\n");
        print(@"dist(y,x) = $(m.dist(y,x))\n");
        print(@"dist(y,z) = $(m.dist(y,z))\n");
        print(@"dist(y,ga) = $(m.dist(y,ga))\n");
        print(@"dist(y,gb) = $(m.dist(y,gb))\n");
        // Any delta at level 1 is always bigger than any delta at level 0.
        assert(m.dist(y,x) < m.dist(y,ga));
        // The same delta at the same level but in the opposite direction.
        // Also in this case the "dist" differ.
        assert(m.dist(y,x) != m.dist(y,z));
    }

    public void test_routing1()
    {
        gsizes = new ArrayList<int>.wrap({2,2,2,2});
        network = new HashMap<string,SimNode>();
        var h00 = new HCoord(0,0);
        var h01 = new HCoord(0,1);
        var h10 = new HCoord(1,0);
        var h11 = new HCoord(1,1);
        var h20 = new HCoord(2,0);
        var h21 = new HCoord(2,1);
        var h30 = new HCoord(3,0);
        var h31 = new HCoord(3,1);
        // g-node 1:1:0
        var node_a = new SimNode(this, "a", new ArrayList<int>.wrap({1,0,1,1}));
        var node_b = new SimNode(this, "b", new ArrayList<int>.wrap({0,0,1,1}));
        assert(node_a.network_by_hcoord.has_key(h00));
        node_a.network_by_hcoord[h00].add(node_b);
        assert(node_b.network_by_hcoord.has_key(h01));
        node_b.network_by_hcoord[h01].add(node_a);
        // g-node 1:1:1
        var node_c = new SimNode(this, "c", new ArrayList<int>.wrap({1,1,1,1}));
        // g-node 1:1
        assert(node_a.network_by_hcoord.has_key(h11));
        node_a.network_by_hcoord[h11].add(node_b);
        assert(node_b.network_by_hcoord.has_key(h11));
        node_b.network_by_hcoord[h11].add(node_c);
        assert(node_c.network_by_hcoord.has_key(h10));
        node_c.network_by_hcoord[h10].add(node_b);
        // g-node 1:0:1
        var node_e = new SimNode(this, "e", new ArrayList<int>.wrap({1,1,0,1}));
        var node_h = new SimNode(this, "h", new ArrayList<int>.wrap({0,1,0,1}));
        assert(node_e.network_by_hcoord.has_key(h00));
        node_e.network_by_hcoord[h00].add(node_h);
        assert(node_h.network_by_hcoord.has_key(h01));
        node_h.network_by_hcoord[h01].add(node_e);
        // g-node 1:0:0
        var node_f = new SimNode(this, "f", new ArrayList<int>.wrap({1,0,0,1}));
        var node_g = new SimNode(this, "g", new ArrayList<int>.wrap({0,0,0,1}));
        assert(node_f.network_by_hcoord.has_key(h00));
        node_f.network_by_hcoord[h00].add(node_g);
        assert(node_g.network_by_hcoord.has_key(h01));
        node_g.network_by_hcoord[h01].add(node_f);
        // g-node 1:0
        assert(node_h.network_by_hcoord.has_key(h10));
        node_h.network_by_hcoord[h10].add(node_e);
        assert(node_e.network_by_hcoord.has_key(h10));
        node_e.network_by_hcoord[h10].add(node_f);
        assert(node_f.network_by_hcoord.has_key(h11));
        node_f.network_by_hcoord[h11].add(node_e);
        assert(node_g.network_by_hcoord.has_key(h11));
        node_g.network_by_hcoord[h11].add(node_f);
        // g-node 1
        assert(node_a.network_by_hcoord.has_key(h20));
        node_a.network_by_hcoord[h20].add(node_e);
        node_a.network_by_hcoord[h20].add(node_h);
        node_a.network_by_hcoord[h20].add(node_b);
        assert(node_b.network_by_hcoord.has_key(h20));
        node_b.network_by_hcoord[h20].add(node_e);
        node_b.network_by_hcoord[h20].add(node_a);
        node_b.network_by_hcoord[h20].add(node_c);
        assert(node_c.network_by_hcoord.has_key(h20));
        node_c.network_by_hcoord[h20].add(node_e);
        node_c.network_by_hcoord[h20].add(node_b);
        assert(node_e.network_by_hcoord.has_key(h21));
        node_e.network_by_hcoord[h21].add(node_a);
        node_e.network_by_hcoord[h21].add(node_b);
        node_e.network_by_hcoord[h21].add(node_c);
        node_e.network_by_hcoord[h21].add(node_h);
        assert(node_h.network_by_hcoord.has_key(h21));
        node_h.network_by_hcoord[h21].add(node_e);
        node_h.network_by_hcoord[h21].add(node_a);
        assert(node_f.network_by_hcoord.has_key(h21));
        node_f.network_by_hcoord[h21].add(node_e);
        assert(node_g.network_by_hcoord.has_key(h21));
        node_g.network_by_hcoord[h21].add(node_f);
        // g-node 0
        var node_d = new SimNode(this, "d", new ArrayList<int>.wrap({0,1,1,0}));
        // whole net
        assert(node_a.network_by_hcoord.has_key(h30));
        node_a.network_by_hcoord[h30].add(node_b);
        node_a.network_by_hcoord[h30].add(node_e);
        node_a.network_by_hcoord[h30].add(node_h);
        assert(node_b.network_by_hcoord.has_key(h30));
        node_b.network_by_hcoord[h30].add(node_c);
        node_b.network_by_hcoord[h30].add(node_e);
        node_b.network_by_hcoord[h30].add(node_a);
        assert(node_c.network_by_hcoord.has_key(h30));
        node_c.network_by_hcoord[h30].add(node_d);
        node_c.network_by_hcoord[h30].add(node_e);
        node_c.network_by_hcoord[h30].add(node_b);
        assert(node_e.network_by_hcoord.has_key(h30));
        node_e.network_by_hcoord[h30].add(node_c);
        node_e.network_by_hcoord[h30].add(node_f);
        node_e.network_by_hcoord[h30].add(node_b);
        node_e.network_by_hcoord[h30].add(node_a);
        node_e.network_by_hcoord[h30].add(node_h);
        assert(node_h.network_by_hcoord.has_key(h30));
        node_h.network_by_hcoord[h30].add(node_e);
        node_h.network_by_hcoord[h30].add(node_a);
        assert(node_f.network_by_hcoord.has_key(h30));
        node_f.network_by_hcoord[h30].add(node_g);
        node_f.network_by_hcoord[h30].add(node_e);
        assert(node_g.network_by_hcoord.has_key(h30));
        node_g.network_by_hcoord[h30].add(node_d);
        node_g.network_by_hcoord[h30].add(node_f);
        assert(node_d.network_by_hcoord.has_key(h31));
        node_d.network_by_hcoord[h31].add(node_c);
        node_d.network_by_hcoord[h31].add(node_g);
        // TODO
        LinkedList<string> lst_k = new LinkedList<string>();
        lst_k.add_all(network.keys);
        lst_k.sort();
        foreach (string k in lst_k)
        {
            SimNode n = network[k];
            print(@"Node '$(n.name)' (address $(address(n.pos))) has knowledge of g-nodes:\n");
            foreach (var e in n.network_by_hcoord.entries)
            {
                HCoord h = e.key;
                Gee.List<SimNode> gwlist = e.@value;
                print(@"   ($(h.lvl), $(h.pos)): $(gwlist.size) gateways for it.\n");
            }
        }
    }

    private static string address(Gee.List<int> pos)
    {
        string ret = ""; string next = "";
        foreach (int p in pos) {
            ret = @"$(p)$(next)$(ret)";
            next = ":";
        }
        return ret;
    }

    private static HCoord find_hcoord(Gee.List<int> me, Gee.List<int> other)
    {
        assert(me.size == other.size);
        var gn_other = new PeerTupleGNode(other, other.size);
        int @case;
        HCoord ret;
        Utils.convert_tuple_gnode(me, gn_other, out @case, out ret);
        return ret;
    }

    public static int main(string[] args)
    {
        GLib.Test.init(ref args);

        // Initialize tasklet system
        PthTaskletImplementer.init();
        tasklet = PthTaskletImplementer.get_tasklet_system();

        GLib.Test.add_func ("/MessageRouting/Dist", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_dist();
            x.tear_down();
        });
        GLib.Test.add_func ("/MessageRouting/Approximate", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_approximate();
            x.tear_down();
        });
        GLib.Test.add_func ("/MessageRouting/Routing_1", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_routing1();
            x.tear_down();
        });
        GLib.Test.run();
        return 0;
    }

    Gee.List<int> gsizes;
    int levels {
        get {
            return gsizes.size;
        }
    }
    HashMap<string,SimNode> network;

    class TupleStub : Object
    {
        public TupleStub(SimNode node, bool deprecated=false)
        {
            this.node = node;
            this.deprecated = deprecated;
        }
        public SimNode node;
        public bool deprecated;
    }

    class SimNode : Object
    {
        private PeersTester tester;
        public string name;
        public Gee.List<int> pos;
        public HashMap<HCoord, Gee.List<SimNode>> network_by_hcoord;
        public HashMap<string,TupleStub> stub_by_tuple;

        public SimNode(PeersTester tester, string name, Gee.List<int> pos)
        {
            assert(pos.size == tester.levels);
            for (int i = 0; i < tester.levels; i++)
            {
                assert(pos[i] >= 0);
                assert(pos[i] < tester.gsizes[i]);
            }

            this.tester = tester;
            this.name = name;
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            network_by_hcoord =
                new HashMap<HCoord, Gee.List<SimNode>>
                (/* key_hash_func  = */(a) => @"$(a.lvl)_$(a.pos)".hash(),
                 /* key_equal_func = */(a, b) => a.equals(b));
            stub_by_tuple = new HashMap<string,TupleStub>();

            foreach (SimNode other in tester.network.values)
            {
                other.add_knowledge_node(this);
                add_knowledge_node(other);
            }

            tester.network[name] = this;
        }

        public void add_knowledge_node(SimNode other)
        {
            HCoord g = find_hcoord(pos, other.pos);
            print(@"$(other.name) for $(name) is HCoord ($(g.lvl),$(g.pos)).\n");
            if (! (g in network_by_hcoord.keys)) network_by_hcoord[g] = new ArrayList<SimNode>();
            // most internal tuple
            Gee.List<int> internal_tuple = new ArrayList<int>();
            int i = 0;
            for (; i <= g.lvl; i++) internal_tuple.add(other.pos[i]);
            string _address = address(internal_tuple);
            stub_by_tuple[_address] = new TupleStub(other);
            print(@"$(other.name) for $(name) is usually $(_address).\n");
            // wider
            for (; i < tester.levels; i++)
            {
                internal_tuple.add(other.pos[i]);
                _address = address(internal_tuple);
                stub_by_tuple[_address] = new TupleStub(other, true);
                print(@"$(other.name) for $(name) is also $(_address), but deprecated.\n");
            }
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
        private TupleStub? internally;
        public FakeUnicastStub.target_internally(TupleStub internally)
        {
            this.internally = internally;
            this.by_gateway = null;
        }

        private SimNode? by_gateway;
        public FakeUnicastStub.target_by_gateway(SimNode by_gateway)
        {
            this.by_gateway = by_gateway;
            this.internally = null;
        }

        public IPeerParticipantSet ask_participant_maps () throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void forward_peer_message (IPeerMessage peer_message) throws StubError, DeserializeError
        {
            tasklet.ms_wait(2); // simulates network latency
            error("not implemented yet");
        }

        public IPeerParticipantSet get_participant_set (int lvl) throws PeersInvalidRequest, StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public IPeersRequest get_request (int msg_id, IPeerTupleNode respondant) throws PeersUnknownMessageError, PeersInvalidRequest, StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void give_participant_maps (IPeerParticipantSet maps) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_failure (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_missing_optional_maps (int msg_id) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_next_destination (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
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
            error("not implemented yet");
        }
    }
}

//FAKE

namespace Netsukuku.PeerServices
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

    internal ITasklet tasklet;
}
