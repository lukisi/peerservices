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

class Entering : Object
{
    private SimNode node_a;
    private SimNode node_b;
    private SimNode node_c;
    private SimNode node_d;
    private SimNode node_e;
    private SimNode node_f;
    private SimNode node_g;
    private SimNode node_h;
    private Network net1;

    public void prepare_network_1()
    {
        gsizes = new ArrayList<int>.wrap({2,2,2,2});
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
        node_a = new SimNode(this, "a", new ArrayList<int>.wrap({1,0,1,1}), net1);
        node_b = new SimNode(this, "b", new ArrayList<int>.wrap({0,0,1,1}), net1);
        assert(node_a.exists_gnode(h00));
        node_a.add_gateway_to_gnode(node_b, h00);
        assert(node_b.exists_gnode(h01));
        node_b.add_gateway_to_gnode(node_a, h01);
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
        private Entering environ;
        public string name;
        public Gee.List<int> pos;
        public HashMap<HCoord, Gee.List<SimNode>> network_by_hcoord;
        public HashMap<string,TupleStub> stub_by_tuple;
        private MessageRouting.MessageRouting message_routing;
        private Databases.Databases databases;

        public SimNode
        (Entering environ, string name,
         Gee.List<int> pos, Network net, SimNode? prev_id=null)
        {
            assert(pos.size == environ.levels);
            for (int i = 0; i < environ.levels; i++)
            {
                assert(pos[i] >= 0);
                assert(pos[i] < environ.gsizes[i]);
            }

            this.environ = environ;
            this.name = name;
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
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
        }

        private bool is_service_optional(int p_id)
        {
            assert(p_id == 1); // The only service simulated in this testcase
            return false;
        }

        private void wait_participation_maps(int target_levels)
        {
            assert_not_reached(); // The only service simulated in this testcase is not optional.
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
            for (; i < environ.levels; i++)
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
    }

    Gee.List<int> gsizes;
    int levels {
        get {
            return gsizes.size;
        }
    }

    public void test_entering()
    {
        print("\nPrepare network net1...\n");
        prepare_network_1();
        print("Network net1 ready.\n");
        tasklet.ms_wait(10);
    }
}