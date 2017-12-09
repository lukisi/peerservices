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

class Replicas : Object
{
    private SimNode node_a;
    private SimNode node_b;
    private SimNode node_c;
    private SimNode node_d;
    private SimNode node_e;
    private SimNode node_f;
    private SimNode node_g;
    private SimNode node_h;

    public void prepare_network()
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
        node_a = new SimNode(this, "a", new ArrayList<int>.wrap({1,0,1,1}));
        node_b = new SimNode(this, "b", new ArrayList<int>.wrap({0,0,1,1}));
        assert(node_a.exists_gnode(h00));
        node_a.add_gateway_to_gnode(node_b, h00);
        assert(node_b.exists_gnode(h01));
        node_b.add_gateway_to_gnode(node_a, h01);
        // g-node 1:1:1
        node_c = new SimNode(this, "c", new ArrayList<int>.wrap({1,1,1,1}));
        // g-node 1:1
        assert(node_a.exists_gnode(h11));
        node_a.add_gateway_to_gnode(node_b, h11);
        assert(node_b.exists_gnode(h11));
        node_b.add_gateway_to_gnode(node_c, h11);
        assert(node_c.exists_gnode(h10));
        node_c.add_gateway_to_gnode(node_b, h10);
        // g-node 1:0:1
        node_e = new SimNode(this, "e", new ArrayList<int>.wrap({1,1,0,1}));
        node_h = new SimNode(this, "h", new ArrayList<int>.wrap({0,1,0,1}));
        assert(node_e.exists_gnode(h00));
        node_e.add_gateway_to_gnode(node_h, h00);
        assert(node_h.exists_gnode(h01));
        node_h.add_gateway_to_gnode(node_e, h01);
        // g-node 1:0:0
        node_f = new SimNode(this, "f", new ArrayList<int>.wrap({1,0,0,1}));
        node_g = new SimNode(this, "g", new ArrayList<int>.wrap({0,0,0,1}));
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
        assert(node_a.exists_gnode(h20));
        node_a.add_gateway_to_gnode(node_e, h20);
        node_a.add_gateway_to_gnode(node_h, h20);
        node_a.add_gateway_to_gnode(node_b, h20);
        assert(node_b.exists_gnode(h20));
        node_b.add_gateway_to_gnode(node_e, h20);
        node_b.add_gateway_to_gnode(node_a, h20);
        node_b.add_gateway_to_gnode(node_c, h20);
        assert(node_c.exists_gnode(h20));
        node_c.add_gateway_to_gnode(node_e, h20);
        node_c.add_gateway_to_gnode(node_b, h20);
        assert(node_e.exists_gnode(h21));
        node_e.add_gateway_to_gnode(node_a, h21);
        node_e.add_gateway_to_gnode(node_b, h21);
        node_e.add_gateway_to_gnode(node_c, h21);
        node_e.add_gateway_to_gnode(node_h, h21);
        assert(node_h.exists_gnode(h21));
        node_h.add_gateway_to_gnode(node_e, h21);
        node_h.add_gateway_to_gnode(node_a, h21);
        assert(node_f.exists_gnode(h21));
        node_f.add_gateway_to_gnode(node_e, h21);
        assert(node_g.exists_gnode(h21));
        node_g.add_gateway_to_gnode(node_f, h21);
        // g-node 0
        node_d = new SimNode(this, "d", new ArrayList<int>.wrap({0,1,1,0}));
        // whole net
        assert(node_a.exists_gnode(h30));
        node_a.add_gateway_to_gnode(node_b, h30);
        node_a.add_gateway_to_gnode(node_e, h30);
        node_a.add_gateway_to_gnode(node_h, h30);
        assert(node_b.exists_gnode(h30));
        node_b.add_gateway_to_gnode(node_c, h30);
        node_b.add_gateway_to_gnode(node_e, h30);
        node_b.add_gateway_to_gnode(node_a, h30);
        assert(node_c.exists_gnode(h30));
        node_c.add_gateway_to_gnode(node_d, h30);
        node_c.add_gateway_to_gnode(node_e, h30);
        node_c.add_gateway_to_gnode(node_b, h30);
        assert(node_e.exists_gnode(h30));
        node_e.add_gateway_to_gnode(node_c, h30);
        node_e.add_gateway_to_gnode(node_f, h30);
        node_e.add_gateway_to_gnode(node_b, h30);
        node_e.add_gateway_to_gnode(node_a, h30);
        node_e.add_gateway_to_gnode(node_h, h30);
        assert(node_h.exists_gnode(h30));
        node_h.add_gateway_to_gnode(node_e, h30);
        node_h.add_gateway_to_gnode(node_a, h30);
        assert(node_f.exists_gnode(h30));
        node_f.add_gateway_to_gnode(node_g, h30);
        node_f.add_gateway_to_gnode(node_e, h30);
        assert(node_g.exists_gnode(h30));
        node_g.add_gateway_to_gnode(node_d, h30);
        node_g.add_gateway_to_gnode(node_f, h30);
        assert(node_d.exists_gnode(h31));
        node_d.add_gateway_to_gnode(node_c, h31);
        node_d.add_gateway_to_gnode(node_g, h31);
        // print info
        LinkedList<string> lst_k = new LinkedList<string>();
        lst_k.add_all(network.keys);
        lst_k.sort();
        foreach (string k in lst_k)
        {
            SimNode n = network[k];
            print(@"Node '$(n.name)': (address $(address(n.pos)))\n");
            print(@"   It has knowledge of $(n.network_by_hcoord.entries.size) g-nodes:\n");
            foreach (var e in n.network_by_hcoord.entries)
            {
                HCoord h = e.key;
                Gee.List<SimNode> gwlist = e.@value;
                print(@"     ($(h.lvl), $(h.pos)): $(gwlist.size) gateways for it.\n");
                assert(!gwlist.is_empty);
            }
            HashMap<int,ArrayList<string>> reachable_nodes =
                new HashMap<int,ArrayList<string>>();
            for (int lvl = 0; lvl < levels; lvl++)
                reachable_nodes[lvl] = new ArrayList<string>();
            foreach (var e in n.stub_by_tuple.entries)
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
                    reachable_nodes[lvl].add(addr);
                }
            }
            for (int lvl = 0; lvl < levels; lvl++)
            {
                int tot = n.nodes_inside_my_gnode(lvl+1);
                int sub = 0;
                if (lvl > 0) sub = n.nodes_inside_my_gnode(lvl);
                assert(tot-sub == reachable_nodes[lvl].size);
                if (reachable_nodes[lvl].size > 0)
                {
                    print(@"   Inside its g-node of level $(lvl+1) there are $(reachable_nodes[lvl].size) routable nodes:\n");
                    foreach (string addr in reachable_nodes[lvl])
                    {
                        print(@"     $(addr) (wich is node '$(n.stub_by_tuple[addr].node.name)')\n");
                    }
                }
            }
        }
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
        private Replicas environ;
        public string name;
        public Gee.List<int> pos;
        public HashMap<HCoord, Gee.List<SimNode>> network_by_hcoord;
        public HashMap<string,TupleStub> stub_by_tuple;
        private MessageRouting.MessageRouting message_routing;
        private Databases.Databases databases;

        // Service 01: name-telephone directory
        private HashMap<string,string> db_part;
        private class RequestStore : Object, IPeersRequest
        {
            public string name {public set; public get;}
            public string number {public set; public get;}
        }
        private class ResponseStoreOk : Object, IPeersResponse
        {
        }
        private class RequestRetr : Object, IPeersRequest
        {
            public string name {public set; public get;}
        }
        private class ResponseRetrOk : Object, IPeersResponse
        {
            public string name {public set; public get;}
            public string number {public set; public get;}
        }
        private class ResponseRetrNotFound : Object, IPeersResponse
        {
        }
        private class RequestReplica : Object, IPeersRequest
        {
            public string name {public set; public get;}
            public string number {public set; public get;}
        }
        private class ResponseReplicaOk : Object, IPeersResponse
        {
        }

        public SimNode(Replicas environ, string name, Gee.List<int> pos)
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

            foreach (SimNode other in environ.network.values)
            {
                other.add_knowledge_node(this);
                add_knowledge_node(other);
            }

            this.environ.network[name] = this;

            message_routing = new MessageRouting.MessageRouting
                (pos, environ.gsizes,
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
                     if (p_id == 1)
                     {
                         if (req is RequestReplica)
                         {
                             RequestReplica _req = (RequestReplica)req;
                             print(@"SimNode $(name): replica for '$(_req.name)' => '$(_req.number)'.\n");
                             db_part[_req.name] = _req.number;
                             ret = new ResponseReplicaOk();
                         }
                         else assert_not_reached();
                     }
                     else assert_not_reached();
                     return ret;
                 });

            databases = new Databases.Databases
                (pos, environ.gsizes, /*guest_gnode_level*/ 0, /*new_gnode_level*/ 0,
                 /* contact_peer     = */  (/*int*/ p_id,
                                            /*PeerTupleNode*/ x_macron,
                                            /*IPeersRequest*/ request,
                                            /*int*/ timeout_exec,
                                            /*int*/ exclude_my_gnode,
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
                          exclude_my_gnode,
                          out respondant,
                          exclude_tuple_list);
                     // Done.
                 },
                 /* assert_service_registered = */  (/*int*/ p_id) => {
                     assert(p_id == 1); // The only service simulated in this testcase
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

            // Service 01: name-telephone directory
            db_part = new HashMap<string,string>();
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
            print(@"$(other.name) for $(name) is HCoord ($(g.lvl),$(g.pos)).\n");
            if (! (g in network_by_hcoord.keys)) network_by_hcoord[g] = new ArrayList<SimNode>();
            // most internal tuple
            Gee.List<int> internal_tuple = new ArrayList<int>();
            int i = 0;
            for (; i <= g.lvl; i++) internal_tuple.add(other.pos[i]);
            string _address = address(internal_tuple);
            stub_by_tuple[_address] = new TupleStub(other);
            print(@"$(other.name) for $(name) is $(_address) (inside their minimum common g-node).\n");
            // wider
            for (; i < environ.levels; i++)
            {
                internal_tuple.add(other.pos[i]);
                _address = address(internal_tuple);
                stub_by_tuple[_address] = new TupleStub(other, false);
                print(@"$(other.name) for $(name) is also $(_address), but redundant.\n");
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

        public void fake_servant_store_and_replica(string name, string number, PeerTupleNode x_macron, int quantity)
        {
            int p_id = 1;

            print(@"SimNode $(this.name): store for '$(name)' => '$(number)'.\n");
            db_part[name] = number;

            RequestReplica request = new RequestReplica();
            request.name = name;
            request.number = number;

            IPeersResponse? resp;
            IReplicaContinuation cont;
            bool ret = databases.begin_replica(quantity, p_id, x_macron.tuple,
                                               request, 1000, out resp, out cont);
            while (ret)
            {
                ret = databases.next_replica(cont, out resp);
            }
        }

        public void rpc_forward_peer_message(PeerMessageForwarder mf, SimNode caller)
        {
            // check if mf.p_id is optional
            // In this testcase is always false.
            bool optional = is_service_optional(mf.p_id);
            int maps_retrieved_below_level = environ.levels;
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

        public void set_refuse_message (int msg_id, string refuse_message, int e_lvl, IPeerTupleNode respondant) throws StubError, DeserializeError
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

    public void test_replicas()
    {
        prepare_network();
        tasklet.ms_wait(10);

        // Now suppose a service request has been done to save a record
        //  and the servant is node_e = 1:0:1:1 because the hash_node is 1:0:1:1.
        string name = "Mark";
        PeerTupleNode x_macron = new PeerTupleNode(new ArrayList<int>.wrap({1,1,0,1}));
        string number = "555 1234";
        int quantity = 9;
        node_e.fake_servant_store_and_replica(name, number, x_macron, quantity);
        tasklet.ms_wait(10);

        // Now suppose a service request has been done to save a record
        //  and the servant is node_a = 1:1:0:1 because the hash_node is 1:1:0:1.
        name = "John";
        x_macron = new PeerTupleNode(new ArrayList<int>.wrap({1,0,1,1}));
        number = "555 3741";
        node_a.fake_servant_store_and_replica(name, number, x_macron, quantity);
        tasklet.ms_wait(10);

        // Now suppose a service request has been done to save a record
        //  and the servant is node_c = 1:1:1:1 because the hash_node is 1:1:1:0.
        name = "Ralph";
        x_macron = new PeerTupleNode(new ArrayList<int>.wrap({0,1,1,1}));
        number = "555 8800";
        node_c.fake_servant_store_and_replica(name, number, x_macron, quantity);
        tasklet.ms_wait(10);
    }
}
