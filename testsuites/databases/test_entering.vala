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
    private class Service01Names : Object
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

    /* Service 00
      * keys = 0, 1, ... levels
      *
      * hash_nodes =
      *    k=0       {} that is the node itself
      *    k=1       {0} a node in my g-node of level 1
      *    k=2       {0,0}
      *    ....
      *    k=levels  {0,0,0,0}
      *
      * values = state of the g-node
      *
      * Requests: add_segment, replica_value
      *
      * string add_segment(string)
      *   the client pass a segment that is to add to the current string and obtains the whole string.
     */

    class Service00Key : Object
    {
        public int level {public get; public set;}
    }

    class Service00Record : Object
    {
        public int level {public get; public set;}
        public string data {public get; public set;}
    }

    class Service00AddSegmentRequest : Object, IPeersRequest
    {
        public int level {public get; public set;}
        public string segment {public get; public set;}
    }
    private static const int Service00AddSegmentRequestTimeoutExec = 2000;

    class Service00AddSegmentResponse : Object, IPeersResponse
    {
        public string data {public get; public set;}
    }

    class Service00ReplicaRequest : Object, IPeersRequest
    {
        public int level {public get; public set;}
        public string data {public get; public set;}
    }
    class Service00ReplicaOkResponse : Object, IPeersResponse
    {
    }

    class Service00Database : Object
    {
        public Service00Database(int levels)
        {
            descriptor = new Descriptor(this);
            this.levels = levels;
            database = new HashMap<int,string>();
        }
        public int levels {public get; private set;}
        public HashMap<int,string> database;

        public string get_default_for_key(int level)
        {
            return "";
        }

        public string add_segment(int level, string data)
        {
            if (! database.has_key(level)) database[level] = get_default_for_key(level);
            database[level] = database[level] + data;
            return database[level];
        }

        public void replica_value(int level, string data)
        {
            database[level] = data;
        }

        public IFixedKeysDatabaseDescriptor descriptor {public get; private set;}
        private class Descriptor : Object, IDatabaseDescriptor, IFixedKeysDatabaseDescriptor
        {
            public Descriptor(Service00Database t)
            {
                this.t = t;
            }
            private Service00Database t;

            public bool is_valid_key(Object k)
            {
                if (k is Service00Key)
                {
                    Service00Key _k = (Service00Key)k;
                    if (_k.level < 0) return false;
                    if (_k.level > t.levels) return false;
                    return true;
                }
                return false;
            }

            public Gee.List<int> evaluate_hash_node(Object k)
            {
                assert(is_valid_key(k));
                Service00Key _k = (Service00Key)k;
                var ret = new ArrayList<int>();
                for (int i = 0; i < _k.level; i++) ret.add(0);
                return ret;
            }

            public bool key_equal_data(Object k1, Object k2)
            {
                assert(is_valid_key(k1));
                Service00Key _k1 = (Service00Key)k1;
                int k1l = _k1.level;
                assert(is_valid_key(k2));
                Service00Key _k2 = (Service00Key)k2;
                int k2l = _k2.level;
                return k1l == k2l;
            }

            public uint key_hash_data(Object k)
            {
                assert(is_valid_key(k));
                Service00Key _k = (Service00Key)k;
                int kl = _k.level;
                return (uint)kl;
            }

            public bool is_valid_record(Object k, Object rec)
            {
                if (k is Service00Key)
                {
                    if (rec is Service00Record)
                    {
                        Service00Key _k = (Service00Key)k;
                        Service00Record _rec = (Service00Record)rec;
                        return _k.level == _rec.level;
                    }
                    return false;
                }
                return false;
            }

            public bool my_records_contains(Object k)
            {
                assert(is_valid_key(k));
                Service00Key _k = (Service00Key)k;
                int kl = _k.level;
                return t.database.has_key(kl);
            }

            public Object get_record_for_key(Object k)
            {
                assert(is_valid_key(k));
                Service00Key _k = (Service00Key)k;
                int kl = _k.level;
                var ret = new Service00Record();
                ret.level = kl;
                ret.data = t.database[kl];
                return ret;
            }

            public void set_record_for_key(Object k, Object rec)
            {
                assert(is_valid_key(k));
                Service00Key _k = (Service00Key)k;
                int kl = _k.level;
                assert(is_valid_record(k, rec));
                Service00Record _rec = (Service00Record)rec;
                t.database[kl] = _rec.data;
            }


            public Object get_key_from_request(IPeersRequest r)
            {
                if (r is Service00AddSegmentRequest)
                {
                    Service00AddSegmentRequest _r = (Service00AddSegmentRequest)r;
                    Service00Key ret = new Service00Key();
                    ret.level = _r.level;
                    return ret;
                }
                else if (r is Service00ReplicaRequest)
                {
                    Service00ReplicaRequest _r = (Service00ReplicaRequest)r;
                    Service00Key ret = new Service00Key();
                    ret.level = _r.level;
                    return ret;
                }
                else assert_not_reached();
            }

            public int get_timeout_exec(IPeersRequest r)
            {
                if (r is Service00AddSegmentRequest)
                {
                    return Service00AddSegmentRequestTimeoutExec;
                }
                else if (r is Service00ReplicaRequest)
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
                if (r is Service00AddSegmentRequest)
                {
                    Service00AddSegmentRequest _r = (Service00AddSegmentRequest)r;
                    if (_r.level < 0) return false;
                    if (_r.level > t.levels) return false;
                    return true;
                }
                return false;
            }

            public bool is_replica_value_request(IPeersRequest r)
            {
                if (r is Service00ReplicaRequest)
                {
                    Service00ReplicaRequest _r = (Service00ReplicaRequest)r;
                    if (_r.level < 0) return false;
                    if (_r.level > t.levels) return false;
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
                if (r is Service00AddSegmentRequest)
                {
                    Service00AddSegmentRequest _r = (Service00AddSegmentRequest)r;
                    Service00AddSegmentResponse ret = new Service00AddSegmentResponse();
                    ret.data = t.add_segment(_r.level, _r.segment);
                    return ret;
                }
                else if (r is Service00ReplicaRequest)
                {
                    Service00ReplicaRequest _r = (Service00ReplicaRequest)r;
                    t.replica_value(_r.level, _r.data);
                    return new Service00ReplicaOkResponse();
                }
                else assert_not_reached();
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
                for (int i = 0; i <= t.levels; i++)
                {
                    var k = new Service00Key();
                    k.level = i;
                    ret.add(k);
                }
                return ret;
            }

            public Object get_default_record_for_key(Object k)
            {
                assert(is_valid_key(k));
                Service00Key _k = (Service00Key)k;
                int kl = _k.level;
                var ret = new Service00Record();
                ret.level = kl;
                ret.data = t.get_default_for_key(kl);
                return ret;
            }
        }
    }

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

    Gee.List<int> gsizes;
    int levels {
        get {
            return gsizes.size;
        }
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
        private Service00Database s00_database;

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
                     if (p_id == 0)
                     {
                         error("not implemented yet");
                     }
                     else if (p_id == 1)
                     {
                         error("not implemented yet");
                     }
                     else assert_not_reached();
                     return ret;
                 });

            databases = new Databases.Databases
                (pos, environ.gsizes,
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

            s00_database = new Service00Database(environ.levels);
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

    public void test_entering()
    {
        print("\nPrepare network net1...\n");
        prepare_network_1();
        print("Network net1 ready.\n");
        tasklet.ms_wait(10);
    }
}