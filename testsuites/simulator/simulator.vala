/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2015 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
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

using Netsukuku;
using Netsukuku.ModRpc;
using Gee;

string json_string_object(Object obj)
{
    Json.Node n = Json.gobject_serialize(obj);
    Json.Generator g = new Json.Generator();
    g.root = n;
    g.pretty = true;
    string ret = g.to_data(null);
    return ret;
}

void print_object(Object obj)
{
    print(@"$(obj.get_type().name())\n");
    string t = json_string_object(obj);
    print(@"$(t)\n");
}

Object dup_object(Object obj)
{
    //print(@"dup_object...\n");
    Type type = obj.get_type();
    string t = json_string_object(obj);
    Json.Parser p = new Json.Parser();
    try {
        assert(p.load_from_data(t));
    } catch (Error e) {assert_not_reached();}
    Object ret = Json.gobject_deserialize(type, p.get_root());
    //print(@"dup_object done.\n");
    return ret;
}

public class SimulatorNode : Object
{
    public string name;
    public Gee.List<int> my_pos;
    public Gee.List<string> neighbors;
    public MyPeersMapPath map_paths;
    public MyPeersBackStubFactory back_factory;
    public MyPeersNeighborsFactory neighbor_factory;
    public PeersManager peers_manager;
}

INtkdTasklet tasklet;
void main(string[] args)
{
    // init tasklet
    MyTaskletSystem.init();
    tasklet = MyTaskletSystem.get_ntkd();

    ttl_100.serialization_tests();

    // TODO delete
    return;

    // pass tasklet system to modules
    PeersManager.init(tasklet);

    var t = new FileTester();
    t.test_file(args[1]);

    // end
    MyTaskletSystem.kill();
}

class Directive : Object
{
    // Activate a node
    public bool activate_node = false;
    public string name;
    public string neighbor_name;
    public int lvl;
    public Gee.List<int> pos = null;
    // Wait
    public bool wait = false;
    public int wait_msec;
    // Info
    public bool info = false;
    public string info_name;
}

string[] read_file(string path)
{
    string[] ret = new string[0];
    if (FileUtils.test(path, FileTest.EXISTS))
    {
        try {
            string contents;
            assert(FileUtils.get_contents(path, out contents));
            ret = contents.split("\n");
        } catch (FileError e) {
            error(@"$(e.domain.to_string()): $(e.code): $(e.message)");
        }
    }
    else error(@"Script $(path) not found");
    return ret;
}

internal class FileTester : Object
{
    int levels;
    ArrayList<int> gsizes;
    HashMap<string, SimulatorNode> nodes;

    public void test_file(string fname)
    {
        // read data
        gsizes = new ArrayList<int>();
        nodes = new HashMap<string, SimulatorNode>();
        ArrayList<Directive> directives = new ArrayList<Directive>();
        string[] data = read_file(fname);
        int data_cur = 0;

        while (data[data_cur] != "topology") data_cur++;
        data_cur++;
        string s_topology = data[data_cur++];
        string[] s_topology_pieces = s_topology.split(" ");
        levels = s_topology_pieces.length;
        foreach (string s_piece in s_topology_pieces) gsizes.insert(0, int.parse(s_piece));

        while (data[data_cur] != "first_node") data_cur++;
        data_cur++;
        string s_first_node = data[data_cur++];
        string[] s_first_node_pieces = s_first_node.split(" ");
        assert(levels == s_first_node_pieces.length);
        string first_node_name = "first_node";
        {
            nodes[first_node_name] = new SimulatorNode();
            SimulatorNode n = nodes[first_node_name];
            n.name = first_node_name;
            n.my_pos = new ArrayList<int>();
            for (int i = 0; i < levels; i++)
            {
                n.my_pos.insert(0, int.parse(s_first_node_pieces[i]));
            }
            n.neighbors = new ArrayList<string>();
            n.map_paths = new MyPeersMapPath(gsizes.to_array(), n.my_pos.to_array());
            n.back_factory = new MyPeersBackStubFactory();
            n.neighbor_factory = new MyPeersNeighborsFactory();
            n.peers_manager = new PeersManager(n.map_paths,
                                     levels,
                                     n.back_factory,
                                     n.neighbor_factory);
        }

        while (true)
        {
            if (data[data_cur] != null && data[data_cur].has_prefix("add_neighbor"))
            {
                Directive dd = new Directive();
                dd.activate_node = true;
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                dd.name = line_pieces[1];
                assert(line_pieces[2] == "to");
                dd.neighbor_name = line_pieces[3];
                assert(line_pieces[4] == "lower_pos");
                assert(line_pieces.length > 5);
                dd.lvl = line_pieces.length - 5;
                dd.pos = new ArrayList<int>();
                for (int i = 5; i < line_pieces.length; i++)
                {
                    dd.pos.insert(0, int.parse(line_pieces[i]));
                }
                directives.add(dd);
                data_cur++;
            }
            else if (data[data_cur] != null && data[data_cur].has_prefix("wait_msec"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                int wait_msec = int.parse(line_pieces[1]);
                // data input done
                Directive dd = new Directive();
                dd.wait = true;
                dd.wait_msec = wait_msec;
                directives.add(dd);
                data_cur++;
                assert(data[data_cur] == "");
            }
            else if (data[data_cur] != null && data[data_cur].has_prefix("print_info"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                string info_name = line_pieces[1];
                // data input done
                Directive dd = new Directive();
                dd.info = true;
                dd.info_name = info_name;
                directives.add(dd);
                data_cur++;
                assert(data[data_cur] == "");
            }
            else if (data_cur >= data.length)
            {
                break;
            }
            else
            {
                data_cur++;
            }
        }

        // execute directives
        foreach (Directive dd in directives)
        {
            if (dd.activate_node)
            {
                // dd.lvl level of the existing g-node where we want to enter with a new lvl-1 gnode.
                assert(dd.lvl <= levels);
                assert(dd.lvl > 0);
                var neighbor_n = nodes[dd.neighbor_name];
                var stub_p = new MyPeersManagerStub(neighbor_n.peers_manager);
                nodes[dd.name] = new SimulatorNode();
                SimulatorNode n = nodes[dd.name];
                n.name = dd.name;
                n.my_pos = new ArrayList<int>();
                for (int i = 0; i < dd.lvl; i++)
                {
                    n.my_pos.add(dd.pos[i]);
                }
                for (int i = dd.lvl; i < levels; i++)
                {
                    n.my_pos.add(neighbor_n.my_pos[i]);
                }
                n.neighbors = new ArrayList<string>();
                n.map_paths = new MyPeersMapPath(gsizes.to_array(), n.my_pos.to_array());
                n.back_factory = new MyPeersBackStubFactory();
                n.neighbor_factory = new MyPeersNeighborsFactory();
                n.map_paths.set_fellow(dd.lvl, stub_p);
                n.peers_manager = new PeersManager(n.map_paths,
                                         dd.lvl-1,
                                         n.back_factory,
                                         n.neighbor_factory);
                n.neighbors.add(dd.neighbor_name);
                neighbor_n.neighbors.add(dd.name);
                // continue in a tasklet
                CompleteHookTasklet ts = new CompleteHookTasklet();
                ts.t = this;
                ts.dd = dd;
                tasklet.spawn(ts);
            }
            else if (dd.wait)
            {
                print(@"waiting $(dd.wait_msec) msec...");
                tasklet.ms_wait(dd.wait_msec);
                print("\n");
            }
            else if (dd.info)
            {
                print(@"examining node $(dd.info_name).\n"); //TODO
                assert(nodes.has_key(dd.info_name));
                SimulatorNode n = nodes[dd.info_name];
                string mypos = "";
                string mypos_next = "";
                foreach (int p in n.my_pos)
                {
                    mypos += @"$(mypos_next)$(p)";
                    mypos_next = ", ";
                }
                print(@"  my_pos: $(mypos)\n");
                print("\n");
            }
        }
    }

    internal class CompleteHookTasklet : Object, INtkdTaskletSpawnable
    {
        public FileTester t;
        public Directive dd;
        public void * func()
        {
            tasklet.ms_wait(20); // simulate little wait before bootstrap
            t.update_my_map(dd.neighbor_name, dd.name);
            t.update_back_factories(dd.name);
            tasklet.ms_wait(100); // simulate little wait before ETPs reach fellows
            t.start_update_their_maps(dd.neighbor_name, dd.name);
            return null;
        }
    }

    void update_back_factories(string name)
    {
        SimulatorNode neo = nodes[name];
        foreach (string name_other in nodes.keys) if (name_other != name)
        {
            SimulatorNode other = nodes[name_other];
            int max_distinct_level = levels-1;
            while (neo.my_pos[max_distinct_level] == other.my_pos[max_distinct_level]) max_distinct_level--;
            int min_common_level = max_distinct_level + 1;
            var positions_neo = new ArrayList<int>();
            var positions_other = new ArrayList<int>();
            for (int j = 0; j < min_common_level; j++)
            {
                positions_neo.add(neo.my_pos[j]);
                positions_other.add(other.my_pos[j]);
            }
            neo.back_factory.add_node(positions_other, other);
            other.back_factory.add_node(positions_neo, neo);
        }
    }

    void update_my_map(string neighbor_name, string name)
    {
        SimulatorNode gw = nodes[neighbor_name];
        SimulatorNode neo = nodes[name];
        int gw_lvl = levels-1;
        while (gw.my_pos[gw_lvl] == neo.my_pos[gw_lvl])
        {
            gw_lvl--;
            assert(gw_lvl >= 0);
        }
        for (int i = gw_lvl; i < levels; i++)
        {
            for (int j = 0; j < gsizes[i]; j++)
            {
                if (! gw.map_paths.i_peers_exists(i, j))
                {
                    neo.map_paths.add_existent_gnode(i, j, new MyPeersManagerStub(gw.peers_manager));
                }
            }
        }
    }

    void start_update_their_maps(string neighbor_name, string name)
    {
        update_their_maps(neighbor_name, name, name);
    }

    void update_their_maps(string name_old, string name_neo, string name_gw_to_neo)
    {
        SimulatorNode old = nodes[name_old];
        SimulatorNode neo = nodes[name_neo];
        SimulatorNode gw_to_neo = nodes[name_gw_to_neo];
        int neo_lvl = levels-1;
        while (neo.my_pos[neo_lvl] == old.my_pos[neo_lvl])
        {
            neo_lvl--;
            assert(neo_lvl >= 0);
        }
        int neo_pos = neo.my_pos[neo_lvl];
        if (! old.map_paths.i_peers_exists(neo_lvl, neo_pos))
        {
            old.map_paths.add_existent_gnode(neo_lvl, neo_pos, new MyPeersManagerStub(gw_to_neo.peers_manager));
        }
        foreach (string neighbor_name in old.neighbors) if (neighbor_name != name_gw_to_neo)
        {
            update_their_maps(neighbor_name, name_neo, name_old);
        }
    }
}

public class MyPeersMapPath : Object, IPeersMapPaths
{
    public MyPeersMapPath(int[] gsizes, int[] mypos)
    {
        this.gsizes = new ArrayList<int>();
        this.gsizes.add_all_array(gsizes);
        this.mypos = new ArrayList<int>();
        this.mypos.add_all_array(mypos);
        map_gnodes = new HashMap<string, IPeersManagerStub>();
    }
    public ArrayList<int> gsizes;
    public ArrayList<int> mypos;
    public void add_existent_gnode(int level, int pos, IPeersManagerStub gateway)
    {
        string k = @"$(level),$(pos)";
        map_gnodes[k] = gateway;
    }
    public HashMap<string, IPeersManagerStub> map_gnodes;
    public void set_fellow(int lvl, IPeersManagerStub fellow)
    {
        this.fellow = fellow;
    }
    private IPeersManagerStub fellow;

    public bool i_peers_exists
    (int level, int pos)
    {
        string k = @"$(level),$(pos)";
        return map_gnodes.has_key(k);
    }

    public IPeersManagerStub i_peers_fellow
    (int level)
    throws PeersNonexistentFellowError
    {
        return fellow;
    }

    public IPeersManagerStub i_peers_gateway
    (int level, int pos, zcd.ModRpc.CallerInfo? received_from = null, IPeersManagerStub? failed = null)
    throws PeersNonexistentDestinationError
    {
        string k = @"$(level),$(pos)";
        if (! (map_gnodes.has_key(k)))
        {
            warning(@"Forwarding a peer-message. gateway not set for $(k).");
            throw new PeersNonexistentDestinationError.GENERIC(@"gateway not set for $(k)");
        }
        // This simulator has a lazy implementation of i_peers_gateway. It simulates well only networks with no loops.
        if (failed != null) error("not implemented yet");
        return map_gnodes[k];
    }

    public int i_peers_get_gsize
    (int level)
    {
        return gsizes[level];
    }

    public int i_peers_get_levels()
    {
        return gsizes.size;
    }

    public int i_peers_get_my_pos
    (int level)
    {
        return mypos[level];
    }

    public int i_peers_get_nodes_in_my_group
    (int level)
    {
        if (level == 0) return 1;
        error("not implemented yet");
    }
}

public class MyPeersBackStubFactory : Object, IPeersBackStubFactory
{
    public MyPeersBackStubFactory()
    {
        nodes = new HashMap<string, SimulatorNode>();
    }
    public void add_node(Gee.List<int> positions, SimulatorNode node)
    {
        string s = "";
        foreach (int pos in positions)
        {
            s += @"$(pos),";
        }
        s += "*";
        nodes[s] = node;
    }
    public HashMap<string, SimulatorNode> nodes;

    public IPeersManagerStub i_peers_get_tcp_inside
    (Gee.List<int> positions)
    {
        string s = "";
        foreach (int pos in positions)
        {
            s += @"$(pos),";
        }
        s += "*";
        if (nodes.has_key(s))
        {
            return new MyPeersManagerStub(nodes[s].peers_manager);
        }
        else
        {
            return new MyPeersManagerStub(null);
        }
    }
}

public class MyPeersNeighborsFactory : Object, IPeersNeighborsFactory
{
    public MyPeersNeighborsFactory()
    {
        neighbors = new ArrayList<SimulatorNode>();
    }
    public ArrayList<SimulatorNode> neighbors;

    public IPeersManagerStub i_peers_get_broadcast
    (IPeersMissingArcHandler missing_handler)
    {
        var lst = new ArrayList<IPeersManagerSkeleton>();
        foreach (SimulatorNode neighbor in neighbors) lst.add(neighbor.peers_manager);
        MyPeersManagerBroadcastStub ret = new MyPeersManagerBroadcastStub(lst);
        return ret;
    }

    public IPeersManagerStub i_peers_get_tcp
    (IPeersArc arc)
    {
        // this is called only on missed arcs for a previous broadcast message
        error("not implemented yet");
    }
}

public class MyPeersManagerStub : Object, IPeersManagerStub
{
    public bool working;
    public Netsukuku.ModRpc.IPeersManagerSkeleton skeleton;
    public MyPeersManagerStub(IPeersManagerSkeleton? skeleton)
    {
        if (skeleton == null) working = false;
        else
        {
            this.skeleton = skeleton;
            working = true;
        }
    }

    public void forward_peer_message
    (IPeerMessage peer_message)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.forward_peer_message(((IPeerMessage)dup_object(peer_message)), caller);
    }

    public IPeerParticipantSet get_participant_set
    (int lvl)
    throws PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        return skeleton.get_participant_set(lvl, caller);
    }

    public IPeersRequest get_request
    (int msg_id, IPeerTupleNode respondant)
    throws Netsukuku.PeersUnknownMessageError, Netsukuku.PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        return skeleton.get_request(msg_id, ((IPeerTupleNode)dup_object(respondant)), caller);
    }

    public void set_failure
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_failure(msg_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_next_destination
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_next_destination(msg_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_non_participant
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_non_participant(msg_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_participant
    (int p_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_participant(p_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_redo_from_start
    (int msg_id, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_redo_from_start(msg_id, ((IPeerTupleNode)dup_object(respondant)), caller);
    }

    public void set_refuse_message
    (int msg_id, string refuse_message, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_refuse_message(msg_id, refuse_message, ((IPeerTupleNode)dup_object(respondant)), caller);
    }

    public void set_response
    (int msg_id, IPeersResponse response, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_response(msg_id, ((IPeersResponse)dup_object(response)), ((IPeerTupleNode)dup_object(respondant)), caller);
    }
}

public class MyPeersManagerBroadcastStub : Object, IPeersManagerStub
{
    public bool working;
    public ArrayList<IPeersManagerSkeleton> skeletons;
    public MyPeersManagerBroadcastStub(Gee.List<IPeersManagerSkeleton> skeletons)
    {
        this.skeletons = new ArrayList<IPeersManagerSkeleton>();
        this.skeletons.add_all(skeletons);
        working = true;
    }

    public void set_participant
    (int p_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        tasklet.ms_wait(2); // simulates network latency
        foreach (IPeersManagerSkeleton skeleton in skeletons)
        {
            var caller = new MyCallerInfo();
            skeleton.set_participant(p_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
        }
    }

    public void forward_peer_message
    (IPeerMessage peer_message)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("forward_peer_message should not be sent in broadcast");
    }

    public IPeerParticipantSet get_participant_set
    (int lvl)
    throws PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("get_participant_set should not be sent in broadcast");
    }

    public IPeersRequest get_request
    (int msg_id, IPeerTupleNode respondant)
    throws PeersUnknownMessageError, PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("forward_peer_message should not be sent in broadcast");
    }

    public void set_failure
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_failure should not be sent in broadcast");
    }

    public void set_next_destination
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_next_destination should not be sent in broadcast");
    }

    public void set_non_participant
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_non_participant should not be sent in broadcast");
    }

    public void set_redo_from_start
    (int msg_id, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_redo_from_start should not be sent in broadcast");
    }

    public void set_refuse_message
    (int msg_id, string refuse_message, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_refuse_message should not be sent in broadcast");
    }

    public void set_response
    (int msg_id, IPeersResponse response, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_response should not be sent in broadcast");
    }
}

public class MyCallerInfo : zcd.ModRpc.CallerInfo
{
}

