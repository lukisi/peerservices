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
    Type type = obj.get_type();
    string t = json_string_object(obj);
    Json.Parser p = new Json.Parser();
    try {
        assert(p.load_from_data(t));
    } catch (Error e) {assert_not_reached();}
    Object ret = Json.gobject_deserialize(type, p.get_root());
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
    public ttl_100.Ttl100Service srv100;
    public ttl_100.Ttl100Client cli100;
}

INtkdTasklet tasklet;
void main(string[] args)
{
    // init tasklet
    MyTaskletSystem.init();
    tasklet = MyTaskletSystem.get_ntkd();

    ttl_100.serialization_tests();

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
    // A node requests a 'insert'
    public bool req_insert = false;
    public string req_insert_q_name;
    public int req_insert_k;
    public string req_insert_v;
    public bool req_insert_retry;
    public bool req_insert_assertok;
    public bool req_insert_assertnotfree;
    // A node requests a 'read'
    public bool req_read = false;
    public string req_read_q_name;
    public int req_read_k;
    public bool req_read_assertok;
    public bool req_read_assertnotfound;
    public bool req_read_expect;
    public string req_read_expect_v;
    // A node requests a 'modify'
    public bool req_modify = false;
    public string req_modify_q_name;
    public int req_modify_k;
    public string req_modify_v;
    public bool req_modify_assertok;
    public bool req_modify_assertnotfound;
    // A node requests a 'touch'
    public bool req_touch = false;
    public string req_touch_q_name;
    public int req_touch_k;
    public bool req_touch_assertok;
    public bool req_touch_assertnotfound;
    // A node requests a 'delete'
    public bool req_delete = false;
    public string req_delete_q_name;
    public int req_delete_k;
    public bool req_delete_assertok;
    public bool req_delete_assertnotfound;
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
            n.srv100 = new ttl_100.Ttl100Service(gsizes, n.peers_manager, true);
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
            else if (data[data_cur] != null && data[data_cur].has_prefix("request_insert"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                Directive dd = new Directive();
                dd.req_insert = true;
                dd.req_insert_k = int.parse(line_pieces[1]);
                dd.req_insert_v = line_pieces[2];
                assert(line_pieces[3] == "query_node");
                dd.req_insert_q_name = line_pieces[4];
                dd.req_insert_retry = false;
                if (line_pieces.length > 5 && line_pieces[5] == "with_retry") dd.req_insert_retry = true;
                dd.req_insert_assertok = false;
                dd.req_insert_assertnotfree = false;
                directives.add(dd);
                data_cur++;
                while (data[data_cur] != "")
                {
                    if (data[data_cur].has_prefix("assert"))
                    {
                        line = data[data_cur];
                        line_pieces = line.split(" ");
                        if (line_pieces[1] == "ok") dd.req_insert_assertok = true;
                        if (line_pieces[1] == "not_free") dd.req_insert_assertnotfree = true;
                    }
                    else error(@"malformed file at line $(data_cur)");
                    data_cur++;
                }
                assert(data[data_cur] == "");
            }
            else if (data[data_cur] != null && data[data_cur].has_prefix("request_read"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                Directive dd = new Directive();
                dd.req_read = true;
                dd.req_read_k = int.parse(line_pieces[1]);
                assert(line_pieces[2] == "query_node");
                dd.req_read_q_name = line_pieces[3];
                dd.req_read_assertnotfound = false;
                dd.req_read_assertok = false;
                dd.req_read_expect = false;
                directives.add(dd);
                data_cur++;
                while (data[data_cur] != "")
                {
                    if (data[data_cur].has_prefix("assert"))
                    {
                        line = data[data_cur];
                        line_pieces = line.split(" ");
                        if (line_pieces[1] == "ok") dd.req_read_assertok = true;
                        if (line_pieces[1] == "not_found") dd.req_read_assertnotfound = true;
                    }
                    else if (data[data_cur].has_prefix("expect"))
                    {
                        line = data[data_cur];
                        line_pieces = line.split(" ");
                        dd.req_read_expect = true;
                        dd.req_read_expect_v = line_pieces[1];
                    }
                    else error(@"malformed file at line $(data_cur)");
                    data_cur++;
                }
                assert(data[data_cur] == "");
            }
            else if (data[data_cur] != null && data[data_cur].has_prefix("request_modify"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                Directive dd = new Directive();
                dd.req_modify = true;
                dd.req_modify_k = int.parse(line_pieces[1]);
                dd.req_modify_v = line_pieces[2];
                assert(line_pieces[3] == "query_node");
                dd.req_modify_q_name = line_pieces[4];
                dd.req_modify_assertok = false;
                dd.req_modify_assertnotfound = false;
                directives.add(dd);
                data_cur++;
                while (data[data_cur] != "")
                {
                    if (data[data_cur].has_prefix("assert"))
                    {
                        line = data[data_cur];
                        line_pieces = line.split(" ");
                        if (line_pieces[1] == "ok") dd.req_modify_assertok = true;
                        if (line_pieces[1] == "not_found") dd.req_modify_assertnotfound = true;
                    }
                    else error(@"malformed file at line $(data_cur)");
                    data_cur++;
                }
                assert(data[data_cur] == "");
            }
            else if (data[data_cur] != null && data[data_cur].has_prefix("request_touch"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                Directive dd = new Directive();
                dd.req_touch = true;
                dd.req_touch_k = int.parse(line_pieces[1]);
                assert(line_pieces[2] == "query_node");
                dd.req_touch_q_name = line_pieces[3];
                dd.req_touch_assertok = false;
                dd.req_touch_assertnotfound = false;
                directives.add(dd);
                data_cur++;
                while (data[data_cur] != "")
                {
                    if (data[data_cur].has_prefix("assert"))
                    {
                        line = data[data_cur];
                        line_pieces = line.split(" ");
                        if (line_pieces[1] == "ok") dd.req_touch_assertok = true;
                        if (line_pieces[1] == "not_found") dd.req_touch_assertnotfound = true;
                    }
                    else error(@"malformed file at line $(data_cur)");
                    data_cur++;
                }
                assert(data[data_cur] == "");
            }
            else if (data[data_cur] != null && data[data_cur].has_prefix("request_delete"))
            {
                string line = data[data_cur];
                string[] line_pieces = line.split(" ");
                Directive dd = new Directive();
                dd.req_delete = true;
                dd.req_delete_k = int.parse(line_pieces[1]);
                assert(line_pieces[2] == "query_node");
                dd.req_delete_q_name = line_pieces[3];
                dd.req_delete_assertok = false;
                dd.req_delete_assertnotfound = false;
                directives.add(dd);
                data_cur++;
                while (data[data_cur] != "")
                {
                    if (data[data_cur].has_prefix("assert"))
                    {
                        line = data[data_cur];
                        line_pieces = line.split(" ");
                        if (line_pieces[1] == "ok") dd.req_delete_assertok = true;
                        if (line_pieces[1] == "not_found") dd.req_delete_assertnotfound = true;
                    }
                    else error(@"malformed file at line $(data_cur)");
                    data_cur++;
                }
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
                print(@"activating node $(dd.name).\n");
                // dd.lvl level of the existing g-node where we want to enter with a new lvl-1 gnode.
                assert(dd.lvl <= levels);
                assert(dd.lvl > 0);
                var neighbor_n = nodes[dd.neighbor_name];
                while (neighbor_n.peers_manager == null) tasklet.ms_wait(10);
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
                n.map_paths.set_fellow(dd.lvl, new MyPeersManagerTcpFellowStub(neighbor_n.peers_manager));
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
                print(@"waiting $(dd.wait_msec) msec...\n");
                tasklet.ms_wait(dd.wait_msec);
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
                HashMap<int,string> recs = n.srv100.get_records();
                if (! recs.is_empty)
                {
                    foreach (int k in recs.keys)
                        print(@"srv100: record $(k), $(recs[k])\n");
                }
                print("\n");
            }
            else if (dd.req_insert)
            {
                print(@"request from node $(dd.req_insert_q_name).\n");
                print(@"insert $(dd.req_insert_k), $(dd.req_insert_v).\n");
                SimulatorNode n = nodes[dd.req_insert_q_name];
                n.cli100 = new ttl_100.Ttl100Client(gsizes, n.peers_manager);
                try {
                    n.cli100.db_insert(dd.req_insert_k, dd.req_insert_v);
                    print("done.\n");
                    assert(! dd.req_insert_assertnotfree);
                } catch (ttl_100.Ttl100OutOfMemoryError e) {
                    print(@"out of memory: $(e.message).\n");
                    if (dd.req_insert_retry)
                    {
                        print("trying again in 100 ms.\n");
                        tasklet.ms_wait(100);
                        print("trying again.\n");
                        try {
                            n.cli100.db_insert(dd.req_insert_k, dd.req_insert_v);
                            print("done.\n");
                            assert(! dd.req_insert_assertnotfree);
                        } catch (ttl_100.Ttl100OutOfMemoryError e) {
                            print(@"out of memory: $(e.message).\n");
                            assert(! dd.req_insert_assertok);
                            assert(! dd.req_insert_assertnotfree);
                        } catch (ttl_100.Ttl100NotFreeError e) {
                            print(@"not free: $(e.message).\n");
                            assert(! dd.req_insert_assertok);
                        }
                    }
                } catch (ttl_100.Ttl100NotFreeError e) {
                    print(@"not free: $(e.message).\n");
                    assert(! dd.req_insert_assertok);
                }
            }
            else if (dd.req_read)
            {
                print(@"request from node $(dd.req_read_q_name).\n");
                print(@"read $(dd.req_read_k).\n");
                SimulatorNode n = nodes[dd.req_read_q_name];
                n.cli100 = new ttl_100.Ttl100Client(gsizes, n.peers_manager);
                try {
                    string v = n.cli100.db_read(dd.req_read_k);
                    print(@"done. content: $(v)\n");
                    assert(! dd.req_read_assertnotfound);
                    if (dd.req_read_expect) assert(dd.req_read_expect_v == v);
                } catch (ttl_100.Ttl100NotFoundError e) {
                    print(@"not found: $(e.message).\n");
                    assert(! dd.req_read_assertok);
                    assert(! dd.req_read_expect);
                }
            }
            else if (dd.req_modify)
            {
                print(@"request from node $(dd.req_modify_q_name).\n");
                print(@"modify $(dd.req_modify_k), $(dd.req_modify_v).\n");
                SimulatorNode n = nodes[dd.req_modify_q_name];
                n.cli100 = new ttl_100.Ttl100Client(gsizes, n.peers_manager);
                try {
                    n.cli100.db_modify(dd.req_modify_k, dd.req_modify_v);
                    print(@"done.\n");
                    assert(! dd.req_modify_assertnotfound);
                } catch (ttl_100.Ttl100NotFoundError e) {
                    print(@"not found: $(e.message).\n");
                    assert(! dd.req_modify_assertok);
                }
            }
            else if (dd.req_delete)
            {
                print(@"request from node $(dd.req_delete_q_name).\n");
                print(@"delete $(dd.req_delete_k).\n");
                SimulatorNode n = nodes[dd.req_delete_q_name];
                n.cli100 = new ttl_100.Ttl100Client(gsizes, n.peers_manager);
                try {
                    n.cli100.db_delete(dd.req_delete_k);
                    print(@"done.\n");
                    assert(! dd.req_delete_assertnotfound);
                } catch (ttl_100.Ttl100NotFoundError e) {
                    print(@"not found: $(e.message).\n");
                    assert(! dd.req_delete_assertok);
                }
            }
            else if (dd.req_touch)
            {
                print(@"request from node $(dd.req_touch_q_name).\n");
                print(@"touch $(dd.req_touch_k).\n");
                SimulatorNode n = nodes[dd.req_touch_q_name];
                n.cli100 = new ttl_100.Ttl100Client(gsizes, n.peers_manager);
                try {
                    n.cli100.db_touch(dd.req_touch_k);
                    print(@"done.\n");
                    assert(! dd.req_touch_assertnotfound);
                } catch (ttl_100.Ttl100NotFoundError e) {
                    print(@"not found: $(e.message).\n");
                    assert(! dd.req_touch_assertok);
                }
            }
            else error("not implemented yet");
        }
    }
    private class CompleteHookTasklet : Object, INtkdTaskletSpawnable
    {
        public FileTester t;
        public Directive dd;
        public void * func()
        {
            t.tasklet_complete_hook(dd);
            return null;
        }
    }
    private void tasklet_complete_hook(Directive dd)
    {
        SimulatorNode n = nodes[dd.name];
        tasklet.ms_wait(20); // simulate little wait before bootstrap
        update_my_map(dd.neighbor_name, dd.name);
        update_back_factories(dd.name);

        // bootstrap complete, we can create our peers_manager
        n.peers_manager = new PeersManager(n.map_paths,
                                 dd.lvl-1,
                                 n.back_factory,
                                 n.neighbor_factory);
        n.srv100 = new ttl_100.Ttl100Service(gsizes, n.peers_manager, false);

        tasklet.ms_wait(100); // simulate little wait before ETPs reach fellows
        start_update_their_maps(dd.neighbor_name, dd.name);
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
        neo.map_paths.add_existent_gnode(gw_lvl, gw.my_pos[gw_lvl], new MyPeersManagerTcpNoWaitStub(gw.peers_manager));
        for (int i = gw_lvl; i < levels; i++)
        {
            for (int j = 0; j < gsizes[i]; j++)
            {
                if (j != gw.my_pos[i])
                {
                    if (gw.map_paths.i_peers_exists(i, j))
                    {
                        neo.map_paths.add_existent_gnode(i, j, new MyPeersManagerTcpNoWaitStub(gw.peers_manager));
                    }
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
            old.map_paths.add_existent_gnode(neo_lvl, neo_pos, new MyPeersManagerTcpNoWaitStub(gw_to_neo.peers_manager));
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
            warning(@"Transmitting a peer-message. gateway not set for $(k).");
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
        // approssimative implementation, it should be ok
        return 20;
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
            return new MyPeersManagerTcpInsideStub(nodes[s].peers_manager);
        }
        else
        {
            return new MyPeersManagerTcpInsideStub(null);
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

public class MyPeersManagerTcpFellowStub : Object, IPeersManagerStub
{
    public bool working;
    public Netsukuku.ModRpc.IPeersManagerSkeleton skeleton;
    public MyPeersManagerTcpFellowStub(IPeersManagerSkeleton skeleton)
    {
        this.skeleton = skeleton;
        working = true;
    }

    public void forward_peer_message
    (IPeerMessage peer_message)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("forward_peer_message should not be sent in tcp-fellow");
    }

    public IPeerParticipantSet get_participant_set
    (int lvl)
    throws PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug("calling get_participant_set...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        debug("executing get_participant_set...\n");
        IPeerParticipantSet ret = skeleton.get_participant_set(lvl, caller);
        debug("returning data from get_participant_set.\n");
        return (IPeerParticipantSet)dup_object(ret);
    }

    public IPeersRequest get_request
    (int msg_id, IPeerTupleNode respondant)
    throws Netsukuku.PeersUnknownMessageError, Netsukuku.PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("get_request should not be sent in tcp-fellow");
    }

    public void set_failure
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_failure should not be sent in tcp-fellow");
    }

    public void set_next_destination
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_next_destination should not be sent in tcp-fellow");
    }

    public void set_non_participant
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_non_participant should not be sent in tcp-fellow");
    }

    public void set_participant
    (int p_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_participant should not be sent in tcp-fellow");
    }

    public void set_redo_from_start
    (int msg_id, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_redo_from_start should not be sent in tcp-fellow");
    }

    public void set_refuse_message
    (int msg_id, string refuse_message, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_refuse_message should not be sent in tcp-fellow");
    }

    public void set_response
    (int msg_id, IPeersResponse response, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_response should not be sent in tcp-fellow");
    }
}

public class MyPeersManagerTcpNoWaitStub : Object, IPeersManagerStub
{
    public bool working;
    public Netsukuku.ModRpc.IPeersManagerSkeleton skeleton;
    public MyPeersManagerTcpNoWaitStub(IPeersManagerSkeleton skeleton)
    {
        this.skeleton = skeleton;
        working = true;
    }

    public void forward_peer_message
    (IPeerMessage peer_message)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"calling forward_peer_message $(peer_message.get_type().name())...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        // in a new tasklet...
        ForwardPeerMessageTasklet ts = new ForwardPeerMessageTasklet();
        ts.t = this;
        ts.peer_message = (IPeerMessage)dup_object(peer_message);
        ts.caller = caller;
        tasklet.spawn(ts);
        debug("returning void from forward_peer_message.\n");
    }
    private class ForwardPeerMessageTasklet : Object, INtkdTaskletSpawnable
    {
        public MyPeersManagerTcpNoWaitStub t;
        public IPeerMessage peer_message;
        public MyCallerInfo caller;
        public void * func()
        {
            t.tasklet_forward_peer_message(peer_message, caller);
            return null;
        }
    }
    private void tasklet_forward_peer_message(IPeerMessage peer_message, MyCallerInfo caller)
    {
        debug("executing forward_peer_message...\n");
        skeleton.forward_peer_message(peer_message, caller);
    }

    public IPeerParticipantSet get_participant_set
    (int lvl)
    throws PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("get_participant_set should not be sent in tcp-nowait");
    }

    public IPeersRequest get_request
    (int msg_id, IPeerTupleNode respondant)
    throws Netsukuku.PeersUnknownMessageError, Netsukuku.PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("get_request should not be sent in tcp-nowait");
    }

    public void set_failure
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_failure should not be sent in tcp-nowait");
    }

    public void set_next_destination
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_next_destination should not be sent in tcp-nowait");
    }

    public void set_non_participant
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_non_participant should not be sent in tcp-nowait");
    }

    public void set_participant
    (int p_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_participant should not be sent in tcp-nowait");
    }

    public void set_redo_from_start
    (int msg_id, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_redo_from_start should not be sent in tcp-nowait");
    }

    public void set_refuse_message
    (int msg_id, string refuse_message, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_refuse_message should not be sent in tcp-nowait");
    }

    public void set_response
    (int msg_id, IPeersResponse response, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_response should not be sent in tcp-nowait");
    }
}

public class MyPeersManagerTcpInsideStub : Object, IPeersManagerStub
{
    public bool working;
    public Netsukuku.ModRpc.IPeersManagerSkeleton skeleton;
    public MyPeersManagerTcpInsideStub(IPeersManagerSkeleton? skeleton)
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
        error("forward_peer_message should not be sent in tcp-inside");
    }

    public IPeerParticipantSet get_participant_set
    (int lvl)
    throws PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("get_participant_set should not be sent in tcp-inside");
    }

    public IPeersRequest get_request
    (int msg_id, IPeerTupleNode respondant)
    throws Netsukuku.PeersUnknownMessageError, Netsukuku.PeersInvalidRequest, zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'get_request' msg_id=$(msg_id)...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        IPeersRequest ret = skeleton.get_request(msg_id, ((IPeerTupleNode)dup_object(respondant)), caller);
        return (IPeersRequest)dup_object(ret);
    }

    public void set_failure
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'set_failure' msg_id=$(msg_id)...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_failure(msg_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_next_destination
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'set_next_destination' msg_id=$(msg_id)...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_next_destination(msg_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_non_participant
    (int msg_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'set_non_participant' msg_id=$(msg_id)...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_non_participant(msg_id, ((IPeerTupleGNode)dup_object(tuple)), caller);
    }

    public void set_participant
    (int p_id, IPeerTupleGNode tuple)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        error("set_participant should not be sent in tcp-inside");
    }

    public void set_redo_from_start
    (int msg_id, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'set_redo_from_start' msg_id=$(msg_id)...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_redo_from_start(msg_id, ((IPeerTupleNode)dup_object(respondant)), caller);
    }

    public void set_refuse_message
    (int msg_id, string refuse_message, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'set_refuse_message' msg_id=$(msg_id)...\n");
        if (!working) throw new zcd.ModRpc.StubError.GENERIC("not working");
        var caller = new MyCallerInfo();
        tasklet.ms_wait(2); // simulates network latency
        skeleton.set_refuse_message(msg_id, refuse_message, ((IPeerTupleNode)dup_object(respondant)), caller);
    }

    public void set_response
    (int msg_id, IPeersResponse response, IPeerTupleNode respondant)
    throws zcd.ModRpc.StubError, zcd.ModRpc.DeserializeError
    {
        debug(@"sending to caller 'set_response' msg_id=$(msg_id)...\n");
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
            // in a new tasklet...
            SetParticipantTasklet ts = new SetParticipantTasklet();
            ts.t = this;
            ts.skeleton = skeleton;
            ts.p_id = p_id;
            ts.tuple = (IPeerTupleGNode)dup_object(tuple);
            ts.caller = caller;
            tasklet.spawn(ts);
        }
    }
    private class SetParticipantTasklet : Object, INtkdTaskletSpawnable
    {
        public MyPeersManagerBroadcastStub t;
        public IPeersManagerSkeleton skeleton;
        public int p_id;
        public IPeerTupleGNode tuple;
        public MyCallerInfo caller;
        public void * func()
        {
            t.tasklet_set_participant(skeleton, p_id, tuple, caller);
            return null;
        }
    }
    private void tasklet_set_participant(IPeersManagerSkeleton skeleton, int p_id, IPeerTupleGNode tuple, MyCallerInfo caller)
    {
        skeleton.set_participant(p_id, tuple, caller);
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

