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
            new MessageRouting.MessageRouting(my_pos, gsizes);
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

    private string address(Gee.List<int> pos)
    {
        string ret = ""; string next = "";
        foreach (int p in pos) {
            ret = @"$(p)$(next)$(ret)";
            next = ":";
        }
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
        GLib.Test.run();
        return 0;
    }

}

//FAKE

namespace Netsukuku.PeerServices
{
    internal ITasklet tasklet;
}
