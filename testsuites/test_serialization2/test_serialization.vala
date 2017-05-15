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
using Netsukuku.PeerServices;

class PeersTester : Object
{
    public void set_up ()
    {
    }

    public void tear_down ()
    {
    }

    public void test_node()
    {
        PeerTupleNode n0;
        {
            Json.Node node;
            {
                Gee.List<int> nums = new ArrayList<int>();
                nums.add_all_array({1, 2, 3, 4});
                PeerTupleNode n = new PeerTupleNode(nums);
                node = Json.gobject_serialize(n);
            }
            n0 = (PeerTupleNode)Json.gobject_deserialize(typeof(PeerTupleNode), node);
        }
        assert(n0.tuple.size == 4);
        assert(n0.tuple[2] == 3);
    }

    public void test_gnode()
    {
        PeerTupleGNode gn0;
        {
            Json.Node node;
            {
                Gee.List<int> nums = new ArrayList<int>();
                nums.add_all_array({1, 2, 3, 4});
                PeerTupleGNode gn = new PeerTupleGNode(nums, 5);
                node = Json.gobject_serialize(gn);
            }
            gn0 = (PeerTupleGNode)Json.gobject_deserialize(typeof(PeerTupleGNode), node);
        }
        assert(gn0.tuple.size == 4);
        assert(gn0.tuple[2] == 3);
        assert(gn0.top == 5);
    }

    public static int main(string[] args)
    {
        GLib.Test.init(ref args);
        GLib.Test.add_func ("/Serializables/TupleNode", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_node();
            x.tear_down();
        });
        GLib.Test.add_func ("/Serializables/TupleGNode", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_gnode();
            x.tear_down();
        });
        GLib.Test.run();
        return 0;
    }
}

