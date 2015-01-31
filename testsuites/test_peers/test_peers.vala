/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2014-2015 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
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

using Tasklets;
using Gee;
using zcd;
using Netsukuku;

class PeersTester : Object
{
    string logger;
    const bool output = false;

    public void set_up ()
    {
        logger = "";
    }

    public void tear_down ()
    {
        logger = "";
    }

    public void test_ser ()
    {
        PeerTupleNode n0;
        {
            uchar[] orig;
            {
                Gee.List<int> nums = new ArrayList<int>();
                nums.add_all_array({1, 2, 3, 4});
                PeerTupleNode n = new PeerTupleNode(nums);
                orig = n.serialize();
            }
            uchar []dest = new uchar[orig.length];
            for (int i = 0; i < orig.length; i++) dest[i] = orig[i];
            n0 = (PeerTupleNode)ISerializable.deserialize(dest);
        }
        assert(n0.tuple.size == 4);
        assert(n0.tuple[2] == 3);
    }

    public static int main(string[] args)
    {
        GLib.Test.init(ref args);
        Tasklet.init();
        GLib.Test.add_func ("/Peers/Serializables", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_ser();
            x.tear_down();
        });
        GLib.Test.run();
        Tasklet.kill();
        return 0;
    }
}

