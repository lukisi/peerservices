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

class PeersTester : Object
{
    public void set_up ()
    {
    }

    public void tear_down ()
    {
    }

    public void test_cont()
    {
        PeerTupleGNodeContainer cont = new PeerTupleGNodeContainer(5);

        Gee.List<int> nums = new ArrayList<int>();
        nums.add_all_array({1, 2, 3, 4});
        PeerTupleGNode gn_inside = new PeerTupleGNode(nums, 5);
        cont.add(gn_inside);
        nums = new ArrayList<int>();
        nums.add_all_array({3, 4});
        PeerTupleGNode gn_outside = new PeerTupleGNode(nums, 5);
        cont.add(gn_outside);
        assert(cont.list.size == 1);

        nums = new ArrayList<int>();
        nums.add_all_array({3, 2});
        PeerTupleGNode gn_another = new PeerTupleGNode(nums, 5);
        cont.add(gn_another);
        nums = new ArrayList<int>();
        nums.add_all_array({1, 3, 2});
        gn_another = new PeerTupleGNode(nums, 5);
        cont.add(gn_another);
        assert(cont.list.size == 2);

        bool chk1 = false;
        bool chk2 = false;
        foreach (PeerTupleGNode gn in cont.list)
        {
            if (gn.tuple.size == 2 &&
                gn.tuple[0] == 3 &&
                gn.tuple[1] == 2)
                    chk1 = true;
            if (gn.tuple.size == 2 &&
                gn.tuple[0] == 3 &&
                gn.tuple[1] == 4)
                    chk2 = true;
        }
        assert(chk1);
        assert(chk2);
    }

    public static int main(string[] args)
    {
        GLib.Test.init(ref args);
        GLib.Test.add_func ("/DataStructs/PeerTupleGNodeContainer", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_cont();
            x.tear_down();
        });
        GLib.Test.run();
        return 0;
    }
}

