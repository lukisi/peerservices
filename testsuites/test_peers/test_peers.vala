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
    public void print_out(string s)
    {
        if (output) print(s);
    }

    public void set_up ()
    {
        logger = "";
    }

    public void tear_down ()
    {
        logger = "";
    }

    public void test_node()
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

    public void test_gnode()
    {
        PeerTupleGNode gn0;
        {
            uchar[] orig;
            {
                Gee.List<int> nums = new ArrayList<int>();
                nums.add_all_array({1, 2, 3, 4});
                PeerTupleGNode gn = new PeerTupleGNode(nums, 3);
                orig = gn.serialize();
            }
            uchar []dest = new uchar[orig.length];
            for (int i = 0; i < orig.length; i++) dest[i] = orig[i];
            gn0 = (PeerTupleGNode)ISerializable.deserialize(dest);
        }
        assert(gn0.tuple.size == 4);
        assert(gn0.tuple[2] == 3);
        assert(gn0.top == 3);
    }

    public void test_cont()
    {
        PeerTupleGNodeContainer cont = new PeerTupleGNodeContainer();

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

    public void test_message()
    {
        PeerMessageForwarder mf0;
        {
            uchar[] orig;
            {
                PeerMessageForwarder mf = new PeerMessageForwarder();
                Gee.List<int> nums = new ArrayList<int>();
                nums.add_all_array({1, 2, 3, 4});
                mf.n = new PeerTupleNode(nums);
                nums = new ArrayList<int>();
                nums.add_all_array({1, 2, 3});
                mf.x_macron = new PeerTupleNode(nums);
                mf.lvl = 3;
                mf.pos = 2;
                mf.reverse = true;
                mf.msg_id = 12345;
                mf.p_id = 12;
                nums = new ArrayList<int>();
                nums.add_all_array({4});
                mf.exclude_tuple_list.add(new PeerTupleGNode(nums, 3));
                nums = new ArrayList<int>();
                nums.add_all_array({0});
                mf.non_participant_tuple_list.add(new PeerTupleGNode(nums, 5));
                nums = new ArrayList<int>();
                nums.add_all_array({3, 6});
                mf.non_participant_tuple_list.add(new PeerTupleGNode(nums, 5));
                nums = new ArrayList<int>();
                nums.add_all_array({4, 6});
                mf.non_participant_tuple_list.add(new PeerTupleGNode(nums, 5));
                nums = new ArrayList<int>();
                nums.add_all_array({3, 2, 5, 6});
                mf.non_participant_tuple_list.add(new PeerTupleGNode(nums, 5));
                orig = mf.serialize();
            }
            uchar []dest = new uchar[orig.length];
            for (int i = 0; i < orig.length; i++) dest[i] = orig[i];
            mf0 = (PeerMessageForwarder)ISerializable.deserialize(dest);
        }
        assert(mf0.n.tuple.size == 4 &&
               mf0.n.tuple[0] == 1 &&
               mf0.n.tuple[1] == 2 &&
               mf0.n.tuple[2] == 3 &&
               mf0.n.tuple[3] == 4);
        assert(mf0.x_macron.tuple.size == 3 &&
               mf0.x_macron.tuple[0] == 1 &&
               mf0.x_macron.tuple[1] == 2 &&
               mf0.x_macron.tuple[2] == 3);
        assert(mf0.lvl == 3);
        assert(mf0.pos == 2);
        assert(mf0.p_id == 12);
        assert(mf0.msg_id == 12345);
        assert(mf0.reverse);
        assert(mf0.exclude_tuple_list.size == 1);
        bool found = false;
        foreach (PeerTupleGNode gn in mf0.exclude_tuple_list)
        {
            if (gn.top == 3 &&
                gn.tuple.size == 1 &&
                gn.tuple[0] == 4) found = true;
        }
        assert(found);
        assert(mf0.non_participant_tuple_list.size == 4);
        found = false;
        foreach (PeerTupleGNode gn in mf0.non_participant_tuple_list)
        {
            if (gn.top == 5 &&
                gn.tuple.size == 1 &&
                gn.tuple[0] == 0) found = true;
        }
        assert(found);
        found = false;
        foreach (PeerTupleGNode gn in mf0.non_participant_tuple_list)
        {
            if (gn.top == 5 &&
                gn.tuple.size == 2 &&
                gn.tuple[0] == 3 &&
                gn.tuple[1] == 6) found = true;
        }
        assert(found);
        found = false;
        foreach (PeerTupleGNode gn in mf0.non_participant_tuple_list)
        {
            if (gn.top == 5 &&
                gn.tuple.size == 2 &&
                gn.tuple[0] == 4 &&
                gn.tuple[1] == 6) found = true;
        }
        assert(found);
        found = false;
        foreach (PeerTupleGNode gn in mf0.non_participant_tuple_list)
        {
            if (gn.top == 5 &&
                gn.tuple.size == 4 &&
                gn.tuple[0] == 3 &&
                gn.tuple[1] == 2 &&
                gn.tuple[2] == 5 &&
                gn.tuple[3] == 6) found = true;
        }
        assert(found);
    }

    public void test_participating()
    {
/*        PeerParticipatingSet s0;
        {
            uchar[] orig;
            {
                PeerParticipatingMap m12 = new PeerParticipatingMap();
                m12.participating_list.add(new HCoord(3, 2));
                m12.participating_list.add(new HCoord(4, 1));
                m12.participating_list.add(new HCoord(0, 2));
                PeerParticipatingMap m15 = new PeerParticipatingMap();
                m15.participating_list.add(new HCoord(3, 5));
                PeerParticipatingSet s = new PeerParticipatingSet();
                s.participating_set[12] = m12;
                s.participating_set[15] = m15;
                orig = s.serialize();
            }
            uchar []dest = new uchar[orig.length];
            for (int i = 0; i < orig.length; i++) dest[i] = orig[i];
            s0 = (PeerParticipatingSet)ISerializable.deserialize(dest);
        }
        assert(s0.participating_set.size == 2);
        assert(s0.participating_set.has_key(12));
        PeerParticipatingMap m = s0.participating_set[12];
        assert(m.participating_list.size == 3);
        assert(m.participating_list.contains(new HCoord(3, 2)));
        assert(m.participating_list.contains(new HCoord(4, 1)));
        assert(m.participating_list.contains(new HCoord(0, 2)));
        assert(s0.participating_set.has_key(15));
        m = s0.participating_set[15];
        assert(m.participating_list.size == 1);
        assert(m.participating_list.contains(new HCoord(3, 5)));*/
    }

    public static int main(string[] args)
    {
        GLib.Test.init(ref args);
        Tasklet.init();
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
        GLib.Test.add_func ("/GNodeContainer/Tests", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_cont();
            x.tear_down();
        });
        GLib.Test.add_func ("/Serializables/MessageForwarder", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_message();
            x.tear_down();
        });
        GLib.Test.add_func ("/Serializables/ParticipatingSet", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_participating();
            x.tear_down();
        });
        GLib.Test.run();
        Tasklet.kill();
        return 0;
    }
}

