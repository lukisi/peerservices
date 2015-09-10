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

using Gee;
using zcd;
using Netsukuku;

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
                PeerTupleGNode gn = new PeerTupleGNode(nums, 3);
                node = Json.gobject_serialize(gn);
            }
            gn0 = (PeerTupleGNode)Json.gobject_deserialize(typeof(PeerTupleGNode), node);
        }
        assert(gn0.tuple.size == 4);
        assert(gn0.tuple[2] == 3);
        assert(gn0.top == 3);
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

    public void test_message()
    {
        PeerMessageForwarder mf0;
        {
            Json.Node node;
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
                node = Json.gobject_serialize(mf);
            }
            mf0 = (PeerMessageForwarder)Json.gobject_deserialize(typeof(PeerMessageForwarder), node);
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

    public void test_participant()
    {
        PeerParticipantSet s0;
        {
            Json.Node node;
            {
                PeerParticipantMap m12 = new PeerParticipantMap();
                m12.participant_list.add(new HCoord(3, 2));
                m12.participant_list.add(new HCoord(4, 1));
                m12.participant_list.add(new HCoord(0, 2));
                PeerParticipantMap m15 = new PeerParticipantMap();
                m15.participant_list.add(new HCoord(3, 5));
                PeerParticipantSet s = new PeerParticipantSet();
                s.participant_set[12] = m12;
                s.participant_set[15] = m15;
                node = Json.gobject_serialize(s);
            }
            s0 = (PeerParticipantSet)Json.gobject_deserialize(typeof(PeerParticipantSet), node);
        }
        assert(s0.participant_set.size == 2);
        assert(s0.participant_set.has_key(12));
        PeerParticipantMap m = s0.participant_set[12];
        assert(m.participant_list.size == 3);
        assert(m.participant_list.contains(new HCoord(3, 2)));
        assert(m.participant_list.contains(new HCoord(4, 1)));
        assert(m.participant_list.contains(new HCoord(0, 2)));
        assert(s0.participant_set.has_key(15));
        m = s0.participant_set[15];
        assert(m.participant_list.size == 1);
        assert(m.participant_list.contains(new HCoord(3, 5)));
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
        GLib.Test.add_func ("/Serializables/ParticipantSet", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_participant();
            x.tear_down();
        });
        GLib.Test.run();
        return 0;
    }
}

