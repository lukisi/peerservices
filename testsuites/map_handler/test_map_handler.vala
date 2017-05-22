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

class PeersTester : Object
{
    public void set_up ()
    {
    }

    public void tear_down ()
    {
    }

    public void test_1()
    {
        int levels = 3;
        ArrayList<int> gsizes = new ArrayList<int>.wrap({2,2,2});

        // My first address is 1·0·1. I am alone in the network. I participate in service #1.
        MapHolder id1 = new MapHolder(new ArrayList<int>.wrap({1,0,1}));
        id1.participate(1);
        MapHandler.MapHandler map_handler_1 = new MapHandler.MapHandler
            (levels,
             /*ClearMapsAtLevel*/ (lvl) => {
                 print(@"map_handler_1: Call to clear_maps_at_level($(lvl)).\n");
                 id1.clear_maps_at_level(lvl);
             },
             /*AddParticipant*/ (p_id, h) => {
                 print(@"map_handler_1: Call to add_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                 if (h.pos == id1.pos[h.lvl])
                 {
                     print ("Ignore because it is my position.\n");
                     return;
                 }
                 id1.add_participant(p_id, h);
             },
             /*RemoveParticipant*/ (p_id, h) => {
                 print(@"map_handler_1: Call to remove_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                 if (h.pos == id1.pos[h.lvl])
                 {
                     print ("Ignore because it is my position.\n");
                     return;
                 }
                 id1.remove_participant(p_id, h);
             },
             /*ProduceMapsCopy*/ () => {
                 print(@"map_handler_1: Call to produce_maps().\n");
                 return id1.produce_maps_copy();
             });
        map_handler_1.create_net();

        // I enter a new network as a single node. I obtain address 0·0·1 in the
        //  existing g-node 0·0.
        MapHolder id2 = new MapHolder(new ArrayList<int>.wrap({1,0,0}));
        id2.participate(1);
        MapHandler.MapHandler map_handler_2 = new MapHandler.MapHandler
            (levels,
             /*ClearMapsAtLevel*/ (lvl) => {
                 print(@"map_handler_2: Call to clear_maps_at_level($(lvl)).\n");
                 id2.clear_maps_at_level(lvl);
             },
             /*AddParticipant*/ (p_id, h) => {
                 print(@"map_handler_2: Call to add_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                 if (h.pos == id2.pos[h.lvl])
                 {
                     print ("Ignore because it is my position.\n");
                     return;
                 }
                 id2.add_participant(p_id, h);
             },
             /*RemoveParticipant*/ (p_id, h) => {
                 print(@"map_handler_2: Call to remove_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                 if (h.pos == id2.pos[h.lvl])
                 {
                     print ("Ignore because it is my position.\n");
                     return;
                 }
                 id2.remove_participant(p_id, h);
             },
             /*ProduceMapsCopy*/ () => {
                 print(@"map_handler_2: Call to produce_maps().\n");
                 return id2.produce_maps_copy();
             });
        map_handler_2.enter_net(map_handler_1, 0, 1);
    }

    class MapHolder : Object
    {
        public ArrayList<int> pos;
        public PeerParticipantSet map;
        public int levels;
        public MapHolder(ArrayList<int> pos)
        {
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.levels = pos.size;
            map = new PeerParticipantSet();
        }
        public void participate(int p_id)
        {
            if (! map.participant_set.has_key(p_id))
                map.participant_set[p_id] = new PeerParticipantMap();
            for (int lvl = 0; lvl < levels; lvl++)
                map.participant_set[p_id].participant_list.add(new HCoord(lvl, pos[lvl]));
        }
        public void clear_maps_at_level(int lvl)
        {
            foreach (int p_id in map.participant_set.keys)
            {
                PeerParticipantMap m = map.participant_set[p_id];
                ArrayList<HCoord> to_del = new ArrayList<HCoord>();
                foreach (HCoord h in m.participant_list) if (h.lvl == lvl) to_del.add(h);
                m.participant_list.remove_all(to_del);
            }
        }
        public void add_participant(int p_id, HCoord h)
        {
            if (! map.participant_set.has_key(p_id))
                map.participant_set[p_id] = new PeerParticipantMap();
            map.participant_set[p_id].participant_list.add(h);
        }
        public void remove_participant(int p_id, HCoord h)
        {
            if (map.participant_set.has_key(p_id))
                map.participant_set[p_id].participant_list.remove(h);
        }
        public PeerParticipantSet produce_maps_copy()
        {
            var ret = new PeerParticipantSet();
            foreach (int p_id in map.participant_set.keys)
            {
                ret.participant_set[p_id] = new PeerParticipantMap();
                ret.participant_set[p_id].participant_list.add_all
                    (map.participant_set[p_id].participant_list);
            }
            return ret;
        }
    }

    public static int main(string[] args)
    {
        GLib.Test.init(ref args);

        // Initialize tasklet system
        PthTaskletImplementer.init();
        tasklet = PthTaskletImplementer.get_tasklet_system();

        GLib.Test.add_func ("/MapHandler/1", () => {
            var x = new PeersTester();
            x.set_up();
            x.test_1();
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
