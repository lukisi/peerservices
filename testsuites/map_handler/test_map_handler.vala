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
        int[] gsizes = {2,2,2};

        int[] pos_1 = {1,0,1};
        MapHandler.MapHandler map_handler_1 = new MapHandler.MapHandler
            (levels,
             /*ClearMapsAtLevel*/ (lvl) => {
                 print(@"map_handler_1: Call to clear_maps_at_level($(lvl)).\n");
             },
             /*AddParticipant*/ (p_id, h) => {
                 print(@"map_handler_1: Call to add_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                 if (h.pos == pos_1[h.lvl]) print ("Ignore because it is my position.\n");
             },
             /*RemoveParticipant*/ (p_id, h) => {
                 print(@"map_handler_1: Call to remove_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
             },
             /*ProduceMapsCopy*/ () => {
                 var ret = new PeerParticipantSet();
                 print(@"map_handler_1: Call to produce_maps().\n");
                 ret.participant_set[1] = new PeerParticipantMap();
                 for (int lvl = 0; lvl < levels; lvl++)
                     ret.participant_set[1].participant_list.add(new HCoord(lvl, pos_1[lvl]));
                 return ret;
             });
        map_handler_1.create_net();

        int[] pos_2 = {1,0,0};
        MapHandler.MapHandler map_handler_2 = new MapHandler.MapHandler
            (levels,
             /*ClearMapsAtLevel*/ (lvl) => {
                 print(@"map_handler_2: Call to clear_maps_at_level($(lvl)).\n");
             },
             /*AddParticipant*/ (p_id, h) => {
                 print(@"map_handler_2: Call to add_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                 if (h.pos == pos_2[h.lvl]) print ("Ignore because it is my position.\n");
             },
             /*RemoveParticipant*/ (p_id, h) => {
                 print(@"map_handler_2: Call to remove_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
             },
             /*ProduceMapsCopy*/ () => {
                 var ret = new PeerParticipantSet();
                 print(@"map_handler_2: Call to produce_maps().\n");
                 
                 return ret;
             });
        map_handler_2.enter_net(map_handler_1, 1, 2);
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
