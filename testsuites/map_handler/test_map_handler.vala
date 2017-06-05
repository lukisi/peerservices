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

    public void test_1()
    {
        ArrayList<int> gsizes = new ArrayList<int>.wrap({2,2,2});

        /* Node 01. Identity 01. Its address is 1·0·1. It's alone in the network N1.
         * It participates in service #1.
         */
        MapHolder no1id1 = new MapHolder("no1id1", new ArrayList<int>.wrap({1,0,1}), gsizes);
        no1id1.handler.create_net();
        no1id1.participate(1);
        tasklet.ms_wait(10);
        print("step 1\n");

        /* Node 02. Identity 01. Its address is 0·0·0. It's alone in the network N2.
         * It participates in service #1.
         */
        MapHolder no2id1 = new MapHolder("no2id1", new ArrayList<int>.wrap({0,0,0}), gsizes);
        no2id1.handler.create_net();
        no2id1.participate(1);
        tasklet.ms_wait(10);
        print("step 2\n");

        /* Node 01 enters in N2 as a single node. From identity 01 creates identity 02.
         * It obtains address 0·0·1 in the existing g-node 0·0.
         * It participates in service #1.
         * A new arc is formed between no1id2 and no2id1.
         */
        MapHolder no1id2 = new MapHolder("no1id2", new ArrayList<int>.wrap({1,0,0}), gsizes);
        no1id2.set_neighbor(no2id1);
        no2id1.set_neighbor(no1id2);
        no1id2.handler.enter_net(no1id1.handler, 0, 1);
        no1id2.participate(1);
        tasklet.ms_wait(100);
        print("step 3\n");

        /* Node 03. Identity 01. Its address is 1·0·1. It's alone in the network N3.
         * It participates in service #1.
         */
        MapHolder no3id1 = new MapHolder("no3id1", new ArrayList<int>.wrap({1,0,1}), gsizes);
        no3id1.handler.create_net();
        no3id1.participate(1);
        tasklet.ms_wait(10);
        print("step 4\n");

        /* Nodes no1id2 and no2id1 together form a g-node g01 of level 1. g01 has address
         * 0·0 in N2.
         * Now g01 enters in N3 at address 1·1. Because of an arc between no3id1 and no2id2.
         * Node no1id3 is created from no1id2.
         * Node no2id2 is created from no2id1.
         * We duplicate the arc between no1id3 and no2id2.
         */
        MapHolder no1id3 = new MapHolder("no1id3", new ArrayList<int>.wrap({1,1,1}), gsizes);
        MapHolder no2id2 = new MapHolder("no2id2", new ArrayList<int>.wrap({0,1,1}), gsizes);
        no1id3.set_neighbor(no2id2);
        no2id2.set_neighbor(no1id3);
        no3id1.set_neighbor(no2id2);
        no2id2.set_neighbor(no3id1);
        no1id3.handler.enter_net(no1id2.handler, 1, 2);
        no2id2.handler.enter_net(no2id1.handler, 1, 2);
        no1id3.participate(1);
        no2id2.participate(1);
        tasklet.ms_wait(100);
        print("step 5\n");
    }

    class MapHolder : Object
    {
        public string name;
        public ArrayList<int> pos;
        public ArrayList<int> gsizes;
        public PeerParticipantSet map;
        public Gee.List<int> my_services;
        public int levels;
        public HashMap<int, ArrayList<MapHolder>> neighbors;
        public MapHandler.MapHandler handler;
        public MapHolder(string name, ArrayList<int> pos, ArrayList<int> gsizes)
        {
            this.name = name;
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            this.levels = pos.size;
            map = new PeerParticipantSet(pos);
            my_services = new ArrayList<int>();
            neighbors = new HashMap<int, ArrayList<MapHolder>>();
            handler = new MapHandler.MapHandler
                (pos,
                 /*ClearMapsAtLevel*/ (lvl) => {
                     print(@"$(name): Call to clear_maps_at_level($(lvl)).\n");
                     clear_maps_at_level(lvl);
                 },
                 /*AddParticipant*/ (p_id, h) => {
                     print(@"$(name): Call to add_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                     if (h.pos == pos[h.lvl])
                     {
                         print ("Ignore because it is my position.\n");
                         return;
                     }
                     add_participant(p_id, h);
                 },
                 /*RemoveParticipant*/ (p_id, h) => {
                     print(@"$(name): Call to remove_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
                     if (h.pos == pos[h.lvl])
                     {
                         print ("Ignore because it is my position.\n");
                         return;
                     }
                     remove_participant(p_id, h);
                 },
                 /*ProduceMapsCopy*/ () => {
                     print(@"$(name): Call to produce_maps().\n");
                     return produce_maps_copy();
                 },
                 /*GetNeighborAtLevel*/ (lvl, failing_stub) => {
                     string s_f_s = (failing_stub == null) ? "null" : "[object]";
                     print(@"$(name): Call to get_neighbor_at_level($(lvl), $(s_f_s)).\n");
                     IPeersManagerStub? ret = get_neighbor_at_level(lvl, failing_stub);
                     if (ret == null)
                         print(@"        Returning null.\n");
                     else
                     {
                         FakeUnicastStub _ret = (FakeUnicastStub)ret;
                         print(@"        Returning $(_ret.holder.name).\n");
                     }
                     return ret;
                 },
                 /*GetBroadcastNeighbors*/ (fn_mah) => {
                     print(@"$(name): Call to get_broadcast_neighbors(fn_mah).\n");
                     FakeBroadcastStub ret = (FakeBroadcastStub)get_broadcast_neighbors();
                     string list = ""; string next = "";
                     foreach (MapHolder holder in ret.holders) {
                         list += @"$(next)$(holder.name)"; next = ", ";
                     }
                     print(@"        Returning [$(list)].\n");
                     return ret;
                 },
                 /*GetUnicastNeighbor*/ (missing_arc) => {
                     print(@"$(name): Call to get_unicast_neighbor(missing_arc=?).\n");
                     // TODO
                     error("not yet implemented. We must test to verify that closures just work.");
                 });
        }

        public void participate(int p_id)
        {
            if (! (p_id in my_services)) my_services.add(p_id);
            handler.participate(p_id);
        }

        public void dont_participate(int p_id)
        {
            if (p_id in my_services) my_services.remove(p_id);
            handler.dont_participate(p_id);
        }

        // receives a RPC unicast: ask_participant_maps
        public PeerParticipantSet ask_participant_maps()
        {
            return handler.produce_maps_below_level(handler.maps_retrieved_below_level);
        }

        // receives a RPC broadcast: give_participant_maps
        public void give_participant_maps(PeerParticipantSet maps)
        {
            handler.give_participant_maps(maps);
        }

        // receives a RPC broadcast: set_participant
        public void set_participant(int p_id, PeerTupleGNode tuple)
        {
            // Check (since the request is from network) that the service is optional.
            // In this testsuite we assume: yes.

            assert(tuple.check_valid(levels, gsizes.to_array()));
            handler.set_participant(p_id, tuple);
        }

        public void set_neighbor(MapHolder n)
        {
            int lvl = levels -1;
            while (pos[lvl] == n.pos[lvl])
            {
                lvl--;
                assert(lvl >= 0);
            }
            if (! (neighbors.has_key(lvl)))
                neighbors[lvl] = new ArrayList<MapHolder>();
            neighbors[lvl].add(n);
        }

        public void unset_neighbor(MapHolder n)
        {
            int lvl = levels -1;
            while (pos[lvl] == n.pos[lvl])
            {
                lvl--;
                assert(lvl >= 0);
            }
            if (neighbors.has_key(lvl))
            {
                neighbors[lvl].remove(n);
                if (neighbors[lvl].is_empty) neighbors.unset(lvl);
            }
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
            if (h.pos == pos[h.lvl]) return; // ignore myself
            if (! map.participant_set.has_key(p_id))
                map.participant_set[p_id] = new PeerParticipantMap();
            var the_list = map.participant_set[p_id].participant_list;
            if (! (h in the_list)) the_list.add(h);
        }
        public void remove_participant(int p_id, HCoord h)
        {
            if (h.pos == pos[h.lvl]) return; // ignore myself
            if (map.participant_set.has_key(p_id))
            {
                var the_list = map.participant_set[p_id].participant_list;
                if (h in the_list) the_list.remove(h);
                if (the_list.is_empty) map.participant_set.unset(p_id);
            }
        }
        public PeerParticipantSet produce_maps_copy()
        {
            var ret = new PeerParticipantSet(pos);
            foreach (int p_id in map.participant_set.keys)
            {
                ret.participant_set[p_id] = new PeerParticipantMap();
                ret.participant_set[p_id].participant_list.add_all
                    (map.participant_set[p_id].participant_list);
            }
            foreach (int p_id in my_services)
            {
                if (! ret.participant_set.has_key(p_id))
                    ret.participant_set[p_id] = new PeerParticipantMap();
                ret.participant_set[p_id].participant_list.add(new HCoord(0, pos[0]));
            }
            return ret;
        }
        public IPeersManagerStub? get_neighbor_at_level(int lvl, IPeersManagerStub? failing_stub)
        {
            FakeUnicastStub? nstub = null;
            if (failing_stub != null)
            {
                FakeUnicastStub failing_stub_real = (FakeUnicastStub)failing_stub;
                MapHolder failing_holder = failing_stub_real.holder;
                unset_neighbor(failing_holder);
                failing_holder.unset_neighbor(this);
            }
            if (neighbors.has_key(lvl))
            {
                nstub = new FakeUnicastStub(neighbors[lvl][0]);
            }
            return nstub;
        }
        public IPeersManagerStub get_broadcast_neighbors()
        {
            FakeBroadcastStub bstub = new FakeBroadcastStub();
            foreach (int lvl in neighbors.keys)
                bstub.holders.add_all(neighbors[lvl]);
            return bstub;
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

    class FakeUnicastStub : Object, IPeersManagerStub
    {
        public MapHolder holder;
        public FakeUnicastStub(MapHolder holder)
        {
            this.holder = holder;
        }

        public IPeerParticipantSet ask_participant_maps () throws StubError, DeserializeError
        {
            tasklet.ms_wait(2); // simulates network latency
            return holder.ask_participant_maps();
        }

        public void forward_peer_message (IPeerMessage peer_message) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public IPeerParticipantSet get_participant_set (int lvl) throws PeersInvalidRequest, StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public IPeersRequest get_request (int msg_id, IPeerTupleNode respondant) throws PeersUnknownMessageError, PeersInvalidRequest, StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void give_participant_maps (IPeerParticipantSet maps) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_failure (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_next_destination (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_non_participant (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_participant (int p_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_redo_from_start (int msg_id, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_refuse_message (int msg_id, string refuse_message, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_response (int msg_id, IPeersResponse response, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }
    }

    class FakeBroadcastStub : Object, IPeersManagerStub
    {
        public Gee.List<MapHolder> holders;
        public FakeBroadcastStub()
        {
            holders = new ArrayList<MapHolder>();
        }

        public IPeerParticipantSet ask_participant_maps () throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void forward_peer_message (IPeerMessage peer_message) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public IPeerParticipantSet get_participant_set (int lvl) throws PeersInvalidRequest, StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public IPeersRequest get_request (int msg_id, IPeerTupleNode respondant) throws PeersUnknownMessageError, PeersInvalidRequest, StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void give_participant_maps (IPeerParticipantSet maps) throws StubError, DeserializeError
        {
            tasklet.ms_wait(2); // simulates network latency
            foreach (MapHolder holder in holders)
            {
                // make a copy
                PeerParticipantSet maps_copy = (PeerParticipantSet)dup_object(maps);
                // in a tasklet
                GiveParticipantMapsTasklet ts = new GiveParticipantMapsTasklet();
                ts.t = this;
                ts.maps = maps_copy;
                ts.holder = holder;
                tasklet.spawn(ts);
            }
        }
        class GiveParticipantMapsTasklet : Object, ITaskletSpawnable
        {
            public FakeBroadcastStub t;
            public PeerParticipantSet maps;
            public MapHolder holder;
            public void * func()
            {
                t.give_participant_maps_tasklet(holder, maps);
                return null;
            }
        }
        private void give_participant_maps_tasklet(MapHolder holder, PeerParticipantSet maps)
        {
            holder.give_participant_maps(maps);
        }

        public void set_failure (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_next_destination (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_non_participant (int msg_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_participant (int p_id, IPeerTupleGNode tuple) throws StubError, DeserializeError
        {
            tasklet.ms_wait(2); // simulates network latency
            foreach (MapHolder holder in holders)
            {
                // make a copy
                PeerTupleGNode tuple_copy = (PeerTupleGNode)dup_object(tuple);
                // in a tasklet
                SetParticipantTasklet ts = new SetParticipantTasklet();
                ts.t = this;
                ts.p_id = p_id;
                ts.tuple = tuple_copy;
                ts.holder = holder;
                tasklet.spawn(ts);
            }
        }
        class SetParticipantTasklet : Object, ITaskletSpawnable
        {
            public FakeBroadcastStub t;
            public int p_id;
            public PeerTupleGNode tuple;
            public MapHolder holder;
            public void * func()
            {
                t.set_participant_tasklet(holder, p_id, tuple);
                return null;
            }
        }
        private void set_participant_tasklet(MapHolder holder, int p_id, PeerTupleGNode tuple)
        {
            holder.set_participant(p_id, tuple);
        }

        public void set_redo_from_start (int msg_id, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_refuse_message (int msg_id, string refuse_message, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }

        public void set_response (int msg_id, IPeersResponse response, IPeerTupleNode respondant) throws StubError, DeserializeError
        {
            error("not implemented yet");
        }
    }
}

//FAKE

namespace Netsukuku.PeerServices
{
    internal ITasklet tasklet;
}
