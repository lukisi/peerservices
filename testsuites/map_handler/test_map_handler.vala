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
        int levels = 3;
        ArrayList<int> gsizes = new ArrayList<int>.wrap({2,2,2});

        /* Node 01. Identity 01. Its address is 1·0·1. It's alone in the network N1.
         * It participates in service #1.
         */
        MapHolder no1id1 = new MapHolder("no1id1", new ArrayList<int>.wrap({1,0,1}));
        no1id1.participate(1);
        no1id1.handler.create_net();
        tasklet.ms_wait(10);

        /* Node 02. Identity 01. Its address is 0·0·0. It's alone in the network N2.
         * It participates in service #1.
         */
        MapHolder no2id1 = new MapHolder("no2id1", new ArrayList<int>.wrap({0,0,0}));
        no2id1.participate(1);
        no2id1.handler.create_net();
        tasklet.ms_wait(10);

        /* Node 01 enters in N2 as a single node. From identity 01 creates identity 02.
         * It obtains address 0·0·1 in the existing g-node 0·0.
         * It participates in service #1.
         * A new arc is formed between no1id2 and no2id1.
         */
        MapHolder no1id2 = new MapHolder("no1id2", new ArrayList<int>.wrap({1,0,0}));
        no1id2.participate(1);
        no1id2.set_neighbor(no2id1);
        no2id1.set_neighbor(no1id2);
        no1id2.handler.enter_net(no1id1.handler, 0, 1);
        tasklet.ms_wait(100);
    }

    class MapHolder : Object
    {
        public string name;
        public ArrayList<int> pos;
        public PeerParticipantSet map;
        public int levels;
        public HashMap<int, ArrayList<MapHolder>> neighbors;
        public MapHandler.MapHandler handler;
        public MapHolder(string name, ArrayList<int> pos)
        {
            this.name = name;
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.levels = pos.size;
            map = new PeerParticipantSet();
            neighbors = new HashMap<int, ArrayList<MapHolder>>();
            handler = new MapHandler.MapHandler
                (levels,
                 callback_clear_maps_at_level,
                 callback_add_participant,
                 callback_remove_participant,
                 callback_produce_maps_copy,
                 callback_get_neighbor_at_level,
                 callback_get_broadcast_neighbors);
        }

        public void participate(int p_id)
        {
            if (! map.participant_set.has_key(p_id))
                map.participant_set[p_id] = new PeerParticipantMap();
            for (int lvl = 0; lvl < levels; lvl++)
                map.participant_set[p_id].participant_list.add(new HCoord(lvl, pos[lvl]));
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

        private void callback_clear_maps_at_level(int lvl)
        {
            print(@"$(name): Call to clear_maps_at_level($(lvl)).\n");
            clear_maps_at_level(lvl);
        }
        private void callback_add_participant(int p_id, HCoord h)
        {
            print(@"$(name): Call to add_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
            if (h.pos == pos[h.lvl])
            {
                print ("Ignore because it is my position.\n");
                return;
            }
            add_participant(p_id, h);
        }
        private void callback_remove_participant(int p_id, HCoord h)
        {
            print(@"$(name): Call to remove_participant($(p_id), ($(h.lvl), $(h.pos))).\n");
            if (h.pos == pos[h.lvl])
            {
                print ("Ignore because it is my position.\n");
                return;
            }
            remove_participant(p_id, h);
        }
        private PeerParticipantSet callback_produce_maps_copy()
        {
            print(@"$(name): Call to produce_maps().\n");
            return produce_maps_copy();
        }
        private IPeersManagerStub? callback_get_neighbor_at_level(int lvl, IPeersManagerStub? failing_stub)
        {
            string s_f_s = (failing_stub == null) ? "null" : "[object]";
            print(@"$(name): Call to get_neighbor_at_level($(lvl), $(s_f_s)).\n");
            return get_neighbor_at_level(lvl, failing_stub);
        }
        private IPeersManagerStub callback_get_broadcast_neighbors()
        {
            print(@"$(name): Call to get_broadcast_neighbors().\n");
            return get_broadcast_neighbors();
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
            return holder.handler.produce_maps_below_level(holder.handler.maps_retrieved_below_level);
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
            holder.handler.give_participant_maps(maps);
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
}

//FAKE

namespace Netsukuku.PeerServices
{
    internal ITasklet tasklet;
}
