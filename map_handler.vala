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
using TaskletSystem;

namespace Netsukuku.PeerServices.MapHandler
{
    /* Remove all of my knowledge relative to a given level.
     */
    internal delegate void ClearMapsAtLevel(int lvl);
    /* Add to my knowledge that a given g-node participates to a given service.
     */
    internal delegate void AddParticipant(int p_id, HCoord h);
    /* Add to my knowledge that a given g-node does NOT participate to a given service.
     */
    internal delegate void RemoveParticipant(int p_id, HCoord h);
    /* Produce a [copy of the] set of the participation maps for all the services.
     */
    internal delegate PeerParticipantSet ProduceMapsCopy();
    /* Get a stub to talk to a neighbor which has a given level as maximum distinct g-node
     * with respect to our address. If we have such a neighbor; otherwise return null.
     */
    internal delegate IPeersManagerStub? GetNeighborAtLevel(int lvl, IPeersManagerStub? failing_stub);
    /* Get a stub to transmit in broadcast to all neighbors.
     */
    internal delegate IPeersManagerStub GetBroadcastNeighbors();

    internal class MapHandler : Object
    {
        private int levels;
        private ArrayList<int> pos;
        private int guest_gnode_level;
        private int host_gnode_level;
        private unowned ClearMapsAtLevel clear_maps_at_level;
        private unowned AddParticipant add_participant;
        private unowned RemoveParticipant remove_participant;
        private unowned ProduceMapsCopy produce_maps;
        private unowned GetNeighborAtLevel get_neighbor_at_level;
        private unowned GetBroadcastNeighbors get_groadcast_neighbors;
        public int maps_retrieved_below_level {get; private set;}
        private HashMap<int, ITaskletHandle> participation_tasklets;

        public MapHandler
        (ArrayList<int> pos,
         ClearMapsAtLevel clear_maps_at_level,
         AddParticipant add_participant,
         RemoveParticipant remove_participant,
         ProduceMapsCopy produce_maps,
         GetNeighborAtLevel get_neighbor_at_level,
         GetBroadcastNeighbors get_groadcast_neighbors)
        {
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.levels = pos.size;
            this.clear_maps_at_level = clear_maps_at_level;
            this.add_participant = add_participant;
            this.remove_participant = remove_participant;
            this.produce_maps = produce_maps;
            this.get_neighbor_at_level = get_neighbor_at_level;
            this.get_groadcast_neighbors = get_groadcast_neighbors;
            participation_tasklets = new HashMap<int, ITaskletHandle>();
        }

        /* Produce a [copy of the] set of the participation maps for all the services
         * up to a given level.
         */
        public PeerParticipantSet produce_maps_below_level(int below_level)
        {
            PeerParticipantSet ret = produce_maps();
            foreach (int p_id in ret.participant_set.keys)
            {
                PeerParticipantMap m = ret.participant_set[p_id];
                ArrayList<HCoord> to_del = new ArrayList<HCoord>();
                foreach (HCoord h in m.participant_list) if (h.lvl >= below_level) to_del.add(h);
                m.participant_list.remove_all(to_del);
            }
            ret.retrieved_below_level = below_level;
            return ret;
        }

        /* This is called (once) right after the constructor if this identity created a
         * new network.
         */
        public void create_net()
        {
            // set level of my knowledge
            maps_retrieved_below_level = levels;
        }

        /* This is called (once) right after the constructor if this identity has been
         * added for a migration or a entering in another network.
         */
        public void enter_net(MapHandler old_identity, int guest_gnode_level, int host_gnode_level)
        {
            this.guest_gnode_level = guest_gnode_level;
            this.host_gnode_level = host_gnode_level;
            // use old_identity to retrieve up to guest_gnode_level - 1
            PeerParticipantSet old_ps = old_identity.produce_maps_below_level(guest_gnode_level);
            foreach (int p_id in old_ps.participant_set.keys)
                foreach (HCoord h in old_ps.participant_set[p_id].participant_list)
                    add_participant(p_id, h);
            // set level of my knowledge
            maps_retrieved_below_level = guest_gnode_level;
            // spawn tasklet
            RetrieveParticipantSetTasklet ts = new RetrieveParticipantSetTasklet();
            ts.t = this;
            tasklet.spawn(ts);
        }

        private class RetrieveParticipantSetTasklet : Object, ITaskletSpawnable
        {
            public MapHandler t;
            public void * func()
            {
                t.retrieve_participant_set();
                return null;
            }
        }
        private void retrieve_participant_set()
        {
            IPeersManagerStub? n_stub = null;
            while (true) {
                n_stub = get_neighbor_at_level(host_gnode_level - 1, n_stub);
                if (n_stub == null)
                {
                    debug(@"retrieve_participant_set: no more neighbors at level $(host_gnode_level - 1)");
                    return;
                }
                debug(@"retrieve_participant_set: contacting a neighbor at level $(host_gnode_level - 1)");

                PeerParticipantSet ret_maps;
                try {
                    IPeerParticipantSet resp = n_stub.ask_participant_maps();
                    ret_maps = (PeerParticipantSet)resp;
                    // includes ret_maps.retrieved_below_level.
                } catch (StubError e) {
                    debug(@"retrieve_participant_set: Failed call to ask_participant_maps: StubError $(e.message)");
                    continue;
                } catch (DeserializeError e) {
                    debug(@"retrieve_participant_set: Failed call to ask_participant_maps: DeserializeError $(e.message)");
                    continue;
                }
                // copy for levels greater than maps_retrieved_below_level
                if (maps_retrieved_below_level < ret_maps.retrieved_below_level)
                {
                    copy_and_forward(ret_maps);
                }
                break;
            }
        }

        public void give_participant_maps(PeerParticipantSet maps)
        {
            if (maps.retrieved_below_level <= maps_retrieved_below_level) return; // Ignore this data.
            copy_and_forward(maps);
        }

        void copy_and_forward(PeerParticipantSet maps)
        {
            // Find level of maximum distinct gnode.
            int mdg_lvl = levels - 1;
            while (pos[mdg_lvl] == maps.my_pos[mdg_lvl]) mdg_lvl--;
            int mdg_pos = maps.my_pos[mdg_lvl];
            // Find services in mdg.
            ArrayList<int> mdg_services = new ArrayList<int>();
            foreach (int p_id in maps.participant_set.keys)
            {
                var the_list = maps.participant_set[p_id].participant_list;
                foreach (HCoord hc in the_list) if (hc.lvl < mdg_lvl)
                    if (! (p_id in mdg_services)) mdg_services.add(p_id);
            }
            // Group lower levels.
            foreach (int p_id in maps.participant_set.keys)
            {
                var the_list = maps.participant_set[p_id].participant_list;
                ArrayList<HCoord> to_del = new ArrayList<HCoord>((a,b) => a.equals(b));
                foreach (HCoord hc in the_list) if (hc.lvl < mdg_lvl)
                    to_del.add(hc);
                the_list.remove_all(to_del);
            }
            foreach (int mdg_service in mdg_services)
                maps.participant_set[mdg_service].participant_list.add(new HCoord(mdg_lvl, mdg_pos));
            // Now maps.participant_set conform to my own address.
            for (int lvl = maps_retrieved_below_level; lvl < maps.retrieved_below_level; lvl++)
            {
                foreach (int p_id in maps.participant_set.keys)
                {
                    PeerParticipantMap map = maps.participant_set[p_id];
                    foreach (HCoord hc in map.participant_list) if (hc.lvl == lvl)
                        add_participant(p_id, hc);
                }
            }
            maps_retrieved_below_level = maps.retrieved_below_level;
            // get a stub for broadcast, no wait.
            IPeersManagerStub broadcast_stub = get_groadcast_neighbors();
            try {
                broadcast_stub.give_participant_maps(produce_maps_below_level(maps_retrieved_below_level));
            } catch (StubError e) {
                assert_not_reached();
            } catch (DeserializeError e) {
                assert_not_reached();
            }
        }

        public void participate(int p_id)
        {
            if (participation_tasklets.has_key(p_id))
            {
                warning(@"MapHandler: partipate($(p_id)): called twice.");
                return;
            }
            // spawn tasklet
            ParticipateTasklet ts = new ParticipateTasklet();
            ts.t = this;
            ts.p_id = p_id;
            participation_tasklets[p_id] = tasklet.spawn(ts);
        }
        private class ParticipateTasklet : Object, ITaskletSpawnable
        {
            public MapHandler t;
            public int p_id;
            public void * func()
            {
                t.participate_tasklet(p_id);
                return null;
            }
        }
        private void participate_tasklet(int p_id)
        {
            while (true) tasklet.ms_wait(100); // TODO
        }

        public void dont_participate(int p_id)
        {
            if (! participation_tasklets.has_key(p_id))
            {
                warning(@"MapHandler: dont_participate($(p_id)): was not participating.");
                return;
            }
            // kill tasklet
            participation_tasklets[p_id].kill();
        }
    }
}