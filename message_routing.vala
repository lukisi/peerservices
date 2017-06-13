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
using Netsukuku.PeerServices;

namespace Netsukuku.PeerServices.MessageRouting
{
    /* GnodeExists: Determine if a certain g-node exists in the network.
     */
    internal delegate bool GnodeExists(int lvl, int pos);

    /* GetNodesInMyGroup: Determine the number of nodes in my g-node of a given level.
     */
    internal delegate int GetNodesInMyGroup(int lvl);

    /* GetNonParticipantGnodes: Get list of non-participant gnodes to a given service.
     */
    internal delegate Gee.List<HCoord> GetNonParticipantGnodes(int p_id, int target_levels);

    /* ExecService: Execute a request on a service that the current node participates.
     * Requires that the node participates.
     */
    internal delegate IPeersResponse ExecService
         (int p_id, IPeersRequest req, Gee.List<int> client_tuple)
         throws PeersRefuseExecutionError, PeersRedoFromStartError;

    internal class WaitingAnswer : Object
    {
        public IChannel ch;
        public IPeersRequest? request;
        public PeerTupleGNode min_target;
        public PeerTupleGNode? exclude_gnode;
        public PeerTupleGNode? non_participant_gnode;
        public PeerTupleNode? respondant_node;
        public IPeersResponse? response;
        public string? refuse_message;
        public bool redo_from_start;
        public WaitingAnswer(IPeersRequest? request, PeerTupleGNode min_target)
        {
            ch = tasklet.get_channel();
            this.request = request;
            this.min_target = min_target;
            exclude_gnode = null;
            non_participant_gnode = null;
            respondant_node = null;
            response = null;
            refuse_message = null;
            redo_from_start = false;
        }
    }

    internal class MessageRouting : Object
    {
        private int levels;
        private ArrayList<int> pos;
        private ArrayList<int> gsizes;
        private GnodeExists gnode_exists;
        private GetNodesInMyGroup get_nodes_in_my_group;
        private GetNonParticipantGnodes get_non_participant_gnodes;
        private ExecService exec_service;

        public MessageRouting
        (Gee.List<int> pos,
         Gee.List<int> gsizes,
         owned GnodeExists gnode_exists,
         owned GetNodesInMyGroup get_nodes_in_my_group,
         owned GetNonParticipantGnodes get_non_participant_gnodes,
         owned ExecService exec_service
         )
        {
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            assert(gsizes.size == pos.size);
            this.levels = pos.size;
            this.gnode_exists = (owned) gnode_exists;
            this.get_nodes_in_my_group = (owned) get_nodes_in_my_group;
            this.get_non_participant_gnodes = (owned) get_non_participant_gnodes;
            this.exec_service = (owned) exec_service;
        }

        public int dist(PeerTupleNode x_macron, PeerTupleNode x)
        {
            assert(x_macron.tuple.size == x.tuple.size);
            int distance = 0;
            for (int j = x.tuple.size-1; j >= 0; j--)
            {
                distance *= gsizes[j];
                distance += x.tuple[j] - x_macron.tuple[j];
                if (x_macron.tuple[j] > x.tuple[j])
                    distance += gsizes[j];
            }
            return distance;
        }

        internal HCoord? approximate(PeerTupleNode? x_macron,
                                     Gee.List<HCoord> _exclude_list)
        {
            // Make sure that exclude_list is searchable
            Gee.List<HCoord> exclude_list = new ArrayList<HCoord>((a, b) => a.equals(b));
            exclude_list.add_all(_exclude_list);
            // This function is x=Ht(x̄)
            if (x_macron == null)
            {
                // It's me or nobody
                HCoord x = new HCoord(0, pos[0]);
                if (!(x in exclude_list))
                    return x;
                else
                    return null;
            }
            // The search can be restricted to a certain g-node
            int valid_levels = x_macron.tuple.size;
            assert (valid_levels <= levels);
            HCoord? ret = null;
            int min_distance = -1;
            // for each known g-node other than me
            for (int l = 0; l < valid_levels; l++)
                for (int p = 0; p < gsizes[l]; p++)
                if (pos[l] != p)
                if (gnode_exists(l, p))
            {
                HCoord x = new HCoord(l, p);
                if (!(x in exclude_list))
                {
                    PeerTupleNode tuple_x = Utils.make_tuple_node(pos, x, valid_levels);
                    int distance = dist(x_macron, tuple_x);
                    if (min_distance == -1 || distance < min_distance)
                    {
                        ret = x;
                        min_distance = distance;
                    }
                }
            }
            // and then for me
            HCoord x = new HCoord(0, pos[0]);
            if (!(x in exclude_list))
            {
                PeerTupleNode tuple_x = Utils.make_tuple_node(pos, x, valid_levels);
                int distance = dist(x_macron, tuple_x);
                if (min_distance == -1 || distance < min_distance)
                {
                    ret = x;
                    min_distance = distance;
                }
            }
            // If null yet, then nobody participates.
            return ret;
        }

        private int find_timeout_routing(int nodes)
        {
            // number of msec to wait for a routing between a group of $(nodes) nodes.
            int ret = 2000;
            if (nodes > 100) ret = 20000;
            if (nodes > 1000) ret = 200000;
            // TODO explore values
            return ret;
        }

        internal IPeersResponse contact_peer
        (int p_id,
         bool optional,
         PeerTupleNode x_macron,
         IPeersRequest request,
         int timeout_exec,
         bool exclude_myself,
         out PeerTupleNode? respondant,
         PeerTupleGNodeContainer? _exclude_tuple_list=null)
        throws PeersNoParticipantsInNetworkError, PeersDatabaseError
        {
            error("not implemented yet");
        }
    }
}