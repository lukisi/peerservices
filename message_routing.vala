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

    /* GetGateway: Determine if a certain g-node exists in the network.
     */
    internal delegate IPeersManagerStub? GetGateway
         (int level, int pos,
          CallerInfo? received_from=null,
          IPeersManagerStub? failed=null);

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
        public bool missing_optional_maps;
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
            missing_optional_maps = false;
        }
    }

    internal class MessageRouting : Object
    {
        private int levels;
        private ArrayList<int> pos;
        private ArrayList<int> gsizes;
        private GnodeExists gnode_exists;
        private GetGateway get_gateway;
        private GetNodesInMyGroup get_nodes_in_my_group;
        private GetNonParticipantGnodes get_non_participant_gnodes;
        private ExecService exec_service;
        private HashMap<int, WaitingAnswer> waiting_answer_map;

        /* Emits this signal when discovers that a certain g-node does not participate
         * to a certain optional service. The user SHOULD listen to this event and
         * update the participation maps.
         */
        public signal void gnode_is_not_participating(HCoord g, int p_id);

        public MessageRouting
        (Gee.List<int> pos,
         Gee.List<int> gsizes,
         owned GnodeExists gnode_exists,
         owned GetGateway get_gateway,
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
            this.get_gateway = (owned) get_gateway;
            this.get_nodes_in_my_group = (owned) get_nodes_in_my_group;
            this.get_non_participant_gnodes = (owned) get_non_participant_gnodes;
            this.exec_service = (owned) exec_service;
            waiting_answer_map = new HashMap<int, WaitingAnswer>();
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
            bool redofromstart = true;
            while (redofromstart)
            {
                redofromstart = false;
                ArrayList<string> refuse_messages = new ArrayList<string>();
                respondant = null;
                int target_levels = x_macron.tuple.size;
                var exclude_gnode_list = new ArrayList<HCoord>();
                exclude_gnode_list.add_all(get_non_participant_gnodes(p_id, target_levels));
                if (exclude_myself)
                    exclude_gnode_list.add(new HCoord(0, pos[0]));
                PeerTupleGNodeContainer exclude_tuple_list;
                if (_exclude_tuple_list != null)
                    exclude_tuple_list = _exclude_tuple_list;
                else
                    exclude_tuple_list = new PeerTupleGNodeContainer(target_levels);
                assert(exclude_tuple_list.top == target_levels);
                foreach (PeerTupleGNode gn in exclude_tuple_list.list)
                {
                    int @case;
                    HCoord ret;
                    Utils.convert_tuple_gnode(pos, gn, out @case, out ret);
                    if (@case == 2)
                        exclude_gnode_list.add(ret);
                }
                PeerTupleGNodeContainer non_participant_tuple_list = new PeerTupleGNodeContainer(target_levels);
                IPeersResponse? response = null;
                while (true)
                {
                    HCoord? x = approximate(x_macron, exclude_gnode_list);
                    if (x == null)
                    {
                        if (! refuse_messages.is_empty)
                        {
                            string err_msg = "";
                            foreach (string msg in refuse_messages) err_msg += @"$(msg) - ";
                            throw new PeersDatabaseError.GENERIC(err_msg);
                        }
                        throw new PeersNoParticipantsInNetworkError.GENERIC("");
                    }
                    if (x.lvl == 0 && x.pos == pos[0])
                    {
                        try {
                            IPeersRequest copied_request = (IPeersRequest)dup_object(request);
                            IPeersResponse response_to_be_copied
                                = exec_service(p_id, copied_request, new ArrayList<int>());
                            response = (IPeersResponse)dup_object(response_to_be_copied);
                        } catch (PeersRedoFromStartError e) {
                            redofromstart = true;
                            break;
                        } catch (PeersRefuseExecutionError e) {
                            refuse_messages.add(e.message);
                            if (refuse_messages.size > 10)
                            {
                                refuse_messages.remove_at(0);
                                refuse_messages.remove_at(0);
                                refuse_messages.insert(0, "...");
                            }
                            exclude_gnode_list.add(new HCoord(0, pos[0]));
                            continue; // next iteration of cicle 1.
                        }
                        respondant = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), target_levels);
                        return response;
                    }
                    PeerMessageForwarder mf = new PeerMessageForwarder();
                    mf.inside_level = target_levels;
                    mf.n = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), x.lvl+1);
                    // That is n0·n1·...·nj, where j = x.lvl
                    if (x.lvl == 0)
                        mf.x_macron = null;
                    else
                        mf.x_macron = new PeerTupleNode(x_macron.tuple.slice(0, x.lvl));
                        // That is x̄0·x̄1·...·x̄j-1.
                    mf.lvl = x.lvl;
                    mf.pos = x.pos;
                    mf.p_id = p_id;
                    mf.msg_id = Random.int_range(0, int.MAX);
                    foreach (PeerTupleGNode t in exclude_tuple_list.list)
                    {
                        int @case;
                        HCoord ret;
                        Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                        if (@case == 3)
                        {
                            if (ret.equals(x))
                            {
                                int eps = t.top - t.tuple.size;
                                PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-eps), x.lvl);
                                mf.exclude_tuple_list.add(_t);
                            }
                        }
                    }
                    foreach (PeerTupleGNode t in non_participant_tuple_list.list)
                    {
                        if (Utils.visible_by_someone_inside_my_gnode(pos, t, x.lvl+1))
                            mf.non_participant_tuple_list.add(t);
                    }
                    int timeout_routing = find_timeout_routing(get_nodes_in_my_group(x.lvl+1));
                    WaitingAnswer waiting_answer =
                        new WaitingAnswer
                        (/*request    = */ request,
                         /*min_target = */ Utils.make_tuple_gnode(pos, x, x.lvl+1));
                    waiting_answer_map[mf.msg_id] = waiting_answer;
                    IPeersManagerStub? gwstub;
                    IPeersManagerStub? failed = null;
                    bool redo_approximate = false;
                    while (true)
                    {
                        gwstub = get_gateway(x.lvl, x.pos, null, failed);
                        if (gwstub == null) {
                            redo_approximate = true;
                            break;
                        }
                        try {
                            gwstub.forward_peer_message(mf);
                        } catch (StubError e) {
                            failed = gwstub;
                            continue;
                        } catch (DeserializeError e) {
                            assert_not_reached();
                        }
                        break;
                    }
                    if (redo_approximate)
                    {
                        waiting_answer_map.unset(mf.msg_id);
                        tasklet.ms_wait(20);
                        continue;
                    }
                    int timeout = timeout_routing;
                    while (true)
                    {
                        try {
                            waiting_answer.ch.recv_with_timeout(timeout);
                            if (waiting_answer.missing_optional_maps)
                            {
                                tasklet.ms_wait(20);
                                redofromstart = true;
                                respondant = null;
                                break;
                            }
                            if (waiting_answer.exclude_gnode != null)
                            {
                                PeerTupleGNode t =
                                    Utils.rebase_tuple_gnode
                                    (pos, waiting_answer.exclude_gnode, target_levels);
                                // t represents the same g-node of waiting_answer.exclude_gnode, but with top=target_levels
                                int @case;
                                HCoord ret;
                                Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                                if (@case == 2)
                                {
                                    exclude_gnode_list.add(ret);
                                }
                                exclude_tuple_list.add(t);
                                waiting_answer_map.unset(mf.msg_id);
                                break;
                            }
                            else if (waiting_answer.non_participant_gnode != null)
                            {
                                PeerTupleGNode t =
                                    Utils.rebase_tuple_gnode
                                    (pos, waiting_answer.non_participant_gnode, target_levels);
                                // t represents the same g-node of waiting_answer.non_participant_gnode, but with top=target_levels
                                int @case;
                                HCoord ret;
                                Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                                if (@case == 2)
                                {
                                    if (optional)
                                    {
                                        // signal to the outside
                                        gnode_is_not_participating(ret, p_id);
                                    }
                                    exclude_gnode_list.add(ret);
                                }
                                exclude_tuple_list.add(t);
                                non_participant_tuple_list.add(t);
                                waiting_answer_map.unset(mf.msg_id);
                                break;
                            }
                            else if (respondant == null && waiting_answer.respondant_node != null)
                            {
                                respondant =
                                    Utils.rebase_tuple_node
                                    (pos, waiting_answer.respondant_node, target_levels);
                                // respondant represents the same node of waiting_answer.respondant_node, but with top=target_levels
                                timeout = timeout_exec;
                            }
                            else if (waiting_answer.response != null)
                            {
                                response = waiting_answer.response;
                                waiting_answer_map.unset(mf.msg_id);
                                break;
                            }
                            else if (respondant != null && waiting_answer.refuse_message != null)
                            {
                                refuse_messages.add(waiting_answer.refuse_message);
                                if (refuse_messages.size > 10)
                                {
                                    refuse_messages.remove_at(0);
                                    refuse_messages.remove_at(0);
                                    refuse_messages.insert(0, "...");
                                }
                                PeerTupleGNode t =
                                    Utils.rebase_tuple_gnode
                                    (pos, Utils.tuple_node_to_tuple_gnode(respondant), target_levels);
                                // t represents the same node of respondant, but as GNode and with top=target_levels
                                int @case;
                                HCoord ret;
                                Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                                if (@case == 2)
                                {
                                    exclude_gnode_list.add(ret);
                                }
                                exclude_tuple_list.add(t);
                                respondant = null;
                                waiting_answer_map.unset(mf.msg_id);
                                break;
                            }
                            else if (respondant != null && waiting_answer.redo_from_start)
                            {
                                redofromstart = true;
                                respondant = null;
                                break;
                            }
                            else
                            {
                                // A new destination (min_target) is found, nothing to do.
                            }
                        } catch (ChannelError e) {
                            // TIMEOUT_EXPIRED
                            PeerTupleGNode t =
                                Utils.rebase_tuple_gnode
                                (pos, waiting_answer.min_target, target_levels);
                            // t represents the same g-node of waiting_answer.min_target, but with top=target_levels
                            int @case;
                            HCoord ret;
                            Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                            if (@case == 2)
                            {
                                exclude_gnode_list.add(ret);
                            }
                            exclude_tuple_list.add(t);
                            respondant = null;
                            waiting_answer_map.unset(mf.msg_id);
                            break;
                        }
                    }
                    if (redofromstart) break;
                    if (response != null)
                        break;
                }
                if (redofromstart) continue;
                return response;
            }
            assert_not_reached();
        }

        internal bool check_non_participation(HCoord g, int p_id)
        {
            // Decide if it's secure to state that `g` does not participate to service `p_id`.
            PeerTupleNode? x_macron = null;
            if (g.lvl > 0)
            {
                ArrayList<int> tuple = new ArrayList<int>();
                for (int i = 0; i < g.lvl; i++) tuple.add(0);
                x_macron = new PeerTupleNode(tuple);
            }
            PeerTupleNode n = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), g.lvl+1);
            PeerMessageForwarder mf = new PeerMessageForwarder();
            mf.inside_level = levels;
            mf.n = n;
            mf.x_macron = x_macron;
            mf.lvl = g.lvl;
            mf.pos = g.pos;
            mf.p_id = p_id;
            mf.msg_id = Random.int_range(0, int.MAX);
            int timeout_routing = find_timeout_routing(get_nodes_in_my_group(g.lvl+1));
            WaitingAnswer waiting_answer =
                new WaitingAnswer
                (/*request    = */ null,
                 /*min_target = */ Utils.make_tuple_gnode(pos, g, g.lvl+1));
            waiting_answer_map[mf.msg_id] = waiting_answer;
            IPeersManagerStub? gwstub;
            IPeersManagerStub? failed = null;
            while (true)
            {
                gwstub = get_gateway(g.lvl, g.pos, null, failed);
                if (gwstub == null) {
                    waiting_answer_map.unset(mf.msg_id);
                    return true;
                }
                try {
                    gwstub.forward_peer_message(mf);
                } catch (StubError e) {
                    failed = gwstub;
                    continue;
                } catch (DeserializeError e) {
                    assert_not_reached();
                }
                break;
            }
            try {
                waiting_answer.ch.recv_with_timeout(timeout_routing);
                if (waiting_answer.missing_optional_maps)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
                if (waiting_answer.exclude_gnode != null)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
                else if (waiting_answer.non_participant_gnode != null)
                {
                    int @case;
                    HCoord ret;
                    Utils.convert_tuple_gnode(pos, waiting_answer.non_participant_gnode, out @case, out ret);
                    if (@case == 2)
                    {
                        waiting_answer_map.unset(mf.msg_id);
                        return true;
                    }
                    else
                    {
                        waiting_answer_map.unset(mf.msg_id);
                        return false;
                    }
                }
                else if (waiting_answer.response != null)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
                else
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
            } catch (ChannelError e) {
                // TIMEOUT_EXPIRED
                waiting_answer_map.unset(mf.msg_id);
                return false;
            }
        }
    }
}