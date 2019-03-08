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
using Netsukuku.PeerServices.Utils;

namespace Netsukuku.PeerServices.MessageRouting
{
    /* GnodeExists: Determine if a certain g-node exists in the network.
     */
    internal delegate bool GnodeExists(int lvl, int pos);

    /* GetGateway: Get a stub to send a message in unicast (reliable, no wait) to
     * the best gateway towards a certain g-node. You can optionally specify that
     * a certain gateway must be avoided because it was the previous step (received_from).
     * You can optionally specify that a certain gateway must be first removed from the
     * list of available neighbors because it failed a previous message.
     */
    internal delegate IPeersManagerStub? GetGateway
         (int level, int pos,
          CallerInfo? received_from=null,
          IPeersManagerStub? failed=null);

    /* GetClientInternally: Get a stub to send a message through a TCP connection to
     * the client. This connection is with an IP that is internal to a certain g-node.
     */
    internal delegate IPeersManagerStub GetClientInternally(PeerTupleNode n);

    /* GetNodesInMyGroup: Determine the number of nodes in my g-node of a given level.
     */
    internal delegate int GetNodesInMyGroup(int lvl);

    /* MyGnodeParticipates: Determine if my g_node of a given level participates to a given service.
     */
    internal delegate bool MyGnodeParticipates(int p_id, int lvl);

    /* GetNonParticipantGnodes: Get list of non-participant gnodes to a given service.
     */
    internal delegate Gee.List<HCoord> GetNonParticipantGnodes(int p_id, int target_levels);

    /* ExecService: Execute a request on a service that the current node participates.
     * Requires that the node participates.
     */
    internal delegate IPeersResponse ExecService
         (int p_id, IPeersRequest req, Gee.List<int> client_tuple)
         throws PeersRefuseExecutionError, PeersRedoFromStartError;

    internal int extract_level_from_refuse_execution_message(string message)
    {
        // message is @"blah blah. level=$(ret)" where ret is an 'int'
        int i = message.index_of("level=");
        if (i == -1) return 0;
        int start = i + "level=".length;
        if (start == message.length) return 0;
        string nm = message.substring(start);
        int64 ret;
        if (int64.try_parse(nm, out ret)) return (int)ret;
        else return 0;
    }

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
        public int? e_lvl;
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
            e_lvl = null;
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
        private GetClientInternally get_client_internally;
        private GetNodesInMyGroup get_nodes_in_my_group;
        private MyGnodeParticipates my_gnode_participates;
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
         owned GetClientInternally get_client_internally,
         owned GetNodesInMyGroup get_nodes_in_my_group,
         owned MyGnodeParticipates my_gnode_participates,
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
            this.get_client_internally = (owned) get_client_internally;
            this.get_nodes_in_my_group = (owned) get_nodes_in_my_group;
            this.my_gnode_participates = (owned) my_gnode_participates;
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
            if (x_macron == null || x_macron.tuple.size == 0)
            {
                // It's me or nobody.
                // In this case, this node is certainly not a *virtual* node,
                // because a *virtual* node never is allowed to act as a client.
                HCoord x = new HCoord(0, pos[0]);
                if (!(x in exclude_list))
                    return x;
                else
                    return null;
            }
            // The search can be restricted to a certain g-node.
            int valid_levels = x_macron.tuple.size;
            assert (valid_levels <= levels);
            HCoord? ret = null;
            int min_distance = -1;
            // First, evaluate *dist* for each known g-node other than me.
            // This search already excludes *virtual* g-nodes.
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
            // Then evaluate *dist* for me.
            // Consider that a *virtual* node cannot answer a request. At least not a node
            //  that has *virtual* positions below level `valid_levels`.
            HCoord x = new HCoord(0, pos[0]);
            if (!(x in exclude_list) && i_am_real_up_to(valid_levels-1))
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

        private bool i_am_real_up_to(int lvl)
        {
            for (int i = 0; i <= lvl; i++)
                if (pos[i] >= gsizes[i]) return false;
            return true;
        }

        private bool i_am_real_down_to(int lvl)
        {
            for (int i = levels-1; i >= lvl; i--)
                if (pos[i] >= gsizes[i]) return false;
            return true;
        }

        private const int min_timeout = 500;
        private int find_timeout_routing(int nodes)
        {
            // number of msec to wait for a routing between a group of $(nodes) nodes.
            int ret = 200;
            if (nodes > 100) ret = 2000;
            if (nodes > 1000) ret = 20000;
            // TODO explore values
            // Consider a minimum threshold for the same reasons for which a reply
            //  (see remotable method forward_msg) could result in a StubError a few times: that
            //  is, due to some routing rule not been added yet, here or elsewhere in the path.
            return ret + min_timeout;
        }

        [NoReturn]
        private void client_not_main_id(IPeersRequest request)
        {
            debug(@"$(request.get_type().name()): contact_peer: no more main id.");
            tasklet.exit_tasklet();
        }

        /* Before calling this method the user MUST wait (if the service is optional)
         * that the participation maps are ready at the needed level.
         */
        public IPeersResponse contact_peer
        (int p_id,
         bool optional,
         PeerTupleNode x_macron,
         IPeersRequest request,
         int timeout_exec,
         int exclude_my_gnode,
         out PeerTupleNode? respondant,
         PeerTupleGNodeContainer? _exclude_tuple_list=null)
        throws PeersNoParticipantsInNetworkError, PeersDatabaseError
        {
            debug(@"$(request.get_type().name()): contact_peer: to $(x_macron): '$(json_string_object(request))'");
            int target_levels = x_macron.tuple.size;
            if (! i_am_real_up_to(target_levels-1)) error("A virtual node cannot be a client of p2p.");
            bool redofromstart = true;
            while (redofromstart)
            {
                redofromstart = false;
                if (!i_am_real_up_to(levels-1)) client_not_main_id(request);
                ArrayList<string> refuse_messages = new ArrayList<string>();
                respondant = null;
                var exclude_gnode_list = new ArrayList<HCoord>();
                exclude_gnode_list.add_all(get_non_participant_gnodes(p_id, target_levels));
                if (exclude_my_gnode >= 0)
                    exclude_gnode_list.add_all(get_all_gnodes_up_to_lvl(exclude_my_gnode));
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
                    if (!i_am_real_up_to(levels-1)) client_not_main_id(request);
                    HCoord? x = approximate(x_macron, exclude_gnode_list);
                    if (x == null)
                    {
                        if (! refuse_messages.is_empty)
                        {
                            string err_msg = "";
                            foreach (string msg in refuse_messages) err_msg += @"$(msg) - ";
                            debug(@"$(request.get_type().name()): contact_peer: Database error: $(err_msg).");
                            throw new PeersDatabaseError.GENERIC(err_msg);
                        }
                        debug(@"$(request.get_type().name()): contact_peer: no [more] participants.");
                        throw new PeersNoParticipantsInNetworkError.GENERIC("");
                    }
                    if (x.lvl == 0 && x.pos == pos[0])
                    {
                        debug(@"$(request.get_type().name()): contact_peer: respondant is myself.");
                        try {
                            IPeersRequest copied_request = (IPeersRequest)dup_object(request);
                            IPeersResponse response_to_be_copied
                                = exec_service(p_id, copied_request, new ArrayList<int>());
                            response = (IPeersResponse)dup_object(response_to_be_copied);
                            debug(@"$(request.get_type().name()): contact_peer: " +
                            @"got response from myself, is a $(response.get_type().name()).");
                        } catch (PeersRedoFromStartError e) {
                            debug(@"$(request.get_type().name()): contact_peer: got PeersRedoFromStartError from myself.");
                            redofromstart = true;
                            break;
                        } catch (PeersRefuseExecutionError e) {
                            debug(@"$(request.get_type().name()): contact_peer: got PeersRefuseExecutionError('$(e.message)') from myself.");
                            int e_lvl = extract_level_from_refuse_execution_message(e.message);
                            refuse_messages.add(e.message);
                            if (refuse_messages.size > 10)
                            {
                                refuse_messages.remove_at(0);
                                refuse_messages.remove_at(0);
                                refuse_messages.insert(0, "...");
                            }
                            exclude_gnode_list.add_all(get_all_gnodes_up_to_lvl(e_lvl));
                            continue; // next iteration of cicle 1.
                        }
                        if (target_levels == 0) respondant = new PeerTupleNode(new ArrayList<int>());
                        else respondant = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), target_levels);
                        return response;
                    }
                    debug(@"$(request.get_type().name()): contact_peer: destination is HCoord($(x.lvl),$(x.pos)).");
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
                    mf.msg_id = PRNGen.int_range(0, int.MAX);
                    foreach (PeerTupleGNode t in exclude_tuple_list.list)
                    {
                        int @case;
                        HCoord ret;
                        Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                        if (@case == 3)
                        {
                            if (ret.equals(x))
                            {
                                PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-t.level), x.lvl);
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
                    debug(@"$(request.get_type().name()): contact_peer: sent msg $(mf.msg_id): " +
                    @"'$(json_string_object(request))' with timeout_routing=$(timeout_routing).");
                    int timeout = timeout_routing;
                    while (true)
                    {
                        if (!i_am_real_up_to(levels-1)) client_not_main_id(request);
                        try {
                            waiting_answer.ch.recv_with_timeout(timeout);
                            if (waiting_answer.missing_optional_maps)
                            {
                                tasklet.ms_wait(20);
                                redofromstart = true;
                                respondant = null;
                                debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): missing_optional_maps. Will redo_from_start.");
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
                                debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): exclude_gnode. Exclude $(t).");
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
                                debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): non_participant_gnode. Exclude $(t).");
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
                                t = Utils.tuple_gnode_containing(pos, t, waiting_answer.e_lvl);
                                // t represents the g-node of level waiting_answer.e_lvl containing
                                // the node respondant, with top=target_levels
                                int @case;
                                HCoord ret;
                                Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                                if (@case == 2)
                                {
                                    // t is visible in my map and I don't belong to it. Its h-coord is ret.
                                    exclude_gnode_list.add(ret);
                                }
                                else if (@case == 1)
                                {
                                    // t is one of my g-nodes.
                                    exclude_gnode_list.add_all(get_all_gnodes_up_to_lvl(waiting_answer.e_lvl));
                                }
                                exclude_tuple_list.add(t);
                                respondant = null;
                                waiting_answer_map.unset(mf.msg_id);
                                debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): " +
                                @"refuse_message '$(waiting_answer.refuse_message)'. Exclude $(t).");
                                break;
                            }
                            else if (respondant != null && waiting_answer.redo_from_start)
                            {
                                redofromstart = true;
                                respondant = null;
                                debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): redo_from_start.");
                                break;
                            }
                            else
                            {
                                // A new destination (min_target) is found, nothing to do.
                            }
                        } catch (ChannelError e) {
                            if (!i_am_real_up_to(levels-1)) client_not_main_id(request);
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
                            debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): " +
                            @"ChannelError.$(e.code)('$(e.message)') for msg $(mf.msg_id). Exclude $(t).");
                            break;
                        }
                    }
                    if (redofromstart) break;
                    if (response != null) debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): " +
                                            @"got response from $(respondant)), is a $(response.get_type().name()).");
                    if (response != null)
                        break;
                    else debug(@"$(request.get_type().name()): contact_peer msg $(mf.msg_id): could not get response.");
                }
                if (redofromstart) continue;
                return response;
            }
            assert_not_reached();
        }

        public bool check_non_participation(HCoord g, int p_id)
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
            mf.msg_id = PRNGen.int_range(0, int.MAX);
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

        private delegate void SendOperation() throws StubError, DeserializeError;
        private void send_with_try_again(string doing, SendOperation op) throws StubError, DeserializeError
        {
            /*eg doing = @"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_missing_optional_maps to msg_id $(mf.msg_id)"*/
            bool once_more = true;
            int wait_next = 10;
            int wait_total = 0;
            while (once_more)
            {
                once_more = false;
                try {
                    op();
                } catch (StubError e) {
                    // This could be due to the routing rule not been added yet. So
                    //  let's wait a bit and try again a few times.
                    if (wait_total < min_timeout) {
                        debug(@"$(doing): failed getting back to originator. Will try again in $(wait_next) msec.");
                        tasklet.ms_wait(wait_next);
                        once_more = true;
                    } else {
                        debug(@"$(doing): permanently failed getting back to originator.");
                        throw e;
                    }
                }
            }
        }

        [NoReturn]
        private void server_not_main_id(PeerMessageForwarder mf)
        {
            if (! i_am_real_down_to(mf.n.top))
            {
                // No need to send the message redo_from_start, because the client is virtual too.
                tasklet.exit_tasklet();
            }
            PeerTupleNode tuple_respondant = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), mf.n.tuple.size);
            IPeersManagerStub nstub = get_client_internally(mf.n);
            try {
                send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_redo_from_start to msg_id $(mf.msg_id)",
                    () => {
                        nstub.set_redo_from_start(mf.msg_id, tuple_respondant);
                    });
            } catch (StubError e) {
                // Already logged. Do nothing more.
            } catch (DeserializeError e) {
                // Ignore.
            }
            tasklet.exit_tasklet();
        }

        /* After calling this method the user SHOULD use `mf.non_participant_tuple_list`
         * (if the service is optional) and update it's maps.
         */
        public void forward_msg
        (PeerMessageForwarder mf, bool optional, int maps_retrieved_below_level, CallerInfo caller)
        {
            if (! i_am_real_down_to(mf.n.top))
            {
                // No need to forward the message, because the client is virtual too.
                tasklet.exit_tasklet();
            }
            if (pos[mf.lvl] == mf.pos)
            {
                if (optional && (mf.x_macron != null && maps_retrieved_below_level < mf.x_macron.tuple.size))
                {
                    IPeersManagerStub nstub = get_client_internally(mf.n);
                    try {
                        send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_missing_optional_maps to msg_id $(mf.msg_id)",
                            () => {
                                nstub.set_missing_optional_maps(mf.msg_id);
                            });
                    } catch (StubError e) {
                        // Already logged. Do nothing more.
                    } catch (DeserializeError e) {
                        // Ignore.
                    }
                }
                else if (optional && (! my_gnode_participates(mf.p_id, mf.lvl)))
                {
                    IPeersManagerStub nstub = get_client_internally(mf.n);
                    PeerTupleGNode gn = Utils.make_tuple_gnode(pos, new HCoord(mf.lvl, mf.pos), mf.n.tuple.size);
                    try {
                        send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_non_participant to msg_id $(mf.msg_id)",
                            () => {
                                nstub.set_non_participant(mf.msg_id, gn);
                            });
                    } catch (StubError e) {
                        // Already logged. Do nothing more.
                    } catch (DeserializeError e) {
                        // Ignore.
                    }
                }
                else
                {
                    ArrayList<HCoord> exclude_gnode_list = new ArrayList<HCoord>();
                    exclude_gnode_list.add_all(get_non_participant_gnodes(mf.p_id, mf.inside_level));
                    foreach (PeerTupleGNode gn in mf.exclude_tuple_list)
                    {
                        int @case;
                        HCoord ret;
                        Utils.convert_tuple_gnode(pos, gn, out @case, out ret);
                        if (@case == 1)
                            exclude_gnode_list.add_all(get_all_gnodes_up_to_lvl(ret.lvl));
                        else if (@case == 2)
                            exclude_gnode_list.add(ret);
                    }
                    bool delivered = false;
                    while (! delivered)
                    {
                        HCoord? x = approximate(mf.x_macron, exclude_gnode_list);
                        if (x == null)
                        {
                            IPeersManagerStub nstub = get_client_internally(mf.n);
                            PeerTupleGNode gn = Utils.make_tuple_gnode(pos, new HCoord(mf.lvl, mf.pos), mf.n.tuple.size);
                            try {
                                send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_failure to msg_id $(mf.msg_id)",
                                    () => {
                                        nstub.set_failure(mf.msg_id, gn);
                                    });
                            } catch (StubError e) {
                                // Already logged. Do nothing more.
                            } catch (DeserializeError e) {
                                // Ignore.
                            }
                            break; // Not delivered, but aborted.
                        }
                        else if (x.lvl == 0 && x.pos == pos[0])
                        {
                            if (!i_am_real_up_to(levels-1)) server_not_main_id(mf);
                            IPeersManagerStub nstub = get_client_internally(mf.n);
                            PeerTupleNode tuple_respondant = Utils.make_tuple_node(pos, new HCoord(0, pos[0]), mf.n.tuple.size);
                            IPeersRequest request = null;
                            try {
                                send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending get_request to msg_id $(mf.msg_id)",
                                    () => {
                                        try {
                                            request = nstub.get_request(mf.msg_id, tuple_respondant);
                                        } catch (PeersUnknownMessageError e) {
                                            // Ignore.
                                        } catch (PeersInvalidRequest e) {
                                            // Ignore.
                                        }
                                    });
                            } catch (StubError e) {
                                // Already logged. Do nothing more.
                            } catch (DeserializeError e) {
                                // Ignore.
                            }
                            if (request != null)
                            {
                                if (!i_am_real_up_to(levels-1)) server_not_main_id(mf);
                                try {
                                    IPeersResponse resp = exec_service(mf.p_id, request, mf.n.tuple);
                                    if (!i_am_real_up_to(levels-1)) server_not_main_id(mf);
                                    // Refresh stub.
                                    nstub = get_client_internally(mf.n);
                                    try {
                                        send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_response to msg_id $(mf.msg_id)",
                                            () => {
                                                nstub.set_response(mf.msg_id, resp, tuple_respondant);
                                            });
                                    } catch (StubError e) {
                                        // Already logged. Do nothing more.
                                    } catch (DeserializeError e) {
                                        // Ignore.
                                    }
                                } catch (PeersRedoFromStartError e) {
                                    // Refresh stub.
                                    nstub = get_client_internally(mf.n);
                                    try {
                                        send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_redo_from_start to msg_id $(mf.msg_id)",
                                            () => {
                                                nstub.set_redo_from_start(mf.msg_id, tuple_respondant);
                                            });
                                    } catch (StubError e) {
                                        // Already logged. Do nothing more.
                                    } catch (DeserializeError e) {
                                        // Ignore.
                                    }
                                } catch (PeersRefuseExecutionError e) {
                                    int e_lvl = extract_level_from_refuse_execution_message(e.message);
                                    string err_message = "";
                                    if (e is PeersRefuseExecutionError.WRITE_OUT_OF_MEMORY)
                                            err_message = "WRITE_OUT_OF_MEMORY: ";
                                    if (e is PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE)
                                            err_message = "READ_NOT_FOUND_NOT_EXHAUSTIVE: ";
                                    if (e is PeersRefuseExecutionError.GENERIC)
                                            err_message = "GENERIC: ";
                                    err_message += e.message;
                                    // Refresh stub.
                                    nstub = get_client_internally(mf.n);
                                    try {
                                        send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_refuse_message to msg_id $(mf.msg_id)",
                                            () => {
                                                nstub.set_refuse_message(mf.msg_id, err_message, e_lvl, tuple_respondant);
                                            });
                                    } catch (StubError e) {
                                        // Already logged. Do nothing more.
                                    } catch (DeserializeError e) {
                                        // Ignore.
                                    }
                                }
                            }
                            break; // Not delivered, but executed.
                        }
                        else
                        {
                            PeerMessageForwarder mf2 = (PeerMessageForwarder)dup_object(mf);
                            mf2.lvl = x.lvl;
                            mf2.pos = x.pos;
                            if (x.lvl == 0)
                                mf2.x_macron = null;
                            else
                                mf2.x_macron = new PeerTupleNode(mf.x_macron.tuple.slice(0, x.lvl));
                            mf2.exclude_tuple_list.clear();
                            mf2.non_participant_tuple_list.clear();
                            foreach (PeerTupleGNode t in mf.exclude_tuple_list)
                            {
                                int @case;
                                HCoord ret;
                                Utils.convert_tuple_gnode(pos, t, out @case, out ret);
                                if (@case == 3)
                                {
                                    if (ret.equals(x))
                                    {
                                        PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-t.level), x.lvl);
                                        mf2.exclude_tuple_list.add(_t);
                                    }
                                }
                            }
                            foreach (PeerTupleGNode t in mf.non_participant_tuple_list)
                            {
                                if (Utils.visible_by_someone_inside_my_gnode(pos, t, x.lvl+1))
                                    mf2.non_participant_tuple_list.add(t);
                            }
                            IPeersManagerStub? gwstub;
                            IPeersManagerStub? failed = null;
                            while (true)
                            {
                                gwstub = get_gateway(mf2.lvl, mf2.pos, caller, failed);
                                if (gwstub == null) {
                                    tasklet.ms_wait(20);
                                    break;
                                }
                                try {
                                    gwstub.forward_peer_message(mf2);
                                } catch (StubError e) {
                                    failed = gwstub;
                                    continue;
                                } catch (DeserializeError e) {
                                    assert_not_reached();
                                }
                                delivered = true;
                                IPeersManagerStub nstub = get_client_internally(mf.n);
                                PeerTupleGNode gn = Utils.make_tuple_gnode(pos, x, mf.n.tuple.size);
                                try {
                                    send_with_try_again(@"PeerServices($(mf.p_id)) $(string_pos(pos)): sending set_next_destination to msg_id $(mf.msg_id)",
                                        () => {
                                            nstub.set_next_destination(mf.msg_id, gn);
                                        });
                                } catch (StubError e) {
                                    // Already logged. Do nothing more.
                                } catch (DeserializeError e) {
                                    // Ignore.
                                }
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                IPeersManagerStub? gwstub;
                IPeersManagerStub? failed = null;
                while (true)
                {
                    gwstub = get_gateway(mf.lvl, mf.pos, caller, failed);
                    if (gwstub == null) {
                        // give up routing
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
            }
        }

        private Gee.List<HCoord> get_all_gnodes_up_to_lvl(int lvl)
        {
            // Returns a list of HCoord representing each gnode visible in my topology which are inside
            //  my g-node at level lvl. Including single nodes, including myself as single node (0, pos[0]).
            ArrayList<HCoord> ret = new ArrayList<HCoord>();
            for (int l = 0; l < lvl; l++)
            {
                for (int p = 0; p < gsizes[l]; p++)
                {
                    if (pos[l] != p)
                        ret.add(new HCoord(l, p));
                }
            }
            ret.add(new HCoord(0, pos[0]));
            return ret;
        }

        public IPeersRequest get_request
        (int msg_id, PeerTupleNode respondant)
        throws PeersUnknownMessageError, PeersInvalidRequest
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.get_request: ignored because unknown msg_id");
                throw new PeersUnknownMessageError.GENERIC("unknown msg_id");
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must be inside my search g-node
            if (wa.min_target.top != respondant.top)
            {
                debug("PeersManager.get_request: ignored because not same g-node of research");
                throw new PeersInvalidRequest.GENERIC("not same g-node of research");
            }
            // ok
            wa.respondant_node = respondant;
            wa.ch.send_async(0);
            // might be a fake request
            if (wa.request == null) throw new PeersUnknownMessageError.GENERIC("was a fake request");
            else return wa.request;
        }

        public void set_response
        (int msg_id, IPeersResponse response, PeerTupleNode respondant)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_response: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            bool mismatch = false;
            if (wa.respondant_node == null) mismatch = true;
            else if (wa.respondant_node.tuple.size != respondant.tuple.size) mismatch = true;
            else
            {
                for (int j = 0; j < respondant.tuple.size; j++)
                {
                    if (respondant.tuple[j] != wa.respondant_node.tuple[j])
                    {
                        mismatch = true;
                        break;
                    }
                }
            }
            if (mismatch)
            {
                debug("PeersManager.set_response: ignored because did not send request to that node");
                return;
            }
            wa.response = response;
            wa.ch.send_async(0);
        }

        public void set_refuse_message
        (int msg_id, string refuse_message, int e_lvl, PeerTupleNode respondant)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_refuse_message: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            bool mismatch = false;
            if (wa.respondant_node == null) mismatch = true;
            else if (wa.respondant_node.tuple.size != respondant.tuple.size) mismatch = true;
            else
            {
                for (int j = 0; j < respondant.tuple.size; j++)
                {
                    if (respondant.tuple[j] != wa.respondant_node.tuple[j])
                    {
                        mismatch = true;
                        break;
                    }
                }
            }
            if (mismatch)
            {
                debug("PeersManager.set_refuse_message: ignored because did not send request to that node");
                return;
            }
            wa.refuse_message = refuse_message;
            wa.e_lvl = e_lvl;
            debug(@"PeersManager.set_refuse_message: $(refuse_message)");
            wa.ch.send_async(0);
        }

        public void set_redo_from_start
        (int msg_id, PeerTupleNode respondant)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_redo_from_start: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            bool mismatch = false;
            if (wa.respondant_node == null) mismatch = true;
            else if (wa.respondant_node.tuple.size != respondant.tuple.size) mismatch = true;
            else
            {
                for (int j = 0; j < respondant.tuple.size; j++)
                {
                    if (respondant.tuple[j] != wa.respondant_node.tuple[j])
                    {
                        mismatch = true;
                        break;
                    }
                }
            }
            if (mismatch)
            {
                debug("PeersManager.set_redo_from_start: ignored because did not send request to that node");
                return;
            }
            wa.redo_from_start = true;
            wa.ch.send_async(0);
        }

        public void set_next_destination
        (int msg_id, PeerTupleGNode tuple)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_next_destination: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must maintain the smallest value k
            if (wa.min_target.top != tuple.top)
            {
                debug("PeersManager.set_next_destination: ignored because not same g-node of research");
                return;
            }
            int old_k = wa.min_target.top - wa.min_target.tuple.size;
            if (tuple.top - tuple.tuple.size >= old_k)
            {
                debug("PeersManager.set_next_destination: ignored because already reached a lower level");
                return;
            }
            wa.min_target = tuple;
            wa.ch.send_async(0);
        }

        public void set_failure
        (int msg_id, PeerTupleGNode tuple)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_failure: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must be lower than the smallest value k
            if (wa.min_target.top != tuple.top)
            {
                debug("PeersManager.set_failure: ignored because not same g-node of research");
                return;
            }
            int old_k = wa.min_target.top - wa.min_target.tuple.size;
            if (tuple.top - tuple.tuple.size > old_k)
            {
                debug("PeersManager.set_failure: ignored because already reached a lower level");
                return;
            }
            wa.exclude_gnode = tuple;
            wa.ch.send_async(0);
        }

        public void set_non_participant
        (int msg_id, PeerTupleGNode tuple)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_non_participant: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            // must be lower than the smallest value k
            if (wa.min_target.top != tuple.top)
            {
                debug("PeersManager.set_non_participant: ignored because not same g-node of research");
                return;
            }
            int old_k = wa.min_target.top - wa.min_target.tuple.size;
            if (tuple.top - tuple.tuple.size > old_k)
            {
                debug("PeersManager.set_non_participant: ignored because already reached a lower level");
                return;
            }
            wa.non_participant_gnode = tuple;
            wa.ch.send_async(0);
        }

        public void set_missing_optional_maps
        (int msg_id)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_missing_optional_maps: ignored because unknown msg_id");
                return;
            }
            WaitingAnswer wa = waiting_answer_map[msg_id];
            wa.missing_optional_maps = true;
            wa.ch.send_async(0);
        }
    }
}
