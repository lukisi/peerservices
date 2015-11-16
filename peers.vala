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
using Netsukuku.ModRpc;
using zcd.ModRpc;
using LibPeersInternals;

namespace Netsukuku
{
    public errordomain PeersNonexistentDestinationError {
        GENERIC
    }

    public errordomain PeersNonexistentFellowError {
        GENERIC
    }

    public errordomain PeersNoParticipantsInNetworkError {
        GENERIC
    }

    public errordomain PeersRefuseExecutionError {
        WRITE_OUT_OF_MEMORY,
        READ_NOT_FOUND_NOT_EXHAUSTIVE,
        GENERIC
    }

    public errordomain PeersDatabaseError {
        GENERIC
    }

    public errordomain PeersRedoFromStartError {
        GENERIC
    }

    public interface IPeersMapPaths : Object
    {
        public abstract int i_peers_get_levels();
        public abstract int i_peers_get_gsize(int level);
        public abstract int i_peers_get_my_pos(int level);
        public abstract int i_peers_get_nodes_in_my_group(int level);
        public abstract bool i_peers_exists(int level, int pos);
        public abstract IPeersManagerStub i_peers_gateway
            (int level, int pos,
             zcd.ModRpc.CallerInfo? received_from=null,
             IPeersManagerStub? failed=null)
            throws PeersNonexistentDestinationError;
        public abstract IPeersManagerStub i_peers_fellow(int level)
            throws PeersNonexistentFellowError;
    }

    public interface IPeersBackStubFactory : Object
    {
        // positions[0] is pos[0] of the node to contact inside our gnode
        // of level positions.size
        public abstract IPeersManagerStub i_peers_get_tcp_inside
            (Gee.List<int> positions);
    }

    public interface IPeersNeighborsFactory : Object
    {
        public abstract IPeersManagerStub i_peers_get_broadcast(
                            IPeersMissingArcHandler missing_handler);
        public abstract IPeersManagerStub i_peers_get_tcp(
                            IPeersArc arc);
    }

    public interface IPeersMissingArcHandler : Object
    {
        public abstract void i_peers_missing(IPeersArc missing_arc);
    }

    public interface IPeersArc : Object
    {
    }

    public interface IPeersContinuation : Object
    {
    }

    internal class PeerTupleNode : Object, Json.Serializable, IPeerTupleNode
    {
        public Gee.List<int> tuple {get; set;}
        public int top {get {return tuple.size;}}
        public PeerTupleNode(Gee.List<int> tuple)
        {
            this.tuple = new ArrayList<int>();
            this.tuple.add_all(tuple);
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "top":
                // get-only dynamic property
                return false;
            case "tuple":
                try {
                    @value = deserialize_list_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "top":
                return serialize_int(0); // get-only dynamic property
            case "tuple":
                return serialize_list_int((Gee.List<int>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class PeerTupleGNode : Object, Json.Serializable, IPeerTupleGNode
    {
        public Gee.List<int> tuple {get; set;}
        public int top {get; set;}
        public PeerTupleGNode(Gee.List<int> tuple, int top)
        {
            this._tuple = new ArrayList<int>();
            this._tuple.add_all(tuple);
            this.top = top;
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "tuple":
                try {
                    @value = deserialize_list_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "top":
                try {
                    @value = deserialize_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "tuple":
                return serialize_list_int((Gee.List<int>)@value);
            case "top":
                return serialize_int((int)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class PeerTupleGNodeContainer : Object
    {
        private ArrayList<PeerTupleGNode> _list;
        public Gee.List<PeerTupleGNode> list {
            owned get {
                return _list.read_only_view;
            }
        }
        public int top {get; private set;}

        public PeerTupleGNodeContainer(int top)
        {
            this.top = top;
            _list = new ArrayList<PeerTupleGNode>();
        }

        private int get_rev_pos(PeerTupleGNode g, int j)
        {
            // g.tuple has positions for levels from ε >= 0 to g.top-1.
            //  e.g.:
            //     g.top = 5
            //       that is, the g-node h that we live inside is at level 5.
            //     g.tuple.size = 3
            //     g.tuple = [2,1,3]
            //       that is g is g-node 3.1.2 inside h
            //       epsilon is 2, position for 2 is 2, position for 3 is 1, position for 4 is 3
            // get position for top-1-j.
            return g.tuple[g.tuple.size-1 - j];
        }

        public void add(PeerTupleGNode g)
        {
            assert(g.top == top);
            // If g is already contained in the list, return.
            if (contains(g)) return;
            // Cycle the list:
            int i = 0;
            while (i < _list.size)
            {
                PeerTupleGNode e = _list[i];
                assert(e.top == g.top);
                if (e.tuple.size > g.tuple.size)
                {
                    // If a gnode which is inside g is in the list, remove it.
                    bool mismatch = false;
                    for (int j = 0; j < g.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(g,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        _list.remove_at(i);
                        i--;
                    }
                }
                i++;
            }
            // Then add g.
            _list.add(g);
        }

        private bool contains(PeerTupleGNode g)
        {
            // Cycle the list:
            foreach (PeerTupleGNode e in _list)
            {
                assert(e.top == g.top);
                if (e.tuple.size <= g.tuple.size)
                {
                    // If g is already in the list, return true.
                    bool mismatch = false;
                    for (int j = 0; j < e.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(g,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    internal class PeerMessageForwarder : Object, Json.Serializable, IPeerMessage
    {
        public PeerTupleNode n {get; set;}
        public PeerTupleNode? x_macron {get; set;}
        public int lvl {get; set;}
        public int pos {get; set;}
        public int p_id {get; set;}
        public int msg_id {get; set;}
        public Gee.List<PeerTupleGNode> exclude_tuple_list {get; set;}
        public Gee.List<PeerTupleGNode> non_participant_tuple_list {get; set;}

        public PeerMessageForwarder()
        {
            exclude_tuple_list = new ArrayList<PeerTupleGNode>();
            non_participant_tuple_list = new ArrayList<PeerTupleGNode>();
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "n":
                try {
                    @value = deserialize_peer_tuple_node(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "x-macron":
            case "x_macron":
                try {
                    @value = deserialize_nullable_peer_tuple_node(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "lvl":
            case "pos":
            case "p-id":
            case "p_id":
            case "msg-id":
            case "msg_id":
                try {
                    @value = deserialize_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "exclude-tuple-list":
            case "exclude_tuple_list":
                try {
                    @value = deserialize_list_peer_tuple_gnode(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "non_participant_tuple_list":
            case "non-participant-tuple-list":
                try {
                    @value = deserialize_list_peer_tuple_gnode(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "n":
                return serialize_peer_tuple_node((PeerTupleNode)@value);
            case "x-macron":
            case "x_macron":
                return serialize_nullable_peer_tuple_node((PeerTupleNode?)@value);
            case "lvl":
            case "pos":
            case "p-id":
            case "p_id":
            case "msg-id":
            case "msg_id":
                return serialize_int((int)@value);
            case "exclude-tuple-list":
            case "exclude_tuple_list":
                return serialize_list_peer_tuple_gnode((Gee.List<PeerTupleGNode>)@value);
            case "non_participant_tuple_list":
            case "non-participant-tuple-list":
                return serialize_list_peer_tuple_gnode((Gee.List<PeerTupleGNode>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class WaitingAnswer : Object
    {
        public INtkdChannel ch;
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

    internal class PeerParticipantMap : Object, Json.Serializable
    {
        public Gee.List<HCoord> participant_list {get; set;}

        public PeerParticipantMap()
        {
            participant_list = new ArrayList<HCoord>((a,b) => a.equals(b));
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "participant-list":
            case "participant_list":
                try {
                    @value = deserialize_list_hcoord(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "participant-list":
            case "participant_list":
                return serialize_list_hcoord((Gee.List<HCoord>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class PeerParticipantSet : Object, Json.Serializable, IPeerParticipantSet
    {
        public HashMap<int, PeerParticipantMap> participant_set {get; set;}

        public PeerParticipantSet()
        {
            participant_set = new HashMap<int, PeerParticipantMap>();
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "participant-set":
            case "participant_set":
                try {
                    @value = deserialize_map_int_peer_participant_map(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "participant_set":
            case "participant-set":
                return serialize_map_int_peer_participant_map((HashMap<int, PeerParticipantMap>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class RequestWaitThenSendRecord : Object, Json.Serializable, IPeersRequest
    {
        public Object k {get; set;}
        public RequestWaitThenSendRecord(Object k)
        {
            this.k = k;
        }

        public const int timeout_exec = 99999; //TODO

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "k":
                try {
                    @value = deserialize_object(typeof(Object), false, property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "k":
                return serialize_object((Object)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class RequestWaitThenSendRecordResponse : Object, Json.Serializable, IPeersResponse
    {
        public Object record {get; set;}
        public RequestWaitThenSendRecordResponse(Object record)
        {
            this.record = record;
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "record":
                try {
                    @value = deserialize_object(typeof(Object), false, property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "record":
                return serialize_object((Object)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class RequestWaitThenSendRecordNotFound : Object, IPeersResponse
    {
    }

    internal class RequestSendKeys : Object, IPeersRequest
    {
        public int max_count {get; set;}
        public RequestSendKeys(int max_count)
        {
            this.max_count = max_count;
        }
    }

    internal class RequestSendKeysResponse : Object, Json.Serializable, IPeersResponse
    {
        public Gee.List<Object> keys {get; set;}

        public RequestSendKeysResponse()
        {
            keys = new ArrayList<Object>();
        }

        public bool deserialize_property
        (string property_name,
         out GLib.Value @value,
         GLib.ParamSpec pspec,
         Json.Node property_node)
        {
            @value = 0;
            switch (property_name) {
            case "keys":
                try {
                    @value = deserialize_list_object(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            default:
                return false;
            }
            return true;
        }

        public unowned GLib.ParamSpec find_property
        (string name)
        {
            return get_class().find_property(name);
        }

        public Json.Node serialize_property
        (string property_name,
         GLib.Value @value,
         GLib.ParamSpec pspec)
        {
            switch (property_name) {
            case "keys":
                return serialize_list_object((Gee.List<Object>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }
    }

    internal class Timer : Object
    {
        private TimeVal start;
        private long msec_ttl;
        public Timer(long msec_ttl)
        {
            start = TimeVal();
            start.get_current_time();
            this.msec_ttl = msec_ttl;
        }

        private long get_lap()
        {
            TimeVal lap = TimeVal();
            lap.get_current_time();
            long sec = lap.tv_sec - start.tv_sec;
            long usec = lap.tv_usec - start.tv_usec;
            if (usec < 0)
            {
                usec += 1000000;
                sec--;
            }
            return sec*1000000 + usec;
        }

        public bool is_expired()
        {
            return get_lap() > msec_ttl*1000;
        }
    }

    public class DatabaseHandler : Object
    {
        internal DatabaseHandler()
        {
            // ...
        }
        internal int p_id;
        internal bool ready;
        internal HashMap<Object,INtkdChannel> retrieving_keys;
        // for TTL-based services
        internal HashMap<Object,Timer> not_exhaustive_keys;
        internal ArrayList<Object> not_found_keys;
        internal Timer timer_default_not_exhaustive;
        // for few-fixed-keys services
        internal ArrayList<Object> not_completed_keys;
    }

    public interface IDatabaseDescriptor : Object
    {
        public abstract bool is_valid_key(Object k);
        public abstract Gee.List<int> evaluate_hash_node(Object k);
        public abstract bool key_equal_data(Object k1, Object k2);
        public abstract uint key_hash_data(Object k);
        public abstract bool is_valid_record(Object k, Object rec);
        public abstract bool my_records_contains(Object k);
        public abstract Object get_record_for_key(Object k);
        public abstract void set_record_for_key(Object k, Object rec);

        public abstract Object get_key_from_request(IPeersRequest r);
        public abstract int get_timeout_exec(IPeersRequest r);
        public abstract bool is_insert_request(IPeersRequest r);
        public abstract bool is_read_only_request(IPeersRequest r);
        public abstract bool is_update_request(IPeersRequest r);
        public abstract bool is_replica_value_request(IPeersRequest r);
        public abstract bool is_replica_delete_request(IPeersRequest r);
        public abstract IPeersResponse prepare_response_not_found(IPeersRequest r);
        public abstract IPeersResponse prepare_response_not_free(IPeersRequest r, Object rec);
        public abstract IPeersResponse execute(IPeersRequest r) throws PeersRefuseExecutionError, PeersRedoFromStartError;

        public DatabaseHandler dh {get {return dh_getter();} set {dh_setter(value);}}
        public abstract unowned DatabaseHandler dh_getter();
        public abstract void dh_setter(DatabaseHandler x);
    }

    public interface ITemporalDatabaseDescriptor : Object, IDatabaseDescriptor
    {
        public int ttl_db_max_records {get {return ttl_db_max_records_getter();}}
        public abstract int ttl_db_max_records_getter();
        public abstract int ttl_db_my_records_size();
        public int ttl_db_max_keys {get {return ttl_db_max_keys_getter();}}
        public abstract int ttl_db_max_keys_getter();
        public int ttl_db_msec_ttl {get {return ttl_db_msec_ttl_getter();}}
        public abstract int ttl_db_msec_ttl_getter();
        public abstract Gee.List<Object> ttl_db_get_all_keys();
        public int ttl_db_timeout_exec_send_keys {get {return ttl_db_timeout_exec_send_keys_getter();}}
        public abstract int ttl_db_timeout_exec_send_keys_getter();
    }

    public interface IFixedKeysDatabaseDescriptor : Object, IDatabaseDescriptor
    {
        public abstract Gee.List<Object> get_full_key_domain();
        public abstract Object get_default_record_for_key(Object k);
    }

    internal INtkdTasklet tasklet;
    public class PeersManager : Object,
                                IPeersManagerSkeleton
    {
        public static void init(INtkdTasklet _tasklet)
        {
            // Register serializable types
            typeof(PeerTupleNode).class_peek();
            typeof(PeerTupleGNode).class_peek();
            typeof(PeerMessageForwarder).class_peek();
            typeof(PeerParticipantMap).class_peek();
            typeof(PeerParticipantSet).class_peek();
            tasklet = _tasklet;
        }

        private IPeersMapPaths map_paths;
        private int levels;
        private int[] gsizes;
        private int[] pos;
        private IPeersBackStubFactory back_stub_factory;
        private IPeersNeighborsFactory neighbors_factory;
        private HashMap<int, PeerService> services;
        private HashMap<int, WaitingAnswer> waiting_answer_map;
        private HashMap<int, PeerParticipantMap> participant_maps;
        private ArrayList<HCoord> recent_published_list;
        public int level_new_gnode {get; private set;}
        public bool participant_maps_retrieved {get; private set;}
        public bool participant_maps_failed {get; private set;}

        // The maps of participants are now ready.
        public signal void participant_maps_ready();
        // The retrieve of maps of participants failed.
        public signal void participant_maps_failure();

        public PeersManager
            (IPeersMapPaths map_paths,
             int level_new_gnode,
             IPeersBackStubFactory back_stub_factory,
             IPeersNeighborsFactory neighbors_factory)
        {
            this.map_paths = map_paths;
            levels = map_paths.i_peers_get_levels();
            gsizes = new int[levels];
            pos = new int[levels];
            for (int i = 0; i < levels; i++)
            {
                gsizes[i] = map_paths.i_peers_get_gsize(i);
                pos[i] = map_paths.i_peers_get_my_pos(i);
            }
            this.back_stub_factory = back_stub_factory;
            this.neighbors_factory = neighbors_factory;
            services = new HashMap<int, PeerService>();
            waiting_answer_map = new HashMap<int, WaitingAnswer>();
            participant_maps = new HashMap<int, PeerParticipantMap>();
            recent_published_list = new ArrayList<HCoord>((a, b) => a.equals(b));
            this.level_new_gnode = level_new_gnode;
            participant_maps_retrieved = true;
            participant_maps_failed = false;
            if (level_new_gnode < levels)
            {
                participant_maps_retrieved = false;
                RetrieveParticipantSetTasklet ts = new RetrieveParticipantSetTasklet();
                ts.t = this;
                ts.lvl = level_new_gnode + 1;
                tasklet.spawn(ts);
            }
        }

        private class RetrieveParticipantSetTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public int lvl;
            public void * func()
            {
                bool ret = t.retrieve_participant_set(lvl);
                if (ret)
                {
                    t.participant_maps_retrieved = true;
                    t.participant_maps_ready();
                }
                else
                {
                    t.participant_maps_failed = true;
                    t.participant_maps_failure();
                }
                return null;
            }
        }
        private bool retrieve_participant_set(int lvl)
        {
            IPeersManagerStub f_stub;
            try {
                f_stub = map_paths.i_peers_fellow(lvl);
            } catch (PeersNonexistentFellowError e) {
                debug(@"retrieve_participant_set: Failed to get because PeersNonexistentFellowError");
                return false;
            }
            IPeerParticipantSet ret;
            try {
                ret = f_stub.get_participant_set(lvl);
            } catch (PeersInvalidRequest e) {
                debug(@"retrieve_participant_set: Failed to get because PeersInvalidRequest $(e.message)");
                return false;
            } catch (zcd.ModRpc.StubError e) {
                debug(@"retrieve_participant_set: Failed to get because StubError $(e.message)");
                return false;
            } catch (zcd.ModRpc.DeserializeError e) {
                debug(@"retrieve_participant_set: Failed to get because DeserializeError $(e.message)");
                return false;
            }
            if (! (ret is PeerParticipantSet)) {
                debug("retrieve_participant_set: Failed to get because unknown class");
                return false;
            }
            PeerParticipantSet participant_set = (PeerParticipantSet)ret;
            if (! check_valid_participant_set(participant_set)) {
                debug("retrieve_participant_set: Failed to get because not valid data");
                return false;
            }
            // copy
            participant_maps = new HashMap<int, PeerParticipantMap>();
            foreach (int p_id in participant_set.participant_set.keys)
            {
                PeerParticipantMap my_map = new PeerParticipantMap();
                participant_maps[p_id] = my_map;
                PeerParticipantMap map = participant_set.participant_set[p_id];
                foreach (HCoord hc in map.participant_list)
                    my_map.participant_list.add(hc);
            }
            return true;
        }

        public void register(PeerService p)
        {
            if (services.has_key(p.p_id))
            {
                critical("PeersManager.register: Two services with same ID?");
                return;
            }
            services[p.p_id] = p;
            if (p.p_is_optional)
            {
                PublishMyParticipationTasklet ts = new PublishMyParticipationTasklet();
                ts.t = this;
                ts.p_id = p.p_id;
                tasklet.spawn(ts);
                if (!participant_maps.has_key(p.p_id))
                    participant_maps[p.p_id] = new PeerParticipantMap();
                // save my position
                PeerParticipantMap map = participant_maps[p.p_id];
                map.participant_list.add(new HCoord(0, pos[0]));
            }
        }

        /* Helpers */

        private bool check_valid_message(PeerMessageForwarder mf)
        {
            if (! check_valid_tuple_node(mf.n)) return false;
            if (mf.lvl < 0) return false;
            if (mf.lvl >= levels) return false;
            if (mf.pos < 0) return false;
            if (mf.pos >= gsizes[mf.lvl]) return false;
            if (mf.n.tuple.size <= mf.lvl) return false;
            if (mf.x_macron != null)
            {
                if (! check_valid_tuple_node(mf.x_macron)) return false;
                if (mf.x_macron.tuple.size != mf.lvl) return false;
            }
            if (! mf.exclude_tuple_list.is_empty)
            {
                foreach (PeerTupleGNode gn in mf.exclude_tuple_list)
                {
                    if (! check_valid_tuple_gnode(gn)) return false;
                    if (gn.top != mf.lvl) return false;
                }
            }
            if (! mf.non_participant_tuple_list.is_empty)
            {
                PeerTupleGNode gn;
                gn = mf.non_participant_tuple_list[0];
                if (! check_valid_tuple_gnode(gn)) return false;
                if (gn.top <= mf.lvl) return false;
                int first_top = gn.top;
                for (int i = 1; i < mf.non_participant_tuple_list.size; i++)
                {
                    gn = mf.non_participant_tuple_list[i];
                    if (! check_valid_tuple_gnode(gn)) return false;
                    if (gn.top != first_top) return false;
                }
            }
            return true;
        }

        private bool check_valid_tuple_node(PeerTupleNode n)
        {
            if (n.tuple.size == 0) return false;
            if (n.tuple.size > levels) return false;
            for (int i = 0; i < n.tuple.size; i++)
            {
                if (n.tuple[i] < 0) return false;
                if (n.tuple[i] >= gsizes[i]) return false;
            }
            return true;
        }

        private bool check_valid_tuple_gnode(PeerTupleGNode gn)
        {
            if (gn.tuple.size == 0) return false;
            if (gn.top > levels) return false;
            if (gn.tuple.size > gn.top) return false;
            for (int i = 0; i < gn.tuple.size; i++)
            {
                int eps = gn.top - gn.tuple.size;
                if (gn.tuple[i] < 0) return false;
                if (gn.tuple[i] >= gsizes[eps+i]) return false;
            }
            return true;
        }

        private bool check_valid_participant_map(PeerParticipantMap m)
        {
            foreach (HCoord h in m.participant_list)
            {
                if (h.lvl < 0) return false;
                if (h.lvl > levels) return false;
                if (h.pos < 0) return false;
                if (h.pos > gsizes[h.lvl]) return false;
            }
            return true;
        }

        private bool check_valid_participant_set(PeerParticipantSet p)
        {
            foreach (int pid in p.participant_set.keys)
            {
                if (! check_valid_participant_map(p.participant_set[pid])) return false;
            }
            return true;
        }

        private bool my_gnode_participates(int p_id, int lvl)
        {
            // Tell whether my g-node at level lvl participates to service p_id.
            if (services.has_key(p_id))
                return true;
            if (! participant_maps.has_key(p_id))
                return false;
            PeerParticipantMap map = participant_maps[p_id];
            foreach (HCoord g in map.participant_list)
            {
                if (g.lvl < lvl)
                    return true;
            }
            return false;
        }

        private Gee.List<HCoord> get_non_participant_gnodes(int p_id)
        {
            // Returns a list of HCoord representing each gnode visible in my topology which, to my
            //  knowledge, do not participate to service p_id
            ArrayList<HCoord> ret = new ArrayList<HCoord>();
            bool optional = false;
            if (services.has_key(p_id))
                optional = services[p_id].p_is_optional;
            else
                optional = true;
            if (optional)
            {
                PeerParticipantMap? map = null;
                if (participant_maps.has_key(p_id))
                    map = participant_maps[p_id];
                foreach (HCoord lp in get_all_gnodes_up_to_lvl(levels))
                {
                    if (map == null)
                        ret.add(lp);
                    else
                        if (! (lp in map.participant_list))
                            ret.add(lp);
                }
            }
            return ret;
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

        private void convert_tuple_gnode(PeerTupleGNode t, out int @case, out HCoord ret)
        {
            /*
            Given t which represents a g-node h of level ε which lives inside one of my g-nodes,
            where ε = t.top - t.tuple.size,
            this methods returns the following informations:

            * int @case
               * Is 1 iff t represents one of my g-nodes.
               * Is 2 iff t represents a g-node visible in my topology.
               * Is 3 iff t represents a g-node not visible in my topology.
            * HCoord ret
               * The g-node in my map which h resides in.
               * In case 1  ret.lvl = ε. Also, pos[ret.lvl] = ret.pos.
               * In case 2  ret.lvl = ε. Also, pos[ret.lvl] ≠ ret.pos.
               * In case 3  ret.lvl > ε.
            */
            int lvl = t.top;
            int i = t.tuple.size;
            assert(i > 0);
            assert(i <= lvl);
            while (true)
            {
                lvl--;
                i--;
                if (pos[lvl] != t.tuple[i])
                {
                    ret = new HCoord(lvl, t.tuple[i]);
                    if (i == 0)
                        @case = 2;
                    else
                        @case = 3;
                    break;
                }
                if (i == 0)
                {
                    ret = new HCoord(lvl, t.tuple[i]);
                    @case = 1;
                    break;
                }
            }
        }

        private PeerTupleGNode make_tuple_gnode(HCoord h, int top)
        {
            // Returns a PeerTupleGNode that represents h inside our g-node of level top. 
            assert(top > h.lvl);
            ArrayList<int> tuple = new ArrayList<int>();
            int i = top;
            while (true)
            {
                i--;
                if (i == h.lvl)
                {
                    tuple.insert(0, h.pos);
                    break;
                }
                else
                {
                    tuple.insert(0, pos[i]);
                }
            }
            return new PeerTupleGNode(tuple, top);
        }

        private PeerTupleNode make_tuple_node(HCoord h, int top)
        {
            // Returns a PeerTupleNode that represents h inside our g-node of level top. Actually h could be a g-node
            //  but the resulting PeerTupleNode is to be used in method 'dist'. Values of positions for indexes less than
            //  h.lvl are not important, they just have to be in ranges, so we set to 0. 
            assert(top > h.lvl);
            ArrayList<int> tuple = new ArrayList<int>();
            int i = top;
            while (i > 0)
            {
                i--;
                if (i > h.lvl)
                    tuple.insert(0, pos[i]);
                else if (i == h.lvl)
                    tuple.insert(0, h.pos);
                else
                    tuple.insert(0, 0);
            }
            return new PeerTupleNode(tuple);
        }

        private PeerTupleGNode tuple_node_to_tuple_gnode(PeerTupleNode t)
        {
            // Given t that represents a node, returns the same as
            //  a PeerTupleGNode with the same top.
            ArrayList<int> tuple0 = new ArrayList<int>();
            tuple0.add_all(t.tuple);
            return new PeerTupleGNode(tuple0, t.top);
        }

        private PeerTupleGNode rebase_tuple_gnode(PeerTupleGNode t, int new_top)
        {
            // Given t that represents a g-node inside my g-node at level t.top, returns
            //  a PeerTupleGNode with top=new_top that represents the same g-node.
            // Assert that t.top <= new_top.
            assert(t.top <= new_top);
            ArrayList<int> tuple = new ArrayList<int>();
            ArrayList<int> tuple0 = new ArrayList<int>();
            int i = new_top;
            while (true)
            {
                i--;
                if (i >= t.top)
                    tuple.insert(0, pos[i]);
                else
                {
                    tuple0.add_all(t.tuple);
                    tuple0.add_all(tuple);
                    break;
                }
            }
            return new PeerTupleGNode(tuple0, new_top);
        }

        private PeerTupleNode rebase_tuple_node(PeerTupleNode t, int new_top)
        {
            // Given t that represents a node inside my g-node at level t.top, returns
            //  a PeerTupleNode with top=new_top that represents the same node.
            // Assert that t.top <= new_top.
            assert(t.top <= new_top);
            ArrayList<int> tuple = new ArrayList<int>();
            ArrayList<int> tuple0 = new ArrayList<int>();
            int i = new_top;
            while (true)
            {
                i--;
                if (i >= t.top)
                    tuple.insert(0, pos[i]);
                else
                {
                    tuple0.add_all(t.tuple);
                    tuple0.add_all(tuple);
                    break;
                }
            }
            return new PeerTupleNode(tuple0);
        }

        private bool visible_by_someone_inside_my_gnode(PeerTupleGNode t, int lvl)
        {
            // Given a g-node decides if some node inside my g-node of level lvl has
            //  visibility of it.
            //  e.g. I am node 1.2.3.4.5.6
            //       if t represents 1.2.2.0
            //           could be tuple=0,2 top=4
            //           could be tuple=0,2,2 top=5
            //           could be tuple=0,2,2,1 top=6
            //       if lvl=6 answer is true
            //       if lvl=5 answer is true
            //       if lvl=4 answer is true
            //       if lvl=3 answer is false
            //  e.g. I am node 1.2.3.4.5.6
            //       if t represents 1.2.3.0
            //           could be tuple=0 top=3
            //           could be tuple=0,3 top=4
            //           could be tuple=0,3,2 top=5
            //           could be tuple=0,3,2,1 top=6
            //       for any lvl answer would be true
            int eps = t.top - t.tuple.size;
            int l = eps;
            if (l >= lvl-1)
                l = l + 1;
            else
                l = lvl;
            if (t.top <= l)
                return true;
            PeerTupleGNode h = new PeerTupleGNode(t.tuple.slice(l-eps, t.tuple.size), t.top);
            int @case;
            HCoord ret;
            convert_tuple_gnode(h, out @case, out ret);
            if (@case == 1)
                return true;
            return false;
        }

        /* Routing algorithm */

        private int dist(PeerTupleNode x_macron, PeerTupleNode x)
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
                if (map_paths.i_peers_exists(l, p))
            {
                HCoord x = new HCoord(l, p);
                if (!(x in exclude_list))
                {
                    PeerTupleNode tuple_x = make_tuple_node(x, valid_levels);
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
                PeerTupleNode tuple_x = make_tuple_node(x, valid_levels);
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
         PeerTupleNode x_macron,
         IPeersRequest request,
         int timeout_exec,
         bool exclude_myself,
         out PeerTupleNode? respondant,
         PeerTupleGNodeContainer? _exclude_tuple_list=null)
        throws PeersNoParticipantsInNetworkError, PeersDatabaseError
        {
            int call_id = Random.int_range(0, int.MAX);
            bool first_time = true;
            debug(@"contact_peer called call_id=$(call_id)\n");
            bool redofromstart = true;
            while (redofromstart)
            {
                if (first_time) first_time = false;
                else debug(@"contact_peer redo from start call_id=$(call_id)\n");
                redofromstart = false;
                ArrayList<string> refuse_messages = new ArrayList<string>();
                respondant = null;
                int target_levels = x_macron.tuple.size;
                var exclude_gnode_list = new ArrayList<HCoord>();
                bool optional = false;
                if (services.has_key(p_id))
                {
                    optional = services[p_id].p_is_optional;
                    if (! services[p_id].is_ready())
                        exclude_myself = true;
                }
                else
                    optional = true;
                exclude_gnode_list.add_all(get_non_participant_gnodes(p_id));
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
                    convert_tuple_gnode(gn, out @case, out ret);
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
                        debug(@"contact_peer throws an error call_id=$(call_id)\n");
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
                            response = services[p_id].exec(request, new ArrayList<int>());
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
                        respondant = make_tuple_node(new HCoord(0, pos[0]), 1);
                        debug(@"contact_peer returns a response call_id=$(call_id)\n");
                        return response;
                    }
                    PeerMessageForwarder mf = new PeerMessageForwarder();
                    mf.n = make_tuple_node(new HCoord(0, pos[0]), x.lvl+1);
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
                    debug(@"contact_peer generates msg_id=$(mf.msg_id),  call_id=$(call_id).\n");
                    foreach (PeerTupleGNode t in exclude_tuple_list.list)
                    {
                        debug(@"exclude_tuple_list contains $(debugging.tuple_gnode(t).s).\n");
                        int @case;
                        HCoord ret;
                        convert_tuple_gnode(t, out @case, out ret);
                        if (@case == 3)
                        {
                            if (ret.equals(x))
                            {
                                debug(@" it is inside ($(x.lvl), $(x.pos)).\n");
                                int eps = t.top - t.tuple.size;
                                PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-eps), x.lvl);
                                mf.exclude_tuple_list.add(_t);
                            }
                        }
                    }
                    foreach (PeerTupleGNode t in non_participant_tuple_list.list)
                    {
                        if (visible_by_someone_inside_my_gnode(t, x.lvl+1))
                            mf.non_participant_tuple_list.add(t);
                    }
                    int timeout_routing = find_timeout_routing(map_paths.i_peers_get_nodes_in_my_group(x.lvl+1));
                    WaitingAnswer waiting_answer = new WaitingAnswer(request, make_tuple_gnode(x, x.lvl+1));
                    waiting_answer_map[mf.msg_id] = waiting_answer;
                    IPeersManagerStub gwstub;
                    IPeersManagerStub? failed = null;
                    bool redo_approximate = false;
                    while (true)
                    {
                        try {
                            gwstub = map_paths.i_peers_gateway(x.lvl, x.pos, null, failed);
                        } catch (PeersNonexistentDestinationError e) {
                            redo_approximate = true;
                            break;
                        }
                        try {
                            gwstub.forward_peer_message(mf);
                        } catch (zcd.ModRpc.StubError e) {
                            failed = gwstub;
                            continue;
                        } catch (zcd.ModRpc.DeserializeError e) {
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
                            if (waiting_answer.exclude_gnode != null)
                            {
                                PeerTupleGNode t = rebase_tuple_gnode(waiting_answer.exclude_gnode, target_levels);
                                // t represents the same g-node of waiting_answer.exclude_gnode, but with top=target_levels
                                int @case;
                                HCoord ret;
                                convert_tuple_gnode(t, out @case, out ret);
                                if (@case == 2)
                                {
                                    exclude_gnode_list.add(ret);
                                }
                                debug(@"adding t=$(debugging.tuple_gnode(t).s) to exclude_tuple_list because of failure msg_id=$(mf.msg_id).\n");
                                exclude_tuple_list.add(t);
                                waiting_answer_map.unset(mf.msg_id);
                                break;
                            }
                            else if (waiting_answer.non_participant_gnode != null)
                            {
                                PeerTupleGNode t = rebase_tuple_gnode(waiting_answer.non_participant_gnode, target_levels);
                                // t represents the same g-node of waiting_answer.non_participant_gnode, but with top=target_levels
                                int @case;
                                HCoord ret;
                                convert_tuple_gnode(t, out @case, out ret);
                                if (@case == 2)
                                {
                                    if (optional)
                                    {
                                        if (participant_maps.has_key(p_id))
                                        {
                                            PeerParticipantMap map = participant_maps[p_id];
                                            map.participant_list.remove(ret);
                                        }
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
                                respondant = rebase_tuple_node(waiting_answer.respondant_node, target_levels);
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
                                PeerTupleGNode t = rebase_tuple_gnode(tuple_node_to_tuple_gnode(respondant), target_levels);
                                // t represents the same node of respondant, but as GNode and with top=target_levels
                                int @case;
                                HCoord ret;
                                convert_tuple_gnode(t, out @case, out ret);
                                if (@case == 2)
                                {
                                    exclude_gnode_list.add(ret);
                                }
                                debug(@"adding t=$(debugging.tuple_gnode(t).s) to exclude_tuple_list because of refuse msg_id=$(mf.msg_id).\n");
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
                        } catch (NtkdChannelError e) {
                            // TIMEOUT_EXPIRED
                            PeerTupleGNode t = rebase_tuple_gnode(waiting_answer.min_target, target_levels);
                            // t represents the same g-node of waiting_answer.min_target, but with top=target_levels
                            int @case;
                            HCoord ret;
                            convert_tuple_gnode(t, out @case, out ret);
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
                if (redofromstart)
                {
                    debug(@"got a redo_from_start,  call_id=$(call_id).\n");
                    continue;
                }
                debug(@"contact_peer returns a response call_id=$(call_id)\n");
                return response;
            }
            assert_not_reached();
        }

        /* Participation publish algorithms */

        private bool check_non_participation(int p_id, int lvl, int _pos)
        {
            // Decide if it's secure to state that lvl,_pos does not participate to service p_id.
            PeerTupleNode? x_macron = null;
            if (lvl > 0)
            {
                ArrayList<int> tuple = new ArrayList<int>();
                for (int i = 0; i < lvl; i++) tuple.add(0);
                x_macron = new PeerTupleNode(tuple);
            }
            PeerTupleNode n = make_tuple_node(new HCoord(0, pos[0]), lvl+1);
            PeerMessageForwarder mf = new PeerMessageForwarder();
            mf.n = n;
            mf.x_macron = x_macron;
            mf.lvl = lvl;
            mf.pos = _pos;
            mf.p_id = p_id;
            mf.msg_id = Random.int_range(0, int.MAX);
            debug(@"check_non_participation generates msg_id=$(mf.msg_id).\n");
            int timeout_routing = find_timeout_routing(map_paths.i_peers_get_nodes_in_my_group(lvl+1));
            WaitingAnswer waiting_answer = new WaitingAnswer(null, make_tuple_gnode(new HCoord(lvl, _pos), lvl+1));
            IPeersManagerStub gwstub;
            IPeersManagerStub? failed = null;
            while (true)
            {
                try {
                    gwstub = map_paths.i_peers_gateway(lvl, _pos, null, failed);
                } catch (PeersNonexistentDestinationError e) {
                    waiting_answer_map.unset(mf.msg_id);
                    return true;
                }
                try {
                    gwstub.forward_peer_message(mf);
                } catch (zcd.ModRpc.StubError e) {
                    failed = gwstub;
                    continue;
                } catch (zcd.ModRpc.DeserializeError e) {
                    assert_not_reached();
                }
                break;
            }
            try {
                waiting_answer.ch.recv_with_timeout(timeout_routing);
                if (waiting_answer.exclude_gnode != null)
                {
                    waiting_answer_map.unset(mf.msg_id);
                    return false;
                }
                else if (waiting_answer.non_participant_gnode != null)
                {
                    int @case;
                    HCoord ret;
                    convert_tuple_gnode(waiting_answer.non_participant_gnode, out @case, out ret);
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
            } catch (NtkdChannelError e) {
                // TIMEOUT_EXPIRED
                waiting_answer_map.unset(mf.msg_id);
                return false;
            }
        }

        class MissingArcSetParticipant : Object, IPeersMissingArcHandler
        {
            public MissingArcSetParticipant(PeersManager mgr, int p_id, PeerTupleGNode tuple)
            {
                this.mgr = mgr;
                this.p_id = p_id;
                this.tuple = tuple;
            }
            private PeersManager mgr;
            private int p_id;
            private PeerTupleGNode tuple;
            public void i_peers_missing(IPeersArc missing_arc)
            {
                IPeersManagerStub stub =
                    mgr.neighbors_factory.i_peers_get_tcp(missing_arc);
                try {
                    stub.set_participant(p_id, tuple);
                } catch (zcd.ModRpc.StubError e) {
                    // ignore
                } catch (zcd.ModRpc.DeserializeError e) {
                    // ignore
                }
            }
        }

        private class PublishMyParticipationTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public int p_id;
            public void * func()
            {
                t.publish_my_participation(p_id);
                return null;
            }
        }
        private void publish_my_participation(int p_id)
        {
            PeerTupleGNode gn = make_tuple_gnode(new HCoord(0, pos[0]), levels);
            int timeout = 300000; // 5 min
            int iterations = 5;
            while (true)
            {
                if (iterations > 0) iterations--;
                else timeout = Random.int_range(24*60*60*1000, 2*24*60*60*1000); // 1 day to 2 days
                MissingArcSetParticipant missing_handler = new MissingArcSetParticipant(this, p_id, gn);
                IPeersManagerStub br_stub = neighbors_factory.i_peers_get_broadcast(missing_handler);
                try {
                    br_stub.set_participant(p_id, gn);
                } catch (zcd.ModRpc.StubError e) {
                    // ignore
                } catch (zcd.ModRpc.DeserializeError e) {
                    // ignore
                }
                tasklet.ms_wait(timeout);
            }
        }

        /* Algorithms to maintain a robust distributed database */

        private class ReplicaContinuation : Object, IPeersContinuation
        {
            public int q;
            public int p_id;
            public PeerTupleNode x_macron;
            public IPeersRequest r;
            public int timeout_exec;
            public ArrayList<PeerTupleNode> replicas;
            public PeerTupleGNodeContainer exclude_tuple_list;
        }

        public bool begin_replica
                (int q,
                 int p_id,
                 Gee.List<int> perfect_tuple,
                 IPeersRequest r,
                 int timeout_exec,
                 out IPeersResponse? resp,
                 out IPeersContinuation cont)
        {
            ReplicaContinuation _cont = new ReplicaContinuation();
            _cont.q = q;
            _cont.p_id = p_id;
            _cont.x_macron = new PeerTupleNode(perfect_tuple);
            _cont.r = r;
            _cont.timeout_exec = timeout_exec;
            _cont.replicas = new ArrayList<PeerTupleNode>();
            _cont.exclude_tuple_list = new PeerTupleGNodeContainer(_cont.x_macron.tuple.size);
            cont = _cont;
            return next_replica(cont, out resp);
        }

        public bool next_replica(IPeersContinuation cont, out IPeersResponse? resp)
        {
            ReplicaContinuation _cont = (ReplicaContinuation)cont;
            resp = null;
            if (_cont.replicas.size >= _cont.q) return false;
            PeerTupleNode respondant;
            try {
                debug("starting contact_peer for a request of replica.\n");
                resp = contact_peer
                    (_cont.p_id, _cont.x_macron, _cont.r,
                     _cont.timeout_exec, true,
                     out respondant, _cont.exclude_tuple_list);
            } catch (PeersNoParticipantsInNetworkError e) {
                return false;
            } catch (PeersDatabaseError e) {
                return false;
            }
            _cont.replicas.add(respondant);
            PeerTupleGNode respondant_as_gnode = new PeerTupleGNode(respondant.tuple, respondant.tuple.size);
            _cont.exclude_tuple_list.add(respondant_as_gnode);
            return _cont.replicas.size < _cont.q; 
        }

        internal bool ttl_db_is_exhaustive(ITemporalDatabaseDescriptor tdd, Object k)
        {
            if (tdd.dh.not_exhaustive_keys.has_key(k))
            {
                assert(! (k in tdd.dh.not_found_keys));
                if (tdd.dh.not_exhaustive_keys[k].is_expired())
                    tdd.dh.not_exhaustive_keys.unset(k);
                else return false;
            }
            if (k in tdd.dh.not_found_keys)
            {
                assert(! (tdd.dh.not_exhaustive_keys.has_key(k)));
                return true;
            }
            if (tdd.dh.timer_default_not_exhaustive.is_expired())
                return true;
            return false;
        }

        internal bool ttl_db_is_out_of_memory(ITemporalDatabaseDescriptor tdd)
        {
            if (tdd.ttl_db_my_records_size() + tdd.dh.retrieving_keys.size >= tdd.ttl_db_max_records)
                return true;
            return false;
        }

        internal void ttl_db_add_not_exhaustive(ITemporalDatabaseDescriptor tdd, Object k)
        {
            int max_not_exhaustive_keys = tdd.ttl_db_max_keys / 2;
            assert(! (k in tdd.dh.not_found_keys));
            if (tdd.dh.not_exhaustive_keys.has_key(k))
            {
                tdd.dh.not_exhaustive_keys[k] = new Timer(tdd.ttl_db_msec_ttl);
                return;
            }
            if (tdd.dh.not_exhaustive_keys.size < max_not_exhaustive_keys)
            {
                tdd.dh.not_exhaustive_keys[k] = new Timer(tdd.ttl_db_msec_ttl);
                return;
            }
            tdd.dh.timer_default_not_exhaustive = new Timer(tdd.ttl_db_msec_ttl);
            tdd.dh.not_exhaustive_keys.clear();
        }

        internal void ttl_db_add_not_found(ITemporalDatabaseDescriptor tdd, Object k)
        {
            int max_not_found_keys = tdd.ttl_db_max_keys / 2;
            assert(! (tdd.dh.not_exhaustive_keys.has_key(k)));
            if (k in tdd.dh.not_found_keys)
                tdd.dh.not_found_keys.remove(k);
            if (tdd.dh.not_found_keys.size >= max_not_found_keys)
                tdd.dh.not_found_keys.remove_at(0);
            tdd.dh.not_found_keys.add(k);
        }

        internal void ttl_db_remove_not_exhaustive(ITemporalDatabaseDescriptor tdd, Object k)
        {
            if (tdd.dh.not_exhaustive_keys.has_key(k))
                tdd.dh.not_exhaustive_keys.unset(k);
        }

        internal void ttl_db_remove_not_found(ITemporalDatabaseDescriptor tdd, Object k)
        {
            if (k in tdd.dh.not_found_keys)
                tdd.dh.not_found_keys.remove(k);
        }

        public void ttl_db_on_startup(ITemporalDatabaseDescriptor tdd, int p_id)
        {
            assert(services.has_key(p_id));
            TtlDbOnStartupTasklet ts = new TtlDbOnStartupTasklet();
            ts.t = this;
            ts.tdd = tdd;
            ts.p_id = p_id;
            tasklet.spawn(ts);
        }
        private class TtlDbOnStartupTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public ITemporalDatabaseDescriptor tdd;
            public int p_id;
            public void * func()
            {
                t.tasklet_ttl_db_on_startup(tdd, p_id); 
                return null;
            }
        }
        private void tasklet_ttl_db_on_startup(ITemporalDatabaseDescriptor tdd, int p_id)
        {
            debug("starting tasklet_ttl_db_on_startup.\n");
            PeerService srv = services[p_id];
            tdd.dh = new DatabaseHandler();
            tdd.dh.p_id = p_id;
            tdd.dh.ready = false;
            if (srv.p_is_optional)
            {
                while (! participant_maps_retrieved)
                {
                    if (participant_maps_failed) return;
                    tasklet.ms_wait(1000);
                }
            }
            tdd.dh.timer_default_not_exhaustive = new Timer(tdd.ttl_db_msec_ttl);
            tdd.dh.not_found_keys = new ArrayList<Object>(tdd.key_equal_data);
            tdd.dh.not_exhaustive_keys = new HashMap<Object, Timer>(tdd.key_hash_data, tdd.key_equal_data);
            tdd.dh.retrieving_keys = new HashMap<Object, INtkdChannel>(tdd.key_hash_data, tdd.key_equal_data);
            tdd.dh.ready = true;
            IPeersRequest r = new RequestSendKeys(tdd.ttl_db_max_records);
            try
            {
                PeerTupleNode tuple_n = make_tuple_node(new HCoord(0, pos[0]), levels);
                PeerTupleNode respondant;
                debug("starting contact_peer for a request of send_keys.\n");
                IPeersResponse _ret = contact_peer(p_id, tuple_n, r, tdd.ttl_db_timeout_exec_send_keys, true, out respondant);
                debug("returned from contact_peer for a request of send_keys.\n");
                if (_ret is RequestSendKeysResponse)
                {
                    RequestSendKeysResponse ret = (RequestSendKeysResponse)_ret;
                    debug(@"it is a valid response with $(ret.keys.size) keys.\n");
                    foreach (Object k in ret.keys)
                    {
                        if (! ttl_db_is_out_of_memory(tdd))
                        {
                            if (tdd.is_valid_key(k))
                            {
                                if ((! tdd.my_records_contains(k)) &&
                                    (! ttl_db_is_exhaustive(tdd, k)) &&
                                    (! tdd.dh.retrieving_keys.has_key(k)))
                                {
                                    PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k));
                                    if (dist(h_p_k, tuple_n) < dist(h_p_k, respondant))
                                    {
                                        ttl_db_start_retrieve(tdd, k);
                                        tasklet.ms_wait(2000);
                                    }
                                }
                            }
                        }
                    }
                }
                int @case;
                HCoord n0;
                convert_tuple_gnode(tuple_node_to_tuple_gnode(respondant), out @case, out n0);
                int l_n0 = n0.lvl;
                int p_n0 = n0.pos;
                tuple_n = make_tuple_node(new HCoord(0, pos[0]), l_n0+1);
                PeerTupleGNodeContainer exclude_tuple_list = new PeerTupleGNodeContainer(l_n0+1);
                for (int i = 0; i < gsizes[l_n0]; i++) if (i != p_n0)
                {
                    HCoord gn = new HCoord(l_n0, i);
                    PeerTupleGNode t = make_tuple_gnode(gn, l_n0+1);
                    exclude_tuple_list.add(t);
                }
                PeerTupleGNode t_respondant = new PeerTupleGNode(respondant.tuple.slice(0, l_n0+1), l_n0+1);
                exclude_tuple_list.add(t_respondant);
                while (! ttl_db_is_out_of_memory(tdd))
                {
                    tasklet.ms_wait(2000);
                    respondant = null;
                    debug("starting contact_peer for another request of send_keys.\n");
                    _ret = contact_peer(p_id, tuple_n, r, tdd.ttl_db_timeout_exec_send_keys, true, out respondant, exclude_tuple_list);
                    if (_ret is RequestSendKeysResponse)
                    {
                        RequestSendKeysResponse ret = (RequestSendKeysResponse)_ret;
                        foreach (Object k in ret.keys)
                        {
                            if (tdd.is_valid_key(k))
                            {
                                if ((! tdd.my_records_contains(k)) &&
                                    (! ttl_db_is_exhaustive(tdd, k)) &&
                                    (! tdd.dh.retrieving_keys.has_key(k)))
                                {
                                    PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k).slice(0, tuple_n.top));
                                    if (dist(h_p_k, tuple_n) < dist(h_p_k, respondant))
                                    {
                                        ttl_db_start_retrieve(tdd, k);
                                        if (ttl_db_is_out_of_memory(tdd)) break;
                                        tasklet.ms_wait(2000);
                                    }
                                }
                            }
                        }
                        if (ttl_db_is_out_of_memory(tdd)) break;
                    }
                    t_respondant = tuple_node_to_tuple_gnode(respondant);
                    exclude_tuple_list.add(t_respondant);
                }
            } catch (PeersNoParticipantsInNetworkError e) {
                debug("returned from contact_peer for a request of send_keys with a PeersNoParticipantsInNetworkError.\n");
                // Do nothing, terminates.
            } catch (PeersDatabaseError e) {
                debug("returned from contact_peer for a request of send_keys with a PeersDatabaseError.\n");
                // Do nothing, terminates.
            }
            debug("ending tasklet_ttl_db_on_startup.\n");
        }

        public IPeersResponse
        ttl_db_on_request(ITemporalDatabaseDescriptor tdd, IPeersRequest r, int common_lvl)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            if (tdd.dh == null || ! tdd.dh.ready)
                throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not even started");
            if (r is RequestSendKeys)
            {
                RequestSendKeys _r = (RequestSendKeys)r;
                RequestSendKeysResponse ret = new RequestSendKeysResponse();
                foreach (Object k in tdd.ttl_db_get_all_keys())
                {
                    ret.keys.add(k);
                    if (ret.keys.size >= _r.max_count) break;
                }
                return ret;
            }
            if (tdd.is_insert_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.prepare_response_not_free(r, tdd.get_record_for_key(k));
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    if (ttl_db_is_out_of_memory(tdd))
                    {
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        throw new PeersRefuseExecutionError.WRITE_OUT_OF_MEMORY("my node is out of memory");
                    }
                    else
                    {
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_remove_not_exhaustive(tdd, k);
                        IPeersResponse res = tdd.execute(r);
                        if (! tdd.my_records_contains(k))
                            ttl_db_add_not_found(tdd, k);
                        return res;
                    }
                }
                else
                {
                    if (tdd.dh.retrieving_keys.has_key(k))
                    {
                        INtkdChannel ch = tdd.dh.retrieving_keys[k];
                        try {
                            ch.recv_with_timeout(tdd.get_timeout_exec(r) - 1000);
                        } catch (NtkdChannelError e) {
                        }
                        throw new PeersRedoFromStartError.GENERIC("");
                    }
                    else
                    {
                        if (! ttl_db_is_out_of_memory(tdd))
                            ttl_db_start_retrieve(tdd, k);
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                    }
                }
            }
            if (tdd.is_read_only_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.execute(r);
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return tdd.prepare_response_not_found(r);
                }
                else
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                }
            }
            if (r is RequestWaitThenSendRecord)
            {
                Object k = ((RequestWaitThenSendRecord)r).k;
                if ((! tdd.my_records_contains(k)) && (! ttl_db_is_exhaustive(tdd, k)))
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                }
                int delta = eval_coherence_delta(map_paths.i_peers_get_nodes_in_my_group(common_lvl));
                if (delta > RequestWaitThenSendRecord.timeout_exec - 1000)
                    delta = RequestWaitThenSendRecord.timeout_exec - 1000;
                tasklet.ms_wait(delta);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return new RequestWaitThenSendRecordResponse(tdd.get_record_for_key(k));
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return new RequestWaitThenSendRecordNotFound();
                }
                else
                {
                    throw new PeersRedoFromStartError.GENERIC("");
                }
            }
            if (tdd.is_update_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    return tdd.execute(r);
                }
                if (ttl_db_is_exhaustive(tdd, k))
                {
                    ttl_db_add_not_found(tdd, k);
                    return tdd.prepare_response_not_found(r);
                }
                else
                {
                    if (tdd.dh.retrieving_keys.has_key(k))
                    {
                        INtkdChannel ch = tdd.dh.retrieving_keys[k];
                        try {
                            ch.recv_with_timeout(tdd.get_timeout_exec(r) - 1000);
                        } catch (NtkdChannelError e) {
                        }
                        throw new PeersRedoFromStartError.GENERIC("");
                    }
                    else
                    {
                        if (! ttl_db_is_out_of_memory(tdd))
                            ttl_db_start_retrieve(tdd, k);
                        ttl_db_remove_not_found(tdd, k);
                        ttl_db_add_not_exhaustive(tdd, k);
                        throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                    }
                }
            }
            if (tdd.is_replica_value_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                if (tdd.my_records_contains(k))
                {
                    assert(! tdd.dh.not_exhaustive_keys.has_key(k));
                    assert(! (k in tdd.dh.not_found_keys));
                    var res = tdd.execute(r);
                    assert(tdd.my_records_contains(k));
                    return res;
                }
                if (! ttl_db_is_out_of_memory(tdd))
                {
                    ttl_db_remove_not_found(tdd, k);
                    ttl_db_remove_not_exhaustive(tdd, k);
                    var res = tdd.execute(r);
                    assert(tdd.my_records_contains(k));
                    return res;
                }
                else
                {
                    ttl_db_remove_not_found(tdd, k);
                    ttl_db_add_not_exhaustive(tdd, k);
                    throw new PeersRefuseExecutionError.WRITE_OUT_OF_MEMORY("my node is out of memory");
                }
            }
            if (tdd.is_replica_delete_request(r))
            {
                Object k = tdd.get_key_from_request(r);
                var res = tdd.execute(r);
                assert(! tdd.my_records_contains(k));
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_add_not_found(tdd, k);
                return res;
            }
            // none of previous cases
            return tdd.execute(r);
        }
        private int eval_coherence_delta(int num_nodes)
        {
            if (num_nodes < 50) return 2000;
            if (num_nodes < 500) return 20000;
            if (num_nodes < 5000) return 30000;
            return 50000;
        }

        internal void ttl_db_start_retrieve(ITemporalDatabaseDescriptor tdd, Object k)
        {
            INtkdChannel ch = tasklet.get_channel();
            tdd.dh.retrieving_keys[k] = ch;
            TtlDbStartRetrieveTasklet ts = new TtlDbStartRetrieveTasklet();
            ts.t = this;
            ts.tdd = tdd;
            ts.k = k;
            ts.ch = ch;
            tasklet.spawn(ts);
        }
        private class TtlDbStartRetrieveTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public ITemporalDatabaseDescriptor tdd;
            public Object k;
            public INtkdChannel ch;
            public void * func()
            {
                t.tasklet_ttl_db_start_retrieve(tdd, k, ch); 
                return null;
            }
        }
        private void tasklet_ttl_db_start_retrieve(ITemporalDatabaseDescriptor tdd, Object k, INtkdChannel ch)
        {
            Object? record = null;
            IPeersRequest r = new RequestWaitThenSendRecord(k);
            try {
                PeerTupleNode respondant;
                PeerTupleNode h_p_k = new PeerTupleNode(tdd.evaluate_hash_node(k));
                debug(@"starting contact_peer for a request of wait_then_send_record (Key is a $(k.get_type().name())).\n");
                IPeersResponse res = contact_peer(tdd.dh.p_id, h_p_k, r, RequestWaitThenSendRecord.timeout_exec, true, out respondant);
                if (res is RequestWaitThenSendRecordResponse)
                {
                    debug(@"the request of wait_then_send_record returned a record.\n");
                    record = ((RequestWaitThenSendRecordResponse)res).record;
                }
            } catch (PeersNoParticipantsInNetworkError e) {
                debug(@"the request of wait_then_send_record threw a PeersNoParticipantsInNetworkError.\n");
                // nop
            } catch (PeersDatabaseError e) {
                debug(@"the request of wait_then_send_record threw a PeersDatabaseError.\n");
                // nop
            }
            if (record != null && tdd.is_valid_record(k, record))
            {
                debug(@"putting the record in my memory.\n");
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_remove_not_found(tdd, k);
                tdd.set_record_for_key(k, record);
            }
            else
            {
                debug(@"the request of wait_then_send_record returned a not_found.\n");
                ttl_db_remove_not_exhaustive(tdd, k);
                ttl_db_add_not_found(tdd, k);
            }
            INtkdChannel temp_ch = tdd.dh.retrieving_keys[k];
            tdd.dh.retrieving_keys.unset(k);
            while (temp_ch.get_balance() < 0) temp_ch.send_async(0);
        }

        public void fixed_keys_db_on_startup(IFixedKeysDatabaseDescriptor fkdd, int p_id)
        {
            assert(services.has_key(p_id));
            FixedKeysDbOnStartupTasklet ts = new FixedKeysDbOnStartupTasklet();
            ts.t = this;
            ts.fkdd = fkdd;
            ts.p_id = p_id;
            tasklet.spawn(ts);
        }
        private class FixedKeysDbOnStartupTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public IFixedKeysDatabaseDescriptor fkdd;
            public int p_id;
            public void * func()
            {
                t.tasklet_fixed_keys_db_on_startup(fkdd, p_id); 
                return null;
            }
        }
        private void tasklet_fixed_keys_db_on_startup(IFixedKeysDatabaseDescriptor fkdd, int p_id)
        {
            PeerService srv = services[p_id];
            fkdd.dh = new DatabaseHandler();
            fkdd.dh.p_id = p_id;
            fkdd.dh.ready = false;
            if (srv.p_is_optional)
            {
                while (! participant_maps_retrieved)
                {
                    if (participant_maps_failed) return;
                    tasklet.ms_wait(1000);
                }
            }
            fkdd.dh.not_completed_keys = new ArrayList<Object>(fkdd.key_equal_data);
            fkdd.dh.retrieving_keys = new HashMap<Object, INtkdChannel>(fkdd.key_hash_data, fkdd.key_equal_data);
            Gee.List<Object> k_set = fkdd.get_full_key_domain();
            fkdd.dh.not_completed_keys.add_all(k_set);
            fkdd.dh.ready = true;
            foreach (Object k in k_set)
            {
                fixed_keys_db_start_retrieve(fkdd, k);
                tasklet.ms_wait(2000);
            }
        }

        public IPeersResponse
        fixed_keys_db_on_request(IFixedKeysDatabaseDescriptor fkdd, IPeersRequest r, int common_lvl)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            if (fkdd.dh == null || ! fkdd.dh.ready)
                throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not even started");
            if (fkdd.is_read_only_request(r))
            {
                Object k = fkdd.get_key_from_request(r);
                if (! (k in fkdd.dh.not_completed_keys))
                {
                    assert(fkdd.my_records_contains(k));
                    return fkdd.execute(r);
                }
                else
                {
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                }
            }
            if (r is RequestWaitThenSendRecord)
            {
                Object k = ((RequestWaitThenSendRecord)r).k;
                if (k in fkdd.dh.not_completed_keys)
                    throw new PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE("not exhaustive");
                assert(fkdd.my_records_contains(k));
                int delta = eval_coherence_delta(map_paths.i_peers_get_nodes_in_my_group(common_lvl));
                if (delta > RequestWaitThenSendRecord.timeout_exec - 1000)
                    delta = RequestWaitThenSendRecord.timeout_exec - 1000;
                tasklet.ms_wait(delta);
                assert(fkdd.my_records_contains(k));
                return new RequestWaitThenSendRecordResponse(fkdd.get_record_for_key(k));
            }
            if (fkdd.is_update_request(r))
            {
                Object k = fkdd.get_key_from_request(r);
                if (! (k in fkdd.dh.not_completed_keys))
                {
                    assert(fkdd.my_records_contains(k));
                    return fkdd.execute(r);
                }
                else
                {
                    assert(fkdd.dh.retrieving_keys.has_key(k));
                    INtkdChannel ch = fkdd.dh.retrieving_keys[k];
                    try {
                        ch.recv_with_timeout(fkdd.get_timeout_exec(r) - 1000);
                    } catch (NtkdChannelError e) {
                    }
                    throw new PeersRedoFromStartError.GENERIC("");
                }
            }
            if (fkdd.is_replica_value_request(r))
            {
                return fkdd.execute(r);
            }
            // none of previous cases
            return fkdd.execute(r);
        }

        internal void fixed_keys_db_start_retrieve(IFixedKeysDatabaseDescriptor fkdd, Object k)
        {
            INtkdChannel ch = tasklet.get_channel();
            fkdd.dh.retrieving_keys[k] = ch;
            FixedKeysDbStartRetrieveTasklet ts = new FixedKeysDbStartRetrieveTasklet();
            ts.t = this;
            ts.fkdd = fkdd;
            ts.k = k;
            ts.ch = ch;
            tasklet.spawn(ts);
        }
        private class FixedKeysDbStartRetrieveTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public IFixedKeysDatabaseDescriptor fkdd;
            public Object k;
            public INtkdChannel ch;
            public void * func()
            {
                t.tasklet_fixed_keys_db_start_retrieve(fkdd, k, ch); 
                return null;
            }
        }
        private void tasklet_fixed_keys_db_start_retrieve(IFixedKeysDatabaseDescriptor fkdd, Object k, INtkdChannel ch)
        {
            Object? record = null;
            IPeersRequest r = new RequestWaitThenSendRecord(k);
            try {
                PeerTupleNode respondant;
                PeerTupleNode h_p_k = new PeerTupleNode(fkdd.evaluate_hash_node(k));
                debug(@"starting contact_peer for a request of wait_then_send_record (Key is a $(k.get_type().name())).\n");
                IPeersResponse res = contact_peer(fkdd.dh.p_id, h_p_k, r, RequestWaitThenSendRecord.timeout_exec, true, out respondant);
                if (res is RequestWaitThenSendRecordResponse)
                    record = ((RequestWaitThenSendRecordResponse)res).record;
            } catch (PeersNoParticipantsInNetworkError e) {
                // nop
            } catch (PeersDatabaseError e) {
                // nop
            }
            if (record != null && fkdd.is_valid_record(k, record))
            {
                fkdd.set_record_for_key(k, record);
            }
            else
            {
                fkdd.set_record_for_key(k, fkdd.get_default_record_for_key(k));
            }
            INtkdChannel temp_ch = fkdd.dh.retrieving_keys[k];
            fkdd.dh.retrieving_keys.unset(k);
            while (temp_ch.get_balance() < 0) temp_ch.send_async(0);
            fkdd.dh.not_completed_keys.remove(k);
        }

        /* Remotable methods */

        public IPeerParticipantSet get_participant_set
        (int lvl, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        throws PeersInvalidRequest
        {
            // check payload
            if (lvl <= 0 || lvl > levels) throw new PeersInvalidRequest.GENERIC("level out of range");
            // begin
            PeerParticipantSet ret = new PeerParticipantSet();
            foreach (int p_id in participant_maps.keys)
            {
                PeerParticipantMap map = new PeerParticipantMap();
                PeerParticipantMap my_map = participant_maps[p_id];
                bool participation_at_lvl = false;
                foreach (HCoord hc in my_map.participant_list)
                {
                    if (hc.lvl >= lvl) map.participant_list.add(hc);
                    else participation_at_lvl = true;
                }
                if (participation_at_lvl) map.participant_list.add(new HCoord(lvl, pos[lvl]));
                ret.participant_set[p_id] = map;
            }
            return ret;
        }

        public void forward_peer_message
        (IPeerMessage peer_message, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (peer_message is PeerMessageForwarder)) return;
            PeerMessageForwarder mf = (PeerMessageForwarder) peer_message;
            if (! check_valid_message(mf)) return;
            if (_rpc_caller == null) return;
            // begin
            debugging.DebugMessageForwarder deb = debugging.message_forwarder(mf);
            debug(@"... with mf = {msg_id=$(deb.msg_id), to=$(deb.x_macron) inside ($(deb.lvl), $(deb.pos)),\n");
            debug(@"               exclude_tuple_list=$(deb.exclude_tuple_list)}.\n");
            bool optional = false;
            bool exclude_myself = false;
            if (services.has_key(mf.p_id))
            {
                optional = services[mf.p_id].p_is_optional;
                exclude_myself = ! services[mf.p_id].is_ready();
            }
            else
                optional = true;
            if (pos[mf.lvl] == mf.pos)
            {
                if (! my_gnode_participates(mf.p_id, mf.lvl))
                {
                    IPeersManagerStub nstub
                        = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                    PeerTupleGNode gn = make_tuple_gnode(new HCoord(mf.lvl, mf.pos), mf.n.tuple.size);
                    try {
                        nstub.set_non_participant(mf.msg_id, gn);
                    } catch (zcd.ModRpc.StubError e) {
                        // ignore
                    } catch (zcd.ModRpc.DeserializeError e) {
                        // ignore
                    }
                }
                else
                {
                    ArrayList<HCoord> exclude_gnode_list = new ArrayList<HCoord>();
                    exclude_gnode_list.add_all(get_non_participant_gnodes(mf.p_id));
                    if (exclude_myself)
                        exclude_gnode_list.add(new HCoord(0, pos[0]));
                    foreach (PeerTupleGNode gn in mf.exclude_tuple_list)
                    {
                        int @case;
                        HCoord ret;
                        convert_tuple_gnode(gn, out @case, out ret);
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
                            IPeersManagerStub nstub
                                = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                            PeerTupleGNode gn = make_tuple_gnode(new HCoord(mf.lvl, mf.pos), mf.n.tuple.size);
                            try {
                                nstub.set_failure(mf.msg_id, gn);
                            } catch (zcd.ModRpc.StubError e) {
                                // ignore
                            } catch (zcd.ModRpc.DeserializeError e) {
                                // ignore
                            }
                            break;
                        }
                        else if (x.lvl == 0 && x.pos == pos[0])
                        {
                            Netsukuku.ModRpc.IPeersManagerStub nstub
                                = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                            PeerTupleNode tuple_respondant = make_tuple_node(new HCoord(0, pos[0]), mf.n.tuple.size);
                            try {
                                IPeersRequest request
                                    = nstub.get_request(mf.msg_id, tuple_respondant);
                                try {
                                    IPeersResponse resp = services[mf.p_id].exec(request, mf.n.tuple);
                                    nstub.set_response(mf.msg_id, resp, tuple_respondant);
                                } catch (PeersRedoFromStartError e) {
                                    try {
                                        nstub.set_redo_from_start(mf.msg_id, tuple_respondant);
                                    } catch (zcd.ModRpc.StubError e) {
                                        // ignore
                                    } catch (zcd.ModRpc.DeserializeError e) {
                                        // ignore
                                    }
                                } catch (PeersRefuseExecutionError e) {
                                    try {
                                        string err_message = "";
                                        if (e is PeersRefuseExecutionError.WRITE_OUT_OF_MEMORY)
                                                err_message = "WRITE_OUT_OF_MEMORY: ";
                                        if (e is PeersRefuseExecutionError.READ_NOT_FOUND_NOT_EXHAUSTIVE)
                                                err_message = "READ_NOT_FOUND_NOT_EXHAUSTIVE: ";
                                        if (e is PeersRefuseExecutionError.GENERIC)
                                                err_message = "GENERIC: ";
                                        err_message += e.message;
                                        nstub.set_refuse_message(mf.msg_id, err_message, tuple_respondant);
                                    } catch (zcd.ModRpc.StubError e) {
                                        // ignore
                                    } catch (zcd.ModRpc.DeserializeError e) {
                                        // ignore
                                    }
                                }
                            } catch (PeersUnknownMessageError e) {
                                // ignore
                            } catch (PeersInvalidRequest e) {
                                // ignore
                            } catch (zcd.ModRpc.StubError e) {
                                // ignore
                            } catch (zcd.ModRpc.DeserializeError e) {
                                // ignore
                            }
                            break;
                        }
                        else
                        {
                            PeerMessageForwarder mf2 = (PeerMessageForwarder)Json.gobject_deserialize
                                    (typeof(PeerMessageForwarder), Json.gobject_serialize(mf));
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
                                convert_tuple_gnode(t, out @case, out ret);
                                if (@case == 3)
                                {
                                    if (ret.equals(x))
                                    {
                                        int eps = t.top - t.tuple.size;
                                        PeerTupleGNode _t = new PeerTupleGNode(t.tuple.slice(0, x.lvl-eps), x.lvl);
                                        mf2.exclude_tuple_list.add(_t);
                                    }
                                }
                            }
                            foreach (PeerTupleGNode t in mf.non_participant_tuple_list)
                            {
                                if (visible_by_someone_inside_my_gnode(t, x.lvl+1))
                                    mf2.non_participant_tuple_list.add(t);
                            }
                            IPeersManagerStub gwstub;
                            IPeersManagerStub? failed = null;
                            while (true)
                            {
                                try {
                                    gwstub = map_paths.i_peers_gateway(mf2.lvl, mf2.pos, _rpc_caller, failed);
                                } catch (PeersNonexistentDestinationError e) {
                                    tasklet.ms_wait(20);
                                    break;
                                }
                                try {
                                    gwstub.forward_peer_message(mf2);
                                } catch (zcd.ModRpc.StubError e) {
                                    failed = gwstub;
                                    continue;
                                } catch (zcd.ModRpc.DeserializeError e) {
                                    assert_not_reached();
                                }
                                delivered = true;
                                IPeersManagerStub nstub
                                    = back_stub_factory.i_peers_get_tcp_inside(mf.n.tuple);
                                PeerTupleGNode gn = make_tuple_gnode(x, mf.n.tuple.size);
                                try {
                                    nstub.set_next_destination(mf.msg_id, gn);
                                } catch (zcd.ModRpc.StubError e) {
                                    // ignore
                                } catch (zcd.ModRpc.DeserializeError e) {
                                    // ignore
                                }
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                IPeersManagerStub gwstub;
                IPeersManagerStub? failed = null;
                while (true)
                {
                    try {
                        gwstub = map_paths.i_peers_gateway(mf.lvl, mf.pos, _rpc_caller, failed);
                    } catch (PeersNonexistentDestinationError e) {
                        // give up routing
                        break;
                    }
                    try {
                        gwstub.forward_peer_message(mf);
                    } catch (zcd.ModRpc.StubError e) {
                        failed = gwstub;
                        continue;
                    } catch (zcd.ModRpc.DeserializeError e) {
                        assert_not_reached();
                    }
                    break;
                }
            }
            if (optional && participant_maps.has_key(mf.p_id))
            {
                foreach (PeerTupleGNode t in mf.non_participant_tuple_list)
                {
                    int @case;
                    HCoord ret;
                    convert_tuple_gnode(t, out @case, out ret);
                    if (@case == 2)
                    {
                        if (ret in participant_maps[mf.p_id].participant_list)
                        {
                            CheckNonParticipationTasklet ts = new CheckNonParticipationTasklet();
                            ts.t = this;
                            ts.ret = ret;
                            ts.p_id = mf.p_id;
                            tasklet.spawn(ts);
                        }
                    }
                }
            }
        }
        private class CheckNonParticipationTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public HCoord ret;
            public int p_id;
            public void * func()
            {
                if (t.check_non_participation(p_id, ret.lvl, ret.pos))
                    if (t.participant_maps.has_key(p_id))
                        t.participant_maps[p_id].participant_list.remove(ret); 
                return null;
            }
        }

        public IPeersRequest get_request
        (int msg_id, IPeerTupleNode _respondant, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        throws PeersUnknownMessageError, PeersInvalidRequest
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.get_request: ignored because unknown msg_id");
                throw new PeersUnknownMessageError.GENERIC("unknown msg_id");
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.get_request: ignored because unknown class as IPeerTupleNode");
                throw new PeersInvalidRequest.GENERIC("unknown class as IPeerTupleNode");
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
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
        (int msg_id, IPeersResponse response, IPeerTupleNode _respondant, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_response: ignored because unknown msg_id");
                return;
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.set_response: ignored because unknown class as IPeerTupleNode");
                return;
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
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
        (int msg_id, string refuse_message, IPeerTupleNode _respondant, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_refuse_message: ignored because unknown msg_id");
                return;
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.set_refuse_message: ignored because unknown class as IPeerTupleNode");
                return;
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
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
            debug(@"PeersManager.set_refuse_message: $(refuse_message)");
            wa.ch.send_async(0);
        }

        public void set_redo_from_start
        (int msg_id, IPeerTupleNode _respondant, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_redo_from_start: ignored because unknown msg_id");
                return;
            }
            if (! (_respondant is PeerTupleNode))
            {
                debug("PeersManager.set_redo_from_start: ignored because unknown class as IPeerTupleNode");
                return;
            }
            PeerTupleNode respondant = (PeerTupleNode)_respondant;
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
        (int msg_id, IPeerTupleGNode _tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_next_destination: ignored because unknown msg_id");
                return;
            }
            if (! (_tuple is PeerTupleGNode))
            {
                debug("PeersManager.set_next_destination: ignored because unknown class as IPeerTupleGNode");
                return;
            }
            PeerTupleGNode tuple = (PeerTupleGNode)_tuple;
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
        (int msg_id, IPeerTupleGNode _tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_failure: ignored because unknown msg_id");
                return;
            }
            if (! (_tuple is PeerTupleGNode))
            {
                debug("PeersManager.set_failure: ignored because unknown class as IPeerTupleGNode");
                return;
            }
            PeerTupleGNode tuple = (PeerTupleGNode)_tuple;
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
        (int msg_id, IPeerTupleGNode _tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            if (! waiting_answer_map.has_key(msg_id))
            {
                debug("PeersManager.set_non_participant: ignored because unknown msg_id");
                return;
            }
            if (! (_tuple is PeerTupleGNode))
            {
                debug("PeersManager.set_non_participant: ignored because unknown class as IPeerTupleGNode");
                return;
            }
            PeerTupleGNode tuple = (PeerTupleGNode)_tuple;
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

        public void set_participant
        (int p_id, IPeerTupleGNode tuple, zcd.ModRpc.CallerInfo? _rpc_caller=null)
        {
            // check payload
            if (! (tuple is PeerTupleGNode)) return;
            PeerTupleGNode gn = (PeerTupleGNode) tuple;
            if (! check_valid_tuple_gnode(gn)) return;
            // begin
            if (services.has_key(p_id) && ! services[p_id].p_is_optional) return;
            int @case;
            HCoord ret;
            convert_tuple_gnode(gn, out @case, out @ret);
            if (@case == 1) return;
            if (ret in recent_published_list) return;
            recent_published_list.add(ret);
            if (! participant_maps.has_key(p_id))
                participant_maps[p_id] = new PeerParticipantMap();
            participant_maps[p_id].participant_list.add(ret);
            PeerTupleGNode ret_gn = make_tuple_gnode(ret, levels);
            MissingArcSetParticipant missing_handler = new MissingArcSetParticipant(this, p_id, ret_gn);
            IPeersManagerStub br_stub = neighbors_factory.i_peers_get_broadcast(missing_handler);
            try {
                br_stub.set_participant(p_id, ret_gn);
            } catch (zcd.ModRpc.StubError e) {
                // ignore
            } catch (zcd.ModRpc.DeserializeError e) {
                // ignore
            }
            RecentPublishedListRemoveTasklet ts = new RecentPublishedListRemoveTasklet();
            ts.t = this;
            ts.ret = ret;
            tasklet.spawn(ts);
        }
        private class RecentPublishedListRemoveTasklet : Object, INtkdTaskletSpawnable
        {
            public PeersManager t;
            public HCoord ret;
            public void * func()
            {
                tasklet.ms_wait(60000);
                t.recent_published_list.remove(ret);
                return null;
            }
        }
    }

    public abstract class PeerService : Object
    {
        public int p_id {get; private set;}
        public bool p_is_optional {get; private set;}
        public PeerService(int p_id, bool p_is_optional)
        {
            this.p_id = p_id;
            this.p_is_optional = p_is_optional;
        }

        public virtual bool is_ready()
        {
            return true;
        }

        public abstract IPeersResponse
        exec(IPeersRequest req, Gee.List<int> client_tuple)
        throws PeersRefuseExecutionError, PeersRedoFromStartError;
    }

    public abstract class PeerClient : Object
    {
        protected ArrayList<int> gsizes;
        private int p_id;
        private PeersManager peers_manager;
        public PeerClient(int p_id, Gee.List<int> gsizes, PeersManager peers_manager)
        {
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            this.p_id = p_id;
            this.peers_manager = peers_manager;
        }

        protected abstract uint64 hash_from_key(Object k, uint64 top);

        public virtual Gee.List<int> perfect_tuple(Object k)
        {
            uint64 top = 1;
            foreach (int gsize in gsizes) top *= gsize;
            uint64 hash = hash_from_key(k, top-1);
            assert(hash < top);
            ArrayList<int> tuple = new ArrayList<int>();
            foreach (int gsize in gsizes)
            {
                uint64 pos = hash % gsize;
                tuple.add((int)pos);
                hash /= gsize;
            }
            return tuple;
        }

        protected IPeersResponse call(Object k, IPeersRequest request, int timeout_exec)
        throws PeersNoParticipantsInNetworkError, PeersDatabaseError
        {
            PeerTupleNode respondant;
            debug(@"starting contact_peer for a specific request from $(get_type().name()) (Key is a $(k.get_type().name())).\n");
            return
            peers_manager.contact_peer
                (p_id,
                 new PeerTupleNode(perfect_tuple(k)),
                 request,
                 timeout_exec,
                 false,
                 out respondant);
        }
    }
}
