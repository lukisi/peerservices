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

namespace Netsukuku.PeerServices
{
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

        public bool check_valid(int levels, int[] gsizes)
        {
            if (this.tuple.size == 0) return false;
            if (this.tuple.size > levels) return false;
            for (int i = 0; i < this.tuple.size; i++)
            {
                if (this.tuple[i] < 0) return false;
                if (this.tuple[i] >= gsizes[i]) return false;
            }
            return true;
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

        public bool check_valid(int levels, int[] gsizes)
        {
            if (this.tuple.size == 0) return false;
            if (this.top > levels) return false;
            if (this.tuple.size > this.top) return false;
            for (int i = 0; i < this.tuple.size; i++)
            {
                int eps = this.top - this.tuple.size;
                if (this.tuple[i] < 0) return false;
                if (this.tuple[i] >= gsizes[eps+i]) return false;
            }
            return true;
        }
    }

    internal class PeerMessageForwarder : Object, Json.Serializable, IPeerMessage
    {
        public int inside_level {get; set;}
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
            case "inside-level":
            case "inside_level":
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
            case "inside-level":
            case "inside_level":
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

        public bool check_valid(int levels, int[] gsizes)
        {
            if (! this.n.check_valid(levels, gsizes)) return false;
            if (this.lvl < 0) return false;
            if (this.lvl >= levels) return false;
            if (this.pos < 0) return false;
            if (this.pos >= gsizes[this.lvl]) return false;
            if (this.n.tuple.size <= this.lvl) return false;
            if (this.x_macron != null)
            {
                if (! this.x_macron.check_valid(levels, gsizes)) return false;
                if (this.x_macron.tuple.size != this.lvl) return false;
            }
            if (! this.exclude_tuple_list.is_empty)
            {
                foreach (PeerTupleGNode gn in this.exclude_tuple_list)
                {
                    if (! gn.check_valid(levels, gsizes)) return false;
                    if (gn.top != this.lvl) return false;
                }
            }
            if (! this.non_participant_tuple_list.is_empty)
            {
                PeerTupleGNode gn;
                gn = this.non_participant_tuple_list[0];
                if (! gn.check_valid(levels, gsizes)) return false;
                if (gn.top <= this.lvl) return false;
                int first_top = gn.top;
                for (int i = 1; i < this.non_participant_tuple_list.size; i++)
                {
                    gn = this.non_participant_tuple_list[i];
                    if (! gn.check_valid(levels, gsizes)) return false;
                    if (gn.top != first_top) return false;
                }
            }
            return true;
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

        public bool check_valid(int levels, int[] gsizes)
        {
            foreach (HCoord h in this.participant_list)
            {
                if (h.lvl < 0) return false;
                if (h.lvl > levels) return false;
                if (h.pos < 0) return false;
                if (h.pos > gsizes[h.lvl]) return false;
            }
            return true;
        }
    }

    internal class PeerParticipantSet : Object, Json.Serializable, IPeerParticipantSet
    {
        public int retrieved_below_level {get; set;}
        public Gee.List<int> my_pos {get; set;}
        public HashMap<int, PeerParticipantMap> participant_set {get; set;}

        public PeerParticipantSet(Gee.List<int> my_pos)
        {
            participant_set = new HashMap<int, PeerParticipantMap>();
            this.my_pos = new ArrayList<int>();
            this.my_pos.add_all(my_pos);
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
            case "retrieved-below-level":
            case "retrieved_below_level":
                try {
                    @value = deserialize_int(property_node);
                } catch (HelperDeserializeError e) {
                    return false;
                }
                break;
            case "my-pos":
            case "my_pos":
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
            case "participant_set":
            case "participant-set":
                return serialize_map_int_peer_participant_map((HashMap<int, PeerParticipantMap>)@value);
            case "retrieved-below-level":
            case "retrieved_below_level":
                return serialize_int((int)@value);
            case "my-pos":
            case "my_pos":
                return serialize_list_int((Gee.List<int>)@value);
            default:
                error(@"wrong param $(property_name)");
            }
        }

        public bool check_valid(int levels, int[] gsizes)
        {
            if (this.retrieved_below_level < 0) return false;
            if (this.retrieved_below_level > levels) return false;
            foreach (int pid in this.participant_set.keys)
            {
                if (! this.participant_set[pid].check_valid(levels, gsizes)) return false;
            }
            if (this.my_pos.size != levels) return false;
            for (int lvl = 0; lvl < levels; lvl++)
            {
                if (this.my_pos[lvl] < 0) return false;
                // NOT MANDATORY: if (this.my_pos[lvl] >= gsizes[lvl]) return false;
                // because may be a virtual node.
            }
            return true;
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

    internal errordomain HelperDeserializeError {
        GENERIC
    }

    internal Object? deserialize_object(Type expected_type, bool nullable, Json.Node property_node)
    throws HelperDeserializeError
    {
        Json.Reader r = new Json.Reader(property_node.copy());
        if (r.get_null_value())
        {
            if (!nullable)
                throw new HelperDeserializeError.GENERIC("element is not nullable");
            return null;
        }
        if (!r.is_object())
            throw new HelperDeserializeError.GENERIC("element must be an object");
        string typename;
        if (!r.read_member("typename"))
            throw new HelperDeserializeError.GENERIC("element must have typename");
        if (!r.is_value())
            throw new HelperDeserializeError.GENERIC("typename must be a string");
        if (r.get_value().get_value_type() != typeof(string))
            throw new HelperDeserializeError.GENERIC("typename must be a string");
        typename = r.get_string_value();
        r.end_member();
        Type type = Type.from_name(typename);
        if (type == 0)
            throw new HelperDeserializeError.GENERIC(@"typename '$(typename)' unknown class");
        if (!type.is_a(expected_type))
            throw new HelperDeserializeError.GENERIC(@"typename '$(typename)' is not a '$(expected_type.name())'");
        if (!r.read_member("value"))
            throw new HelperDeserializeError.GENERIC("element must have value");
        r.end_member();
        unowned Json.Node p_value = property_node.get_object().get_member("value");
        Json.Node cp_value = p_value.copy();
        return Json.gobject_deserialize(type, cp_value);
    }

    internal Json.Node serialize_object(Object? obj)
    {
        if (obj == null) return new Json.Node(Json.NodeType.NULL);
        Json.Builder b = new Json.Builder();
        b.begin_object();
        b.set_member_name("typename");
        b.add_string_value(obj.get_type().name());
        b.set_member_name("value");
        Json.Node * obj_n = Json.gobject_serialize(obj);
        // json_builder_add_value docs says: The builder will take ownership of the #JsonNode.
        // but the vapi does not specify that the formal parameter is owned.
        // So I try and handle myself the unref of obj_n
        b.add_value(obj_n);
        b.end_object();
        return b.get_root();
    }

    internal class ListDeserializer<T> : Object
    {
        internal Gee.List<T> deserialize_list_object(Json.Node property_node)
        throws HelperDeserializeError
        {
            ArrayList<T> ret = new ArrayList<T>();
            Json.Reader r = new Json.Reader(property_node.copy());
            if (r.get_null_value())
                throw new HelperDeserializeError.GENERIC("element is not nullable");
            if (!r.is_array())
                throw new HelperDeserializeError.GENERIC("element must be an array");
            int l = r.count_elements();
            for (uint j = 0; j < l; j++)
            {
                unowned Json.Node p_value = property_node.get_array().get_element(j);
                Json.Node cp_value = p_value.copy();
                ret.add(deserialize_object(typeof(T), false, cp_value));
            }
            return ret;
        }
    }

    internal Gee.List<Object> deserialize_list_object(Json.Node property_node)
    throws HelperDeserializeError
    {
        ListDeserializer<Object> c = new ListDeserializer<Object>();
        return c.deserialize_list_object(property_node);
    }

    internal Json.Node serialize_list_object(Gee.List<Object> lst)
    {
        Json.Builder b = new Json.Builder();
        b.begin_array();
        foreach (Object obj in lst)
        {
            b.begin_object();
            b.set_member_name("typename");
            b.add_string_value(obj.get_type().name());
            b.set_member_name("value");
            Json.Node * obj_n = Json.gobject_serialize(obj);
            // json_builder_add_value docs says: The builder will take ownership of the #JsonNode.
            // but the vapi does not specify that the formal parameter is owned.
            // So I try and handle myself the unref of obj_n
            b.add_value(obj_n);
            b.end_object();
        }
        b.end_array();
        return b.get_root();
    }

    internal int deserialize_int(Json.Node property_node)
    throws HelperDeserializeError
    {
        Json.Reader r = new Json.Reader(property_node.copy());
        if (r.get_null_value())
            throw new HelperDeserializeError.GENERIC("element is not nullable");
        if (!r.is_value())
            throw new HelperDeserializeError.GENERIC("element must be a int");
        if (r.get_value().get_value_type() != typeof(int64))
            throw new HelperDeserializeError.GENERIC("element must be a int");
        int64 val = r.get_int_value();
        if (val > int.MAX || val < int.MIN)
            throw new HelperDeserializeError.GENERIC("element overflows size of int");
        return (int)val;
    }

    internal Json.Node serialize_int(int i)
    {
        Json.Node ret = new Json.Node(Json.NodeType.VALUE);
        ret.set_int(i);
        return ret;
    }

    internal bool deserialize_bool(Json.Node property_node)
    throws HelperDeserializeError
    {
        Json.Reader r = new Json.Reader(property_node.copy());
        if (r.get_null_value())
            throw new HelperDeserializeError.GENERIC("element is not nullable");
        if (!r.is_value())
            throw new HelperDeserializeError.GENERIC("element must be a boolean");
        if (r.get_value().get_value_type() != typeof(bool))
            throw new HelperDeserializeError.GENERIC("element must be a boolean");
        return r.get_boolean_value();
    }

    internal Json.Node serialize_bool(bool b)
    {
        Json.Node ret = new Json.Node(Json.NodeType.VALUE);
        ret.set_boolean(b);
        return ret;
    }

    internal PeerTupleNode deserialize_peer_tuple_node(Json.Node property_node)
    throws HelperDeserializeError
    {
        return (PeerTupleNode)deserialize_object(typeof(PeerTupleNode), false, property_node);
    }

    internal Json.Node serialize_peer_tuple_node(PeerTupleNode n)
    {
        return serialize_object(n);
    }

    internal PeerTupleNode? deserialize_nullable_peer_tuple_node(Json.Node property_node)
    throws HelperDeserializeError
    {
        return (PeerTupleNode?)deserialize_object(typeof(PeerTupleNode), true, property_node);
    }

    internal Json.Node serialize_nullable_peer_tuple_node(PeerTupleNode? n)
    {
        return serialize_object(n);
    }

    internal Gee.List<HCoord> deserialize_list_hcoord(Json.Node property_node)
    throws HelperDeserializeError
    {
        ListDeserializer<HCoord> c = new ListDeserializer<HCoord>();
        var first_ret = c.deserialize_list_object(property_node);
        // N.B. list of HCoord must be searchable.
        var ret = new ArrayList<HCoord>((a, b) => a.equals(b));
        ret.add_all(first_ret);
        return ret;
    }

    internal Json.Node serialize_list_hcoord(Gee.List<HCoord> lst)
    {
        return serialize_list_object(lst);
    }

    internal Gee.List<PeerTupleGNode> deserialize_list_peer_tuple_gnode(Json.Node property_node)
    throws HelperDeserializeError
    {
        ListDeserializer<PeerTupleGNode> c = new ListDeserializer<PeerTupleGNode>();
        return c.deserialize_list_object(property_node);
    }

    internal Json.Node serialize_list_peer_tuple_gnode(Gee.List<PeerTupleGNode> lst)
    {
        return serialize_list_object(lst);
    }

    internal HashMap<int, PeerParticipantMap> deserialize_map_int_peer_participant_map(Json.Node property_node)
    throws HelperDeserializeError
    {
        Json.Reader r = new Json.Reader(property_node.copy());
        if (r.get_null_value())
            throw new HelperDeserializeError.GENERIC("map is not nullable");
        if (!r.is_object())
            throw new HelperDeserializeError.GENERIC("map must be an object");
        if (!r.read_member("keys"))
            throw new HelperDeserializeError.GENERIC("map must have keys");
        if (!r.is_array())
            throw new HelperDeserializeError.GENERIC("keys must be a array");
        r.end_member();
        if (!r.read_member("values"))
            throw new HelperDeserializeError.GENERIC("map must have values");
        if (!r.is_array())
            throw new HelperDeserializeError.GENERIC("values must be a array");
        r.end_member();
        unowned Json.Node node_k_lst = property_node.get_object().get_member("keys");
        Gee.List<int> k_lst = deserialize_list_int(node_k_lst);
        unowned Json.Node node_v_lst = property_node.get_object().get_member("values");
        ListDeserializer<PeerParticipantMap> c = new ListDeserializer<PeerParticipantMap>();
        Gee.List<PeerParticipantMap> v_lst = c.deserialize_list_object(node_v_lst);
        if (k_lst.size != v_lst.size)
            throw new HelperDeserializeError.GENERIC("map must have same number of keys and values");
        HashMap<int, PeerParticipantMap> ret = new HashMap<int, PeerParticipantMap>();
        for (int i = 0; i < k_lst.size; i++) ret[k_lst[i]] = v_lst[i];
        return ret;
    }

    internal Json.Node serialize_map_int_peer_participant_map(HashMap<int, PeerParticipantMap> map)
    {
        ArrayList<int> k_lst = new ArrayList<int>();
        k_lst.add_all(map.keys);
        ArrayList<PeerParticipantMap> v_lst = new ArrayList<PeerParticipantMap>();
        foreach (int k in k_lst) v_lst.add(map[k]);
        Json.Builder b = new Json.Builder();
        b.begin_object();
        // json_builder_add_value docs says: The builder will take ownership of the #JsonNode.
        // but the vapi does not specify that the formal parameter is owned.
        // So I try and handle myself the unref of obj_k and obj_v.
        b.set_member_name("keys");
        Json.Node * obj_k = serialize_list_int(k_lst);
        b.add_value(obj_k);
        b.set_member_name("values");
        Json.Node * obj_v = serialize_list_object(v_lst);
        b.add_value(obj_v);
        b.end_object();
        return b.get_root();
    }

    internal Gee.List<int> deserialize_list_int(Json.Node property_node)
    throws HelperDeserializeError
    {
        ArrayList<int> ret = new ArrayList<int>();
        Json.Reader r = new Json.Reader(property_node.copy());
        if (r.get_null_value())
            throw new HelperDeserializeError.GENERIC("element is not nullable");
        if (!r.is_array())
            throw new HelperDeserializeError.GENERIC("element must be an array");
        int l = r.count_elements();
        for (int j = 0; j < l; j++)
        {
            r.read_element(j);
            if (r.get_null_value())
                throw new HelperDeserializeError.GENERIC("element is not nullable");
            if (!r.is_value())
                throw new HelperDeserializeError.GENERIC("element must be a int");
            if (r.get_value().get_value_type() != typeof(int64))
                throw new HelperDeserializeError.GENERIC("element must be a int");
            int64 val = r.get_int_value();
            if (val > int.MAX || val < int.MIN)
                throw new HelperDeserializeError.GENERIC("element overflows size of int");
            ret.add((int)val);
            r.end_element();
        }
        return ret;
    }

    internal Json.Node serialize_list_int(Gee.List<int> lst)
    {
        Json.Builder b = new Json.Builder();
        b.begin_array();
        foreach (int i in lst)
        {
            b.add_int_value(i);
        }
        b.end_array();
        return b.get_root();
    }
}