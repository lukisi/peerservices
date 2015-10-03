/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2015 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
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

using Netsukuku;
using Netsukuku.ModRpc;
using Gee;

namespace LibPeersInternals
{
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
        var ret = new ArrayList<HCoord>(/*equal_func*/(a, b) => a.equals(b));
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
