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

namespace fk_101
{
    /* 
     * This class implements a simple service, not optional, whose id is 101.
     * It is a simple distributed database where for each record the key is
     * an integer number between 1 and 10, and the value is a string.
     * 
     * The records have a validity on the g-node of level ''k'' where ''k'' is
     * the key. So, if node ''n'' save/search the record for key 2, then it
     * will be saved/searched inside the g-node of level 2 where the node ''n''
     * lives in.
     * 
     * The default record fo any key has the value "" (emtpy string).
     * 
     * Expected operations are:
     *  * Modify a record k with a new value v. Expected exceptions: none.
     *  * Read the value of a record k. It is read-only. Expected exceptions: none.
     * 
     * For any write-operation, the servant node tries and replicates it on 10 nodes.
     * 
     * For the management of the database the class uses methods fk_db_on_startup and
     * fk_db_on_request provided by the module PeerServices.
     * 
     */
    internal const int p_id = 101;
    internal const int replica_q = 10;

    public class Fk101Key : Object
    {
        public int k {get; set;}
        public Fk101Key(int k)
        {
            this.k = k;
        }

        public static bool equal_data(Fk101Key k1, Fk101Key k2)
        {
            return k1.k == k2.k;
        }

        public static uint hash_data(Fk101Key k)
        {
            return @"$(k.k)".hash();
        }
    }

    public class Fk101Record : Object
    {
        public int k {get; set;}
        public string v {get; set;}
        public Fk101Record(int k, string v)
        {
            this.k = k;
            this.v = v;
        }
    }

    public class Fk101ModifyRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public string v {get; set;}
        public Fk101ModifyRecordRequest(int k, string v)
        {
            this.k = k;
            this.v = v;
        }
    }

    public class Fk101ModifyRecordSuccessResponse : Object, IPeersResponse
    {
    }

    public class Fk101ReadRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public Fk101ReadRecordRequest(int k)
        {
            this.k = k;
        }
    }

    public class Fk101ReadRecordSuccessResponse : Object, IPeersResponse
    {
        public string v {get; set;}
        public Fk101ReadRecordSuccessResponse(string v)
        {
            this.v = v;
        }
    }

    public class Fk101InvalidRequestResponse : Object, IPeersResponse
    {
        public string msg {get; set;}
        public Fk101InvalidRequestResponse(string msg)
        {
            this.msg = msg;
        }
    }

    public class Fk101ReplicaRecordRequest : Object, IPeersRequest
    {
        public Fk101Record record {get; set;}
        public Fk101ReplicaRecordRequest(Fk101Record record)
        {
            this.record = record;
        }
    }

    public class Fk101ReplicaRecordSuccessResponse : Object, IPeersResponse
    {
    }

    internal int timeout_exec_for_request(IPeersRequest r)
    {
        int timeout_write_operation = 8000;
        /* This is intentionally high because it accounts for a retrieve with
         * wait for a delta to guarantee coherence.
         */
        if (r is Fk101ModifyRecordRequest) return timeout_write_operation;
        if (r is Fk101ReadRecordRequest) return 1000;
        if (r is Fk101ReplicaRecordRequest) return 1000;
        assert_not_reached();
    }

    void serialization_tests()
    {
        {
            Fk101Key x0;
            {
                Json.Node node;
                {
                    Fk101Key x = new Fk101Key(8);
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101Key)Json.gobject_deserialize(typeof(Fk101Key), node);
            }
            assert(x0.k == 8);
        }
        {
            Fk101Record x0;
            {
                Json.Node node;
                {
                    Fk101Record x = new Fk101Record(8, "Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101Record)Json.gobject_deserialize(typeof(Fk101Record), node);
            }
            assert(x0.k == 8);
            assert(x0.v == "Unbreakable");
        }
        {
            Fk101ModifyRecordRequest x0;
            {
                Json.Node node;
                {
                    Fk101ModifyRecordRequest x = new Fk101ModifyRecordRequest(8, "Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101ModifyRecordRequest)Json.gobject_deserialize(typeof(Fk101ModifyRecordRequest), node);
            }
            assert(x0.k == 8);
            assert(x0.v == "Unbreakable");
        }
        {
            Fk101ReadRecordRequest x0;
            {
                Json.Node node;
                {
                    Fk101ReadRecordRequest x = new Fk101ReadRecordRequest(8);
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101ReadRecordRequest)Json.gobject_deserialize(typeof(Fk101ReadRecordRequest), node);
            }
            assert(x0.k == 8);
        }
        {
            Fk101ReadRecordSuccessResponse x0;
            {
                Json.Node node;
                {
                    Fk101ReadRecordSuccessResponse x = new Fk101ReadRecordSuccessResponse("Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101ReadRecordSuccessResponse)Json.gobject_deserialize(typeof(Fk101ReadRecordSuccessResponse), node);
            }
            assert(x0.v == "Unbreakable");
        }
        {
            Fk101InvalidRequestResponse x0;
            {
                Json.Node node;
                {
                    Fk101InvalidRequestResponse x = new Fk101InvalidRequestResponse("message in a bottle");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101InvalidRequestResponse)Json.gobject_deserialize(typeof(Fk101InvalidRequestResponse), node);
            }
            assert(x0.msg == "message in a bottle");
        }
        {
            Fk101ReplicaRecordRequest x0;
            {
                Json.Node node;
                {
                    Fk101ReplicaRecordRequest x = new Fk101ReplicaRecordRequest(new Fk101Record(8, "Unbreakable"));
                    node = Json.gobject_serialize(x);
                }
                x0 = (Fk101ReplicaRecordRequest)Json.gobject_deserialize(typeof(Fk101ReplicaRecordRequest), node);
            }
            assert(x0.record.k == 8);
            assert(x0.record.v == "Unbreakable");
        }
    }

    public class Fk101Service : PeerService
    {
        private PeersManager peers_manager;
        private Fk101Client client;
        private DatabaseDescriptor fkdd;
        private HashMap<Fk101Key, Fk101Record> my_records;
        public Fk101Service(Gee.List<int> gsizes, PeersManager peers_manager,
                            int level_new_gnode, bool register=true)
        {
            base(fk_101.p_id, false);
            this.peers_manager = peers_manager;
            this.client = new Fk101Client(gsizes, peers_manager);
            this.my_records = new HashMap<Fk101Key, Fk101Record>(Fk101Key.hash_data, Fk101Key.equal_data);
            this.fkdd = new DatabaseDescriptor(this);
            if (register)
            {
                peers_manager.register(this);
                debug("Service 101 registered.\n");
                // launch fixed_keys_db_on_startup in a tasklet
                StartFixedKeysDbHandlerTasklet ts = new StartFixedKeysDbHandlerTasklet();
                ts.t = this;
                ts.level_new_gnode = level_new_gnode;
                tasklet.spawn(ts);
            }
        }
        private class StartFixedKeysDbHandlerTasklet : Object, INtkdTaskletSpawnable
        {
            public Fk101Service t;
            public int level_new_gnode;
            public void * func()
            {
                t.tasklet_start_fixed_keys_db_handler(level_new_gnode); 
                return null;
            }
        }
        private void tasklet_start_fixed_keys_db_handler(int level_new_gnode)
        {
            peers_manager.fixed_keys_db_on_startup(fkdd, fk_101.p_id, level_new_gnode);
        }

        public override IPeersResponse exec
        (IPeersRequest req, Gee.List<int> client_tuple)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            return peers_manager.fixed_keys_db_on_request(fkdd, req, client_tuple.size);
        }

        private class DatabaseDescriptor : Object, IDatabaseDescriptor, IFixedKeysDatabaseDescriptor
        {
            private Fk101Service t;
            public DatabaseDescriptor(Fk101Service t)
            {
                this.t = t;
            }

            private DatabaseHandler _dh;

            public unowned DatabaseHandler dh_getter()
            {
                return _dh;
            }

            public void dh_setter(DatabaseHandler x)
            {
                _dh = x;
            }

            public bool is_valid_key(Object k)
            {
                if (k is Fk101Key)
                {
                    int _k = ((Fk101Key)k).k;
                    if (_k >= 1 && _k <= 10) return true;
                }
                return false;
            }

            public Gee.List<int> evaluate_hash_node(Object k)
            {
                assert(k is Fk101Key);
                return t.client.perfect_tuple(k);
            }

            public bool key_equal_data(Object k1, Object k2)
            {
                assert(k1 is Fk101Key);
                Fk101Key _k1 = (Fk101Key)k1;
                assert(k2 is Fk101Key);
                Fk101Key _k2 = (Fk101Key)k2;
                return Fk101Key.equal_data(_k1, _k2);
            }

            public uint key_hash_data(Object k)
            {
                assert(k is Fk101Key);
                Fk101Key _k = (Fk101Key)k;
                return Fk101Key.hash_data(_k);
            }

            public bool is_valid_record(Object k, Object rec)
            {
                if (! (k is Fk101Key)) return false;
                if (! (rec is Fk101Record)) return false;
                if (((Fk101Record)rec).k != ((Fk101Key)k).k) return false;
                return true;
            }

            public bool my_records_contains(Object k)
            {
                return true;
            }

            public Object get_record_for_key(Object k)
            {
                assert(k is Fk101Key);
                Fk101Key _k = (Fk101Key)k;
                assert(t.my_records.has_key(_k));
                return t.my_records[_k];
            }

            public void set_record_for_key(Object k, Object rec)
            {
                assert(k is Fk101Key);
                Fk101Key _k = (Fk101Key)k;
                assert(rec is Fk101Record);
                Fk101Record _rec = (Fk101Record)rec;
                t.my_records[_k] = _rec;
                debug(@"Service101: save record for k=$(_k.k)\n");
            }

            public Object get_key_from_request(IPeersRequest r)
            {
                if (r is Fk101ModifyRecordRequest)
                {
                    Fk101ModifyRecordRequest _r = (Fk101ModifyRecordRequest)r;
                    return new Fk101Key(_r.k);
                }
                else if (r is Fk101ReadRecordRequest)
                {
                    Fk101ReadRecordRequest _r = (Fk101ReadRecordRequest)r;
                    return new Fk101Key(_r.k);
                }
                else if (r is Fk101ReplicaRecordRequest)
                {
                    Fk101ReplicaRecordRequest _r = (Fk101ReplicaRecordRequest)r;
                    return new Fk101Key(_r.record.k);
                }
                error("The module is asking for a key when the request does not contain.");
            }

            public int get_timeout_exec(IPeersRequest r)
            {
                if (r is Fk101ModifyRecordRequest)
                {
                    return timeout_exec_for_request(r);
                }
                error("The module is asking for a timeout_exec when the request is not a write.");
            }

            public bool is_insert_request(IPeersRequest r)
            {
                return false;
            }

            public bool is_read_only_request(IPeersRequest r)
            {
                if (r is Fk101ReadRecordRequest) return true;
                return false;
            }

            public bool is_update_request(IPeersRequest r)
            {
                if (r is Fk101ModifyRecordRequest) return true;
                return false;
            }

            public bool is_replica_value_request(IPeersRequest r)
            {
                if (r is Fk101ReplicaRecordRequest) return true;
                return false;
            }

            public bool is_replica_delete_request(IPeersRequest r)
            {
                return false;
            }

            public IPeersResponse prepare_response_not_found(IPeersRequest r)
            {
                assert_not_reached();
            }

            public IPeersResponse prepare_response_not_free(IPeersRequest r, Object rec)
            {
                assert_not_reached();
            }

            public IPeersResponse execute(IPeersRequest r)
            throws PeersRefuseExecutionError, PeersRedoFromStartError
            {
                if (r is Fk101ModifyRecordRequest)
                {
                    Fk101ModifyRecordRequest _r = (Fk101ModifyRecordRequest)r;
                    t.handle_modify(_r.k, _r.v);
                    return new Fk101ModifyRecordSuccessResponse();
                }
                else if (r is Fk101ReadRecordRequest)
                {
                    Fk101ReadRecordRequest _r = (Fk101ReadRecordRequest)r;
                    return new Fk101ReadRecordSuccessResponse(t.handle_read(_r.k));
                }
                else if (r is Fk101ReplicaRecordRequest)
                {
                    Fk101ReplicaRecordRequest _r = (Fk101ReplicaRecordRequest)r;
                    t.handle_replica_record(_r.record);
                    return new Fk101ReplicaRecordSuccessResponse();
                }
                if (r == null)
                    return new Fk101InvalidRequestResponse("Not a valid request class: null");
                return new Fk101InvalidRequestResponse(@"Not a valid request class: $(r.get_type().name())");
            }

            public Gee.List<Object> get_full_key_domain()
            {
                var ret = new ArrayList<Object>();
                for (int i = 1; i <= 10; i++) ret.add(new Fk101Key(i));
                return ret;
            }

            public Object get_default_record_for_key(Object k)
            {
                assert(is_valid_key(k));
                return new Fk101Record(((Fk101Key)k).k, "");
            }
        }

        private void handle_modify(int k, string v)
        {
            debug(@"modify $(k), $(v)\n");
            Fk101Key key = new Fk101Key(k);
            my_records[key] = new Fk101Record(k, v);
            RequestReplicaRecordTasklet ts = new RequestReplicaRecordTasklet();
            ts.t = this;
            ts.k = key;
            ts.record = my_records[key];
            tasklet.spawn(ts);
        }

        private string handle_read(int k)
        {
            Fk101Key key = new Fk101Key(k);
            Fk101Record rec = my_records[key];
            return rec.v;
        }

        private void handle_replica_record(Fk101Record record)
        {
            my_records[new Fk101Key(record.k)] = record;
            debug(@"Service101: save record for k=$(record.k)\n");
        }

        private class RequestReplicaRecordTasklet : Object, INtkdTaskletSpawnable
        {
            public Fk101Service t;
            public Fk101Key k;
            public Fk101Record record;
            public void * func()
            {
                t.tasklet_request_replica_record(k, record); 
                return null;
            }
        }
        private void tasklet_request_replica_record(Fk101Key k, Fk101Record record)
        {
            Gee.List<int> perfect_tuple = client.perfect_tuple(k);
            Fk101ReplicaRecordRequest r = new Fk101ReplicaRecordRequest(record);
            int timeout_exec = timeout_exec_for_request(r);
            IPeersResponse resp;
            IPeersContinuation cont;
            if (peers_manager.begin_replica(replica_q, p_id, perfect_tuple, r, timeout_exec, out resp, out cont))
            {
                while (peers_manager.next_replica(cont, out resp))
                {
                    // nop
                }
            }
        }

        /* methods used for testing purposes
         */
        public HashMap<int, string> get_records()
        {
            HashMap<int, string> ret = new HashMap<int, string>();
            foreach (Fk101Key k in my_records.keys)
            {
                Fk101Record r = my_records[k];
                ret[k.k] = r.v;
            }
            return ret;
        }
    }

    public class Fk101Client : PeerClient
    {
        public Fk101Client(Gee.List<int> gsizes, PeersManager peers_manager)
        {
            base(fk_101.p_id, gsizes, peers_manager);
        }

        /** 32 bit Fowler/Noll/Vo hash
          */
        private uint32 fnv_32(uint8[] buf)
        {
            uint32 hval = (uint32)2166136261;
            foreach (uint8 c in buf)
            {
                hval += (hval<<1) + (hval<<4) + (hval<<7) + (hval<<8) + (hval<<24);
                hval ^= c;
            }
            return hval;
        }

        protected override uint64 hash_from_key(Object k, uint64 top)
        {
            assert(k is Fk101Key);
            Fk101Key _k = (Fk101Key)k;
            // hash of 64 bits (always a even number, but who cares) from the integer:
            uint64 hash = fnv_32(@"$(_k.k)_$(_k.k)_$(_k.k)".data) * 2;
            return hash % (top+1);
        }

        public override Gee.List<int> perfect_tuple(Object k)
        {
            assert(k is Fk101Key);
            Fk101Key _k = (Fk101Key)k;
            int l = _k.k;
            Gee.List<int> ret = base.perfect_tuple(k);
            if (l == 10) return ret;
            if (l < ret.size) ret = ret.slice(0, l);
            return ret;
        }

        public void db_modify(int k, string v)
        {
            IPeersResponse resp;
            IPeersRequest r = new Fk101ModifyRecordRequest(k, v);
            try {
                resp = this.call(new Fk101Key(k), r, timeout_exec_for_request(r));
            } catch (PeersNoParticipantsInNetworkError e) {
                error("Fk101Client: db_modify: Got 'no participants', in this simulation the service is not optional.");
            } catch (PeersDatabaseError e) {
                error("Fk101Client: db_modify: Got 'database error', impossible for a write operation.");
            }
            if (resp is Fk101InvalidRequestResponse)
            {
                warning(@"Fk101Client: db_modify: Got 'invalid request'. Key was $(k). Modification probably failed.");
                return;
            }
            if (resp is Fk101ModifyRecordSuccessResponse)
            {
                return;
            }
            // unexpected class
            if (resp == null)
                warning(@"Fk101Client: db_modify: Got unexpected null" +
                @", throwing an 'not found'. Key was $(k).");
            else
                warning(@"Fk101Client: db_modify: Got unexpected class $(resp.get_type().name())" +
                @", throwing an 'not found'. Key was $(k).");
            return;
        }

        public string db_read(int k)
        {
            IPeersResponse resp;
            IPeersRequest r = new Fk101ReadRecordRequest(k);
            while (true)
            {
                try {
                    resp = this.call(new Fk101Key(k), r, timeout_exec_for_request(r));
                    break;
                } catch (PeersNoParticipantsInNetworkError e) {
                    error("Fk101Client: db_read: Got 'no participants', in this simulation the service is not optional.");
                } catch (PeersDatabaseError e) {
                    debug("Fk101Client: db_read: Got 'database error', will retry.");
                    tasklet.ms_wait(200);
                }
            }
            if (resp is Fk101InvalidRequestResponse)
            {
                warning(@"Fk101Client: db_read: Got 'invalid request'. Key was $(k). Returning a default.");
                return "";
            }
            if (resp is Fk101ReadRecordSuccessResponse)
            {
                return ((Fk101ReadRecordSuccessResponse)resp).v;
            }
            // unexpected class
            if (resp == null)
                warning(@"Fk101Client: db_read: Got unexpected null." +
                @"Key was $(k). Returning a default.");
            else
                warning(@"Fk101Client: db_read: Got unexpected class $(resp.get_type().name())." +
                @"Key was $(k). Returning a default.");
            return "";
        }
    }

}

