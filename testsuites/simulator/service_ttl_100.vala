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

namespace ttl_100
{
    /* 
     * Questa classe implementa un servizio non opzionale, il cui ID è 100.
     * Si tratta di un semplice database distribuito in cui la chiave è un
     * numero e il valore una stringa.
     * 
     * Il record ha un TTL di 20 secondi. La vita dei record è gestita
     * dalla classe Ttl100Service. Viene esposto il valore del TTL perché
     * il modulo PeerServices gestisce il tempo di ''non esaustivo''.
     * 
     * Ogni singolo nodo può decidere quale sia il numero massimo di
     * record che memorizza. Viene specificato nel costruttore di
     * Ttl100Service. Un default è 50. Cosa analoga per il numero
     * massimo di chiavi in memoria (ad uso interno del modulo) il cui
     * default è 200.
     * 
     * Le operazioni previste sono:
     *  * Inserisci un record k,v. Possibili eccezioni: Ttl100OutOfMemoryError, Ttl100NotFreeError.
     *  * Modifica un record k con il nuovo valore v. Possibili eccezioni: Ttl100NotFoundError.
     *  * Tocca un record k. E' di modifica del solo TTL. Possibili eccezioni: Ttl100NotFoundError.
     *  * Leggi il valore di un record k. Di sola lettura. Possibili eccezioni: Ttl100NotFoundError.
     * 
     * Per ogni scrittura, il nodo servente tenta di replicarla su 10 nodi.
     * 
     * La gestione del database si avvale dei metodi ttl_db_on_startup e
     * ttl_db_on_request forniti dal modulo PeerServices.
     * 
     */
    internal const int fresh_msec_ttl = 20000;
    internal const int timeout_send_keys_multiplier = 50;
    /* The module will request a number of keys that is at most the number
     * of records that the node is willing to maintain. This number is available
     * as Ttl100Service.max_records, so the timeout to use when we request a
     * list of keys will be this multiplier x max_records.
     */
    internal const int p_id = 100;
    internal const int replica_q = 10;

    public errordomain Ttl100OutOfMemoryError {GENERIC}
    public errordomain Ttl100NotFreeError {GENERIC}
    public errordomain Ttl100NotFoundError {GENERIC}

    public class Ttl100Key : Object
    {
        public int k {get; set;}
        public Ttl100Key(int k)
        {
            this.k = k;
        }

        public static bool equal_data(Ttl100Key k1, Ttl100Key k2)
        {
            return k1.k == k2.k;
        }

        public static uint hash_data(Ttl100Key k)
        {
            return @"$(k.k)".hash();
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

        internal long msec_remaining {
            get {
                return msec_ttl - get_lap() / 1000;
            }
        }
    }

    public class Ttl100Record : Object
    {
        public int k {get; set;}
        public string v {get; set;}
        public int msec_ttl {
            get{
                return (int)this.ttl.msec_remaining;
            }
            set{
                this.ttl = new Timer(value);
            }
        }
        internal Timer ttl;
        public Ttl100Record(int k, string v, int msec_ttl=-1)
        {
            this.k = k;
            this.v = v;
            if (msec_ttl == -1) this.msec_ttl = fresh_msec_ttl;
            else this.msec_ttl = msec_ttl;
        }
    }

    public class Ttl100InsertRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public string v {get; set;}
        public Ttl100InsertRecordRequest(int k, string v)
        {
            this.k = k;
            this.v = v;
        }
    }

    public class Ttl100InsertRecordSuccessResponse : Object, IPeersResponse
    {
    }

    public class Ttl100InsertRecordNotFreeResponse : Object, IPeersResponse
    {
    }

    public class Ttl100TouchRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public Ttl100TouchRecordRequest(int k)
        {
            this.k = k;
        }
    }

    public class Ttl100TouchRecordSuccessResponse : Object, IPeersResponse
    {
    }

    public class Ttl100ModifyRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public string v {get; set;}
        public Ttl100ModifyRecordRequest(int k, string v)
        {
            this.k = k;
            this.v = v;
        }
    }

    public class Ttl100ModifyRecordSuccessResponse : Object, IPeersResponse
    {
    }

    public class Ttl100ReadRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public Ttl100ReadRecordRequest(int k)
        {
            this.k = k;
        }
    }

    public class Ttl100ReadRecordSuccessResponse : Object, IPeersResponse
    {
        public string v {get; set;}
        public Ttl100ReadRecordSuccessResponse(string v)
        {
            this.v = v;
        }
    }

    public class Ttl100SearchRecordNotFoundResponse : Object, IPeersResponse
    {
    }

    public class Ttl100InvalidRequestResponse : Object, IPeersResponse
    {
        public string msg {get; set;}
        public Ttl100InvalidRequestResponse(string msg)
        {
            this.msg = msg;
        }
    }

    public class Ttl100ReplicaRecordRequest : Object, IPeersRequest
    {
        public Ttl100Record record {get; set;}
        public Ttl100ReplicaRecordRequest(Ttl100Record record)
        {
            this.record = record;
        }
    }

    public class Ttl100ReplicaRecordSuccessResponse : Object, IPeersResponse
    {
    }

    public class Ttl100ReplicaDeleteRequest : Object, IPeersRequest
    {
        public Ttl100Key k {get; set;}
        public Ttl100ReplicaDeleteRequest(Ttl100Key k)
        {
            this.k = k;
        }
    }

    public class Ttl100ReplicaDeleteSuccessResponse : Object, IPeersResponse
    {
    }

    internal int timeout_exec_for_request(IPeersRequest r)
    {
        int timeout_write_operation = 8000;
        /* This is intentionally high because it accounts for a retrieve with
         * wait for a delta to guarantee coherence.
         */
        if (r is Ttl100InsertRecordRequest) return timeout_write_operation;
        if (r is Ttl100TouchRecordRequest) return timeout_write_operation;
        if (r is Ttl100ModifyRecordRequest) return timeout_write_operation;
        if (r is Ttl100ReadRecordRequest) return 1000;
        if (r is Ttl100ReplicaRecordRequest) return 1000;
        if (r is Ttl100ReplicaDeleteRequest) return 1000;
        assert_not_reached();
    }

    void serialization_tests()
    {
        {
            Ttl100Key x0;
            {
                Json.Node node;
                {
                    Ttl100Key x = new Ttl100Key(12);
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100Key)Json.gobject_deserialize(typeof(Ttl100Key), node);
            }
            assert(x0.k == 12);
        }
        {
            Ttl100Record x0;
            {
                Json.Node node;
                {
                    Ttl100Record x = new Ttl100Record(12, "Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100Record)Json.gobject_deserialize(typeof(Ttl100Record), node);
            }
            assert(x0.k == 12);
            assert(x0.v == "Unbreakable");
        }
        {
            Ttl100InsertRecordRequest x0;
            {
                Json.Node node;
                {
                    Ttl100InsertRecordRequest x = new Ttl100InsertRecordRequest(12, "Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100InsertRecordRequest)Json.gobject_deserialize(typeof(Ttl100InsertRecordRequest), node);
            }
            assert(x0.k == 12);
            assert(x0.v == "Unbreakable");
        }
        {
            Ttl100TouchRecordRequest x0;
            {
                Json.Node node;
                {
                    Ttl100TouchRecordRequest x = new Ttl100TouchRecordRequest(12);
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100TouchRecordRequest)Json.gobject_deserialize(typeof(Ttl100TouchRecordRequest), node);
            }
            assert(x0.k == 12);
        }
        {
            Ttl100ModifyRecordRequest x0;
            {
                Json.Node node;
                {
                    Ttl100ModifyRecordRequest x = new Ttl100ModifyRecordRequest(12, "Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100ModifyRecordRequest)Json.gobject_deserialize(typeof(Ttl100ModifyRecordRequest), node);
            }
            assert(x0.k == 12);
            assert(x0.v == "Unbreakable");
        }
        {
            Ttl100ReadRecordRequest x0;
            {
                Json.Node node;
                {
                    Ttl100ReadRecordRequest x = new Ttl100ReadRecordRequest(12);
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100ReadRecordRequest)Json.gobject_deserialize(typeof(Ttl100ReadRecordRequest), node);
            }
            assert(x0.k == 12);
        }
        {
            Ttl100ReadRecordSuccessResponse x0;
            {
                Json.Node node;
                {
                    Ttl100ReadRecordSuccessResponse x = new Ttl100ReadRecordSuccessResponse("Unbreakable");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100ReadRecordSuccessResponse)Json.gobject_deserialize(typeof(Ttl100ReadRecordSuccessResponse), node);
            }
            assert(x0.v == "Unbreakable");
        }
        {
            Ttl100InvalidRequestResponse x0;
            {
                Json.Node node;
                {
                    Ttl100InvalidRequestResponse x = new Ttl100InvalidRequestResponse("message in a bottle");
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100InvalidRequestResponse)Json.gobject_deserialize(typeof(Ttl100InvalidRequestResponse), node);
            }
            assert(x0.msg == "message in a bottle");
        }
        {
            Ttl100ReplicaRecordRequest x0;
            {
                Json.Node node;
                {
                    Ttl100ReplicaRecordRequest x = new Ttl100ReplicaRecordRequest(new Ttl100Record(12, "Unbreakable"));
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100ReplicaRecordRequest)Json.gobject_deserialize(typeof(Ttl100ReplicaRecordRequest), node);
            }
            assert(x0.record.k == 12);
            assert(x0.record.v == "Unbreakable");
            assert(x0.record.ttl.msec_remaining <= fresh_msec_ttl);
            assert(x0.record.ttl.msec_remaining > (fresh_msec_ttl / 2));
        }
        {
            Ttl100ReplicaDeleteRequest x0;
            {
                Json.Node node;
                {
                    Ttl100ReplicaDeleteRequest x = new Ttl100ReplicaDeleteRequest(new Ttl100Key(12));
                    node = Json.gobject_serialize(x);
                }
                x0 = (Ttl100ReplicaDeleteRequest)Json.gobject_deserialize(typeof(Ttl100ReplicaDeleteRequest), node);
            }
            assert(x0.k.k == 12);
        }
    }

    public class Ttl100Service : PeerService
    {
        private PeersManager peers_manager;
        private Ttl100Client client;
        private DatabaseDescriptor tdd;
        private int max_records;
        private const int default_max_records = 50;
        private int max_keys;
        private const int default_max_keys = 200;
        private HashMap<Ttl100Key, Ttl100Record> my_records;
        public Ttl100Service(Gee.List<int> gsizes, PeersManager peers_manager, bool register=true, int max_records=-1, int max_keys=-1)
        {
            base(ttl_100.p_id, false);
            this.peers_manager = peers_manager;
            this.client = new Ttl100Client(gsizes, peers_manager);
            this.max_records = max_records == -1 ? default_max_records : max_records;
            this.max_keys = max_keys == -1 ? default_max_keys : max_keys;
            this.my_records = new HashMap<Ttl100Key, Ttl100Record>(Ttl100Key.hash_data, Ttl100Key.equal_data);
            this.tdd = new DatabaseDescriptor(this);
            if (register)
            {
                peers_manager.register(this);
                // launch ttl_db_on_startup in a tasklet
                StartTtlDbHandlerTasklet ts = new StartTtlDbHandlerTasklet();
                ts.t = this;
                tasklet.spawn(ts);
            }
        }
        private class StartTtlDbHandlerTasklet : Object, INtkdTaskletSpawnable
        {
            public Ttl100Service t;
            public void * func()
            {
                t.tasklet_start_ttl_db_handler(); 
                return null;
            }
        }
        private void tasklet_start_ttl_db_handler()
        {
            peers_manager.ttl_db_on_startup(tdd, ttl_100.p_id);
        }

        private void purge()
        {
            ArrayList<Ttl100Key> todel = new ArrayList<Ttl100Key>();
            foreach (Ttl100Key k in my_records.keys)
            {
                Ttl100Record r = my_records[k];
                if (r.ttl.is_expired()) todel.add(k);
            }
            foreach (Ttl100Key k in todel) my_records.unset(k);
        }

        public override IPeersResponse exec
        (IPeersRequest req, Gee.List<int> client_tuple)
        throws PeersRefuseExecutionError, PeersRedoFromStartError
        {
            purge();
            return peers_manager.ttl_db_on_request(tdd, req, client_tuple.size);
        }

        private class DatabaseDescriptor : Object, IDatabaseDescriptor, ITemporalDatabaseDescriptor
        {
            private Ttl100Service t;
            public DatabaseDescriptor(Ttl100Service t)
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
                if (k is Ttl100Key) return true;
                return false;
            }

            public Gee.List<int> evaluate_hash_node(Object k)
            {
                assert(k is Ttl100Key);
                return t.client.perfect_tuple(k);
            }

            public bool key_equal_data(Object k1, Object k2)
            {
                assert(k1 is Ttl100Key);
                Ttl100Key _k1 = (Ttl100Key)k1;
                assert(k2 is Ttl100Key);
                Ttl100Key _k2 = (Ttl100Key)k2;
                return Ttl100Key.equal_data(_k1, _k2);
            }

            public uint key_hash_data(Object k)
            {
                assert(k is Ttl100Key);
                Ttl100Key _k = (Ttl100Key)k;
                return Ttl100Key.hash_data(_k);
            }

            public bool is_valid_record(Object k, Object rec)
            {
                if (! (k is Ttl100Key)) return false;
                if (! (rec is Ttl100Record)) return false;
                if (((Ttl100Record)rec).k != ((Ttl100Key)k).k) return false;
                return true;
            }

            public bool my_records_contains(Object k)
            {
                assert(k is Ttl100Key);
                Ttl100Key _k = (Ttl100Key)k;
                return t.my_records.has_key(_k);
            }

            public Object get_record_for_key(Object k)
            {
                assert(k is Ttl100Key);
                Ttl100Key _k = (Ttl100Key)k;
                assert(t.my_records.has_key(_k));
                return t.my_records[_k];
            }

            public void set_record_for_key(Object k, Object rec)
            {
                assert(k is Ttl100Key);
                Ttl100Key _k = (Ttl100Key)k;
                assert(rec is Ttl100Record);
                Ttl100Record _rec = (Ttl100Record)rec;
                t.my_records[_k] = _rec;
            }

            public Object get_key_from_request(IPeersRequest r)
            {
                if (r is Ttl100InsertRecordRequest)
                {
                    Ttl100InsertRecordRequest _r = (Ttl100InsertRecordRequest)r;
                    return new Ttl100Key(_r.k);
                }
                else if (r is Ttl100ModifyRecordRequest)
                {
                    Ttl100ModifyRecordRequest _r = (Ttl100ModifyRecordRequest)r;
                    return new Ttl100Key(_r.k);
                }
                else if (r is Ttl100TouchRecordRequest)
                {
                    Ttl100TouchRecordRequest _r = (Ttl100TouchRecordRequest)r;
                    return new Ttl100Key(_r.k);
                }
                else if (r is Ttl100ReadRecordRequest)
                {
                    Ttl100ReadRecordRequest _r = (Ttl100ReadRecordRequest)r;
                    return new Ttl100Key(_r.k);
                }
                else if (r is Ttl100ReplicaRecordRequest)
                {
                    Ttl100ReplicaRecordRequest _r = (Ttl100ReplicaRecordRequest)r;
                    return new Ttl100Key(_r.record.k);
                }
                else if (r is Ttl100ReplicaDeleteRequest)
                {
                    Ttl100ReplicaDeleteRequest _r = (Ttl100ReplicaDeleteRequest)r;
                    return _r.k;
                }
                error("The module is asking for a key when the request does not contain.");
            }

            public int get_timeout_exec(IPeersRequest r)
            {
                if (r is Ttl100InsertRecordRequest)
                {
                    return timeout_exec_for_request(r);
                }
                else if (r is Ttl100ModifyRecordRequest)
                {
                    return timeout_exec_for_request(r);
                }
                else if (r is Ttl100TouchRecordRequest)
                {
                    return timeout_exec_for_request(r);
                }
                // TODO add delete
                error("The module is asking for a timeout_exec when the request is not a write.");
            }

            public bool is_insert_request(IPeersRequest r)
            {
                if (r is Ttl100InsertRecordRequest) return true;
                return false;
            }

            public bool is_read_only_request(IPeersRequest r)
            {
                if (r is Ttl100ReadRecordRequest) return true;
                return false;
            }

            public bool is_update_request(IPeersRequest r)
            {
                if (r is Ttl100TouchRecordRequest) return true;
                if (r is Ttl100ModifyRecordRequest) return true;
                return false;
            }

            public bool is_replica_value_request(IPeersRequest r)
            {
                if (r is Ttl100ReplicaRecordRequest) return true;
                return false;
            }

            public bool is_replica_delete_request(IPeersRequest r)
            {
                if (r is Ttl100ReplicaDeleteRequest) return true;
                return false;
            }

            public IPeersResponse prepare_response_not_found(IPeersRequest r)
            {
                return new Ttl100SearchRecordNotFoundResponse();
            }

            public IPeersResponse prepare_response_not_free(IPeersRequest r, Object rec)
            {
                return new Ttl100InsertRecordNotFreeResponse();
            }

            public IPeersResponse execute(IPeersRequest r)
            throws PeersRefuseExecutionError, PeersRedoFromStartError
            {
                if (r is Ttl100InsertRecordRequest)
                {
                    Ttl100InsertRecordRequest _r = (Ttl100InsertRecordRequest)r;
                    t.handle_insert(_r.k, _r.v);
                    return new Ttl100InsertRecordSuccessResponse();
                }
                else if (r is Ttl100ModifyRecordRequest)
                {
                    Ttl100ModifyRecordRequest _r = (Ttl100ModifyRecordRequest)r;
                    t.handle_modify(_r.k, _r.v);
                    return new Ttl100ModifyRecordSuccessResponse();
                }
                else if (r is Ttl100TouchRecordRequest)
                {
                    Ttl100TouchRecordRequest _r = (Ttl100TouchRecordRequest)r;
                    t.handle_touch(_r.k);
                    return new Ttl100TouchRecordSuccessResponse();
                }
                else if (r is Ttl100ReadRecordRequest)
                {
                    Ttl100ReadRecordRequest _r = (Ttl100ReadRecordRequest)r;
                    return new Ttl100ReadRecordSuccessResponse(t.handle_read(_r.k));
                }
                else if (r is Ttl100ReplicaRecordRequest)
                {
                    Ttl100ReplicaRecordRequest _r = (Ttl100ReplicaRecordRequest)r;
                    t.handle_replica_record(_r.record);
                    return new Ttl100ReplicaRecordSuccessResponse();
                }
                else if (r is Ttl100ReplicaDeleteRequest)
                {
                    Ttl100ReplicaDeleteRequest _r = (Ttl100ReplicaDeleteRequest)r;
                    t.handle_replica_delete(_r.k.k);
                    return new Ttl100ReplicaDeleteSuccessResponse();
                }
                if (r == null)
                    return new Ttl100InvalidRequestResponse("Not a valid request class: null");
                return new Ttl100InvalidRequestResponse(@"Not a valid request class: $(r.get_type().name())");
            }

            public int ttl_db_max_records_getter()
            {
                return t.max_records;
            }

            public int ttl_db_my_records_size()
            {
                return t.my_records.size;
            }
            
            public int ttl_db_max_keys_getter()
            {
                return t.max_keys;
            }

            public int ttl_db_msec_ttl_getter()
            {
                return fresh_msec_ttl;
            }
            
            public Gee.List<Object> ttl_db_get_all_keys()
            {
                ArrayList<Object> r = new ArrayList<Object>();
                r.add_all(t.my_records.keys);
                return r;
            }

            public int ttl_db_timeout_exec_send_keys_getter()
            {
                return timeout_send_keys_multiplier * t.max_records;
            }
        }

        private void handle_insert(int k, string v)
        {
            debug(@"insert $(k), $(v)\n");
            Ttl100Key key = new Ttl100Key(k);
            my_records[key] = new Ttl100Record(k, v);
            HandleReplicaRecordTasklet ts = new HandleReplicaRecordTasklet();
            ts.t = this;
            ts.k = key;
            ts.record = my_records[key];
            tasklet.spawn(ts);
        }

        private void handle_touch(int k)
        {
            Ttl100Key key = new Ttl100Key(k);
            Ttl100Record rec = my_records[key];
            rec.msec_ttl = fresh_msec_ttl;
            HandleReplicaRecordTasklet ts = new HandleReplicaRecordTasklet();
            ts.t = this;
            ts.k = key;
            ts.record = my_records[key];
            tasklet.spawn(ts);
        }

        private void handle_modify(int k, string v)
        {
            Ttl100Key key = new Ttl100Key(k);
            my_records[key] = new Ttl100Record(k, v);
            HandleReplicaRecordTasklet ts = new HandleReplicaRecordTasklet();
            ts.t = this;
            ts.k = key;
            ts.record = my_records[key];
            tasklet.spawn(ts);
        }

        private string handle_read(int k)
        {
            Ttl100Key key = new Ttl100Key(k);
            Ttl100Record rec = my_records[key];
            return rec.v;
        }

        private void handle_replica_record(Ttl100Record record)
        {
            my_records[new Ttl100Key(record.k)] = record;
        }

        private void handle_replica_delete(int k)
        {
            my_records.unset(new Ttl100Key(k));
        }

        private class HandleReplicaRecordTasklet : Object, INtkdTaskletSpawnable
        {
            public Ttl100Service t;
            public Ttl100Key k;
            public Ttl100Record record;
            public void * func()
            {
                t.tasklet_handle_replica_record(k, record); 
                return null;
            }
        }
        private void tasklet_handle_replica_record(Ttl100Key k, Ttl100Record record)
        {
            Gee.List<int> perfect_tuple = client.perfect_tuple(k);
            Ttl100ReplicaRecordRequest r = new Ttl100ReplicaRecordRequest(record);
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

        private class HandleReplicaDeleteTasklet : Object, INtkdTaskletSpawnable
        {
            public Ttl100Service t;
            public Ttl100Key k;
            public void * func()
            {
                t.tasklet_handle_replica_delete(k); 
                return null;
            }
        }
        private void tasklet_handle_replica_delete(Ttl100Key k)
        {
            // TODO
        }
    }

    public class Ttl100Client : PeerClient
    {
        public Ttl100Client(Gee.List<int> gsizes, PeersManager peers_manager)
        {
            base(ttl_100.p_id, gsizes, peers_manager);
        }

        protected override uint64 hash_from_key(Object k, uint64 top)
        {
            assert(k is Ttl100Key);
            Ttl100Key _k = (Ttl100Key)k;
            return @"$(_k.k)".hash() % (top+1);
        }
        /*
     *  * Inserisci un record k,v. Possibili eccezioni: Ttl100OutOfMemoryError, Ttl100NotFreeError.
     *  * Modifica un record k con il nuovo valore v. Possibili eccezioni: Ttl100NotFoundError.
     *  * Tocca un record k. E' di modifica del solo TTL. Possibili eccezioni: Ttl100NotFoundError.
     *  * Leggi il valore di un record k. Di sola lettura. Possibili eccezioni: Ttl100NotFoundError.
        */

        public void db_insert(int k, string v) throws Ttl100OutOfMemoryError, Ttl100NotFreeError
        {
            IPeersResponse resp;
            IPeersRequest r = new Ttl100InsertRecordRequest(k, v);
            try {
                resp = this.call(new Ttl100Key(k), r, timeout_exec_for_request(r));
            } catch (PeersNoParticipantsInNetworkError e) {
                debug("Ttl100Client: db_insert: Got 'no participants', throwing an 'out of memory'.");
                throw new Ttl100OutOfMemoryError.GENERIC("Out of memory");
            } catch (PeersDatabaseError e) {
                debug("Ttl100Client: db_insert: Got 'database error', throwing an 'out of memory'.");
                throw new Ttl100OutOfMemoryError.GENERIC("Out of memory");
            }
            if (resp is Ttl100InsertRecordNotFreeResponse)
            {
                debug(@"Ttl100Client: db_insert: Got 'not free'. Key was $(k).");
                throw new Ttl100NotFreeError.GENERIC(@"$(k) not free");
            }
            if (resp is Ttl100InvalidRequestResponse)
            {
                warning(@"Ttl100Client: db_insert: Got 'invalid request', throwing an 'out of memory'. Key was $(k).");
                throw new Ttl100OutOfMemoryError.GENERIC("Invalid request");
            }
            if (resp is Ttl100InsertRecordSuccessResponse)
            {
                return;
            }
            // unexpected class
            if (resp == null)
                warning(@"Ttl100Client: db_insert: Got unexpected null" +
                @", throwing an 'out of memory'. Key was $(k).");
            else
                warning(@"Ttl100Client: db_insert: Got unexpected class $(resp.get_type().name())" +
                @", throwing an 'out of memory'. Key was $(k).");
            throw new Ttl100OutOfMemoryError.GENERIC("Unexpected response");
        }

        public void db_modify(int k, string v) throws Ttl100NotFoundError
        {
            error("not implemented yet");
        }

        public void db_touch(int k) throws Ttl100NotFoundError
        {
            error("not implemented yet");
        }

        public string db_read(int k) throws Ttl100NotFoundError
        {
            error("not implemented yet");
        }
    }

}

