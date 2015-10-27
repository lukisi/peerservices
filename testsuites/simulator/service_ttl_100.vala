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
     * Ttl100Service. Un default è 50.
     * 
     * Le operazioni previste sono:
     *  * Scrivi un record k,v. Di sola scrittura.
     *  * Tocca un record k. E' di lettura + scrittura.
     *    Se esiste non lo cambia, ma riporta il TTL a nuovo (20 secondi).
     *    Possibile eccezione Ttl100NotFound.
     *  * Leggi il valore di un record k. Di sola lettura.
     *    Possibile eccezione Ttl100NotFound.
     * 
     * Per ogni scrittura, il nodo servente tenta di replicarla su 10 nodi.
     * 
     * Il reperimento iniziale viene svolto con i metodi ttl_db_begin e
     * ttl_db_got_request forniti dal modulo PeerServices.
     * 
     */
    internal const int fresh_msec_ttl = 20000;
    internal const int p_id = 100;
    public class Ttl100Key : Object
    {
        public int k {get; set;}
        public Ttl100Key(int k)
        {
            this.k = k;
        }
    }

    internal bool key_equal_data(Ttl100Key k1, Ttl100Key k2)
    {
        return k1.k == k2.k;
    }

    internal uint key_hash_data(Ttl100Key k)
    {
        return @"$(k.k)".hash();
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
        private Timer ttl;
        public Ttl100Record(int k, string v, int msec_ttl=-1)
        {
            this.k = k;
            this.v = v;
            if (msec_ttl == -1) this.msec_ttl = fresh_msec_ttl;
            else this.msec_ttl = msec_ttl;
        }
    }

    public class Ttl100WriteRecordRequest : Object, IPeersRequest
    {
        public int k {get; set;}
        public string v {get; set;}
        public Ttl100WriteRecordRequest(int k, string v)
        {
            this.k = k;
            this.v = v;
        }
    }

    public class Ttl100WriteRecordResponse : Object, IPeersResponse
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

    public class Ttl100TouchRecordResponse : Object, IPeersResponse
    {
    }

    public class Ttl100TouchRecordNotFound : Object, IPeersResponse
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

    public class Ttl100ReadRecordResponse : Object, IPeersResponse
    {
        public string v {get; set;}
        public Ttl100ReadRecordResponse(string v)
        {
            this.v = v;
        }
    }

    public class Ttl100ReadRecordNotFound : Object, IPeersResponse
    {
    }

    public class Ttl100Service : PeerService
    {
        private PeersManager peers_manager;
        private Ttl100Client client;
        private DatabaseDescriptor tdd;
        private int max_records;
        private HashMap<Ttl100Key, Ttl100Record> my_records;
        public Ttl100Service(Gee.List<int> gsizes, PeersManager peers_manager, int max_records=50)
        {
            base(p_id, false);
            this.peers_manager = peers_manager;
            this.client = new Ttl100Client(gsizes, peers_manager);
            this.max_records = max_records;
            this.my_records = new HashMap<Ttl100Key, Ttl100Record>(ttl_100.key_hash_data, ttl_100.key_equal_data);
            this.tdd = new DatabaseDescriptor(this);
            // TODO start ttl_db_begin in a tasklet
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

        public override IPeersResponse exec(IPeersRequest req, Gee.List<int> client_tuple) throws PeersRefuseExecutionError
        {
            error("not implemented yet");
        }

        private class DatabaseDescriptor : Object, ITemporalDatabaseDescriptor
        {
            private Ttl100Service t;
            public DatabaseDescriptor(Ttl100Service t)
            {
                this.t = t;
            }

            private TemporalDatabaseHandler _tdh;

            public unowned TemporalDatabaseHandler tdh_getter()
            {
                return _tdh;
            }

            public void tdh_setter(TemporalDatabaseHandler x)
            {
                _tdh = x;
            }

            public int p_id_getter()
            {
                return p_id;
            }

            public int timeout_exec_send_keys_getter()
            {
                return 10000;
            }

            public int timeout_exec_wait_then_send_record_getter()
            {
                return 120000;
            }

            public int msec_ttl_getter()
            {
                return fresh_msec_ttl;
            }

            public int max_records_getter()
            {
                return t.max_records;
            }

            public bool is_valid_key(Object k)
            {
                if (k is Ttl100Key) return true;
                return false;
            }

            public Gee.List<int> evaluate_hash_node(Object k, int lvl)
            {
                assert(k is Ttl100Key);
                Gee.List<int> r = t.client.perfect_tuple(k);
                assert(lvl <= r.size);
                ArrayList<int> ret = new ArrayList<int>();
                for (int i = 0; i < lvl; i++) ret.add(r[i]);
                return ret;
            }

            public bool key_equal_data(Object k1, Object k2)
            {
                assert(k1 is Ttl100Key);
                Ttl100Key _k1 = (Ttl100Key)k1;
                assert(k2 is Ttl100Key);
                Ttl100Key _k2 = (Ttl100Key)k2;
                return ttl_100.key_equal_data(_k1, _k2);
            }

            public uint key_hash_data(Object k)
            {
                assert(k is Ttl100Key);
                Ttl100Key _k = (Ttl100Key)k;
                return ttl_100.key_hash_data(_k);
            }

            public int my_records_size()
            {
                return t.my_records.size;
            }

            public bool my_records_contains(Object k)
            {
                assert(k is Ttl100Key);
                Ttl100Key _k = (Ttl100Key)k;
                return t.my_records.has_key(_k);
            }

            public Gee.List<Object> get_all_keys()
            {
                ArrayList<Object> r = new ArrayList<Object>();
                r.add_all(t.my_records.keys);
                return r;
            }

            public Object get_key_from_request(IPeersRequest r)
            {
                if (r is Ttl100ReadRecordRequest)
                    return new Ttl100Key(((Ttl100ReadRecordRequest)r).k);
                if (r is Ttl100WriteRecordRequest)
                    return new Ttl100Key(((Ttl100WriteRecordRequest)r).k);
                if (r is Ttl100TouchRecordRequest)
                    return new Ttl100Key(((Ttl100TouchRecordRequest)r).k);
                error("Got unknown request class from the database handler.");
            }

            public bool is_read_only_request(IPeersRequest r)
            {
                if (r is Ttl100ReadRecordRequest) return true;
                return false;
            }

            public bool is_write_only_request(IPeersRequest r)
            {
                if (r is Ttl100WriteRecordRequest) return true;
                return false;
            }

            public bool is_read_write_request(IPeersRequest r)
            {
                if (r is Ttl100TouchRecordRequest) return true;
                return false;
            }

            public IPeersResponse execute_request(IPeersRequest r)
            {
                if (r is Ttl100ReadRecordRequest)
                {
                    // TODO
                    assert_not_reached();
                }
                if (r is Ttl100WriteRecordRequest)
                {
                    // TODO
                    assert_not_reached();
                }
                if (r is Ttl100TouchRecordRequest)
                {
                    // TODO
                    assert_not_reached();
                }
                error("Got unknown request class from the database handler.");
            }

            public IPeersResponse prepare_response_not_found(IPeersRequest r, Object k)
            {
                if (r is Ttl100ReadRecordRequest)
                    return new Ttl100ReadRecordNotFound();
                if (r is Ttl100WriteRecordRequest)
                    error("Got invalid request class (WriteRecord) from the database handler.");
                if (r is Ttl100TouchRecordRequest)
                    return new Ttl100TouchRecordNotFound();
                error("Got unknown request class from the database handler.");
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
        }
    }

    public class Ttl100Client : PeerClient
    {
        public Ttl100Client(Gee.List<int> gsizes, PeersManager peers_manager)
        {
            base(p_id, gsizes, peers_manager);
        }

        protected override uint64 hash_from_key(Object k, uint64 top)
        {
            assert(k is Ttl100Key);
            Ttl100Key _k = (Ttl100Key)k;
            return @"$(_k.k)".hash() % (top+1);
        }
    }

}

