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
using Netsukuku;
using Netsukuku.PeerServices;
using TaskletSystem;

string json_string_object(Object obj)
{
    Json.Node n = Json.gobject_serialize(obj);
    Json.Generator g = new Json.Generator();
    g.root = n;
    g.pretty = true;
    string ret = g.to_data(null);
    return ret;
}

Object dup_object(Object obj)
{
    Type type = obj.get_type();
    string t = json_string_object(obj);
    Json.Parser p = new Json.Parser();
    try {
        assert(p.load_from_data(t));
    } catch (Error e) {assert_not_reached();}
    Object ret = Json.gobject_deserialize(type, p.get_root());
    return ret;
}

string address(Gee.List<int> pos)
{
    string ret = ""; string next = "";
    foreach (int p in pos) {
        ret = @"$(p)$(next)$(ret)";
        next = ":";
    }
    return ret;
}

HCoord find_hcoord(Gee.List<int> me, Gee.List<int> other)
{
    assert(me.size == other.size);
    var gn_other = new PeerTupleGNode(other, other.size);
    int @case;
    HCoord ret;
    Utils.convert_tuple_gnode(me, gn_other, out @case, out ret);
    return ret;
}

namespace Srv01Names {
    const string Mark = "Mark";
    const string John = "John";
    const string Luke = "Luke";
    const string Stef = "Stef";
    const string Sue = "Sue";
    const string Bob = "Bob";
    const string Clark = "Clark";
}

int main(string[] args)
{
    GLib.Test.init(ref args);

    // Initialize tasklet system
    PthTaskletImplementer.init();
    tasklet = PthTaskletImplementer.get_tasklet_system();

    GLib.Test.add_func ("/Databases/Replicas", () => {
        var x = new Replicas();
        x.test_replicas();
    });
    GLib.Test.add_func ("/Databases/Entering", () => {
        var x = new Entering();
        x.test_entering();
    });
    GLib.Test.add_func ("/Databases/SearchInside", () => {
        var x = new SearchInside();
        //x.test_search_inside();
    });

    GLib.Test.run();
    return 0;
}

//FAKE

namespace Netsukuku.PeerServices
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

    internal ITasklet tasklet;
}
