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
using Netsukuku;
using Netsukuku.ModRpc;
using Tasklets;

class MyMapPaths : Object, IPeersMapPaths
{
    public int i_peers_get_levels()
    {
        error("not implemented yet");
    }

    public int i_peers_get_gsize(int level)
    {
        error("not implemented yet");
    }

    public int i_peers_get_nodes_in_my_group(int level)
    {
        error("not implemented yet");
    }

    public int i_peers_get_my_pos(int level)
    {
        error("not implemented yet");
    }

    public bool i_peers_exists(int level, int pos)
    {
        error("not implemented yet");
    }

    public IAddressManagerStub i_peers_gateway
        (int level, int pos,
         zcd.ModRpc.CallerInfo? received_from=null,
         IAddressManagerStub? failed=null)
        throws PeersNonexistentDestinationError
    {
        error("not implemented yet");
    }

    public IAddressManagerStub i_peers_fellow(int level)
        throws PeersNonexistentFellowError
    {
        error("not implemented yet");
    }
}

class MyBackFactory : Object, IPeersBackStubFactory
{
    public IAddressManagerStub i_peers_get_tcp_inside
        (Gee.List<int> positions)
    {
        error("not implemented yet");
    }
}

class MyNeighborsFactory : Object, IPeersNeighborsFactory
{
    public IAddressManagerStub i_peers_get_broadcast
        (IPeersMissingArcHandler missing_handler)
    {
        error("not implemented yet");
    }

    public IAddressManagerStub i_peers_get_tcp
        (IPeersArc arc)
    {
        error("not implemented yet");
    }
}

zcd.IZcdTasklet zcd_tasklet;
Netsukuku.INtkdTasklet ntkd_tasklet;

int main(string[] args)
{
    // Initialize tasklet system
    MyTaskletSystem.init();
    zcd_tasklet = MyTaskletSystem.get_zcd();
    ntkd_tasklet = MyTaskletSystem.get_ntkd();

    // Initialize known serializable classes
    // ... TODO
    // Pass tasklet system to ModRpc (and ZCD)
    zcd.ModRpc.init_tasklet_system(zcd_tasklet);
    // Pass tasklet system to module peerservices
    PeersManager.init(ntkd_tasklet);

    var m = new MyMapPaths();
    var bf = new MyBackFactory();
    var nf = new MyNeighborsFactory();
    PeersManager peers = new PeersManager(m, 4/*levels*/, bf, nf);
    //peers.register(p);

    MyTaskletSystem.kill();
    print("\nExiting.\n");
    return 0;
}
