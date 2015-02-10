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

using Tasklets;
using Gee;
using zcd;
using Netsukuku;

namespace Netsukuku
{
    public void    log_debug(string msg)   {print(msg+"\n");}
    public void    log_trace(string msg)   {print(msg+"\n");}
    public void  log_verbose(string msg)   {print(msg+"\n");}
    public void     log_info(string msg)   {print(msg+"\n");}
    public void   log_notice(string msg)   {print(msg+"\n");}
    public void     log_warn(string msg)   {print(msg+"\n");}
    public void    log_error(string msg)   {print(msg+"\n");}
    public void log_critical(string msg)   {print(msg+"\n");}
}

class MyMapPaths : Object, IPeersMapPaths
{
    public int i_peers_get_levels()
    {
        assert_not_reached();
    }

    public int i_peers_get_gsize(int level)
    {
        assert_not_reached();
    }

    public int i_peers_get_nodes_in_my_group(int level)
    {
        assert_not_reached();
    }

    public int i_peers_get_my_pos(int level)
    {
        assert_not_reached();
    }

    public bool i_peers_exists(int level, int pos)
    {
        assert_not_reached();
    }

    public IAddressManagerRootDispatcher i_peers_gateway
        (int level, int pos,
         CallerInfo? received_from=null,
         IAddressManagerRootDispatcher? failed=null)
        throws PeersNonexistentDestinationError
    {
        assert_not_reached();
    }

}

class MyBackFactory : Object, IPeersBackStubFactory
{
    public IAddressManagerRootDispatcher i_peers_get_tcp_inside
        (Gee.List<int> positions)
    {
        assert_not_reached();
    }
}

class MyNeighborsFactory : Object, IPeersNeighborsFactory
{
    public IAddressManagerRootDispatcher i_peers_get_broadcast
        (IPeersMissingArcHandler missing_handler)
    {
        assert_not_reached();
    }

    public IAddressManagerRootDispatcher i_peers_get_tcp
        (IPeersArc arc)
    {
        assert_not_reached();
    }
}

int main(string[] args)
{
    PeersManager.init();
    var m = new MyMapPaths();
    var bf = new MyBackFactory();
    var nf = new MyNeighborsFactory();
    PeersManager peers = new PeersManager(m, bf, nf);
    //peers.register(p);

    return 0;
}
