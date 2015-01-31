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

int main(string[] args)
{
    PeersManager.init();
    var m = new MyMapPaths();
    var bf = new MyBackFactory();
    PeersManager peers = new PeersManager(m, bf);
    //peers.register(p);

    return 0;
}
