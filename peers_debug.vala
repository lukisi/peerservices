using Netsukuku;
using Netsukuku.PeerServices;

namespace debugging
{
    internal class DebugTupleNode : Object
    {
        public string s;
    }
    internal DebugTupleNode tuple_node(PeerTupleNode n)
    {
        DebugTupleNode ret = new DebugTupleNode();
        ret.s = "";
        string prefix = "";
        foreach (int p in n.tuple)
        {
            ret.s += @"$(prefix)$(p)";
            prefix = ", ";
        }
        ret.s = @"[$(ret.s)]";
        return ret;
    }

    internal class DebugTupleGNode : Object
    {
        public string s;
    }
    internal DebugTupleGNode tuple_gnode(PeerTupleGNode n)
    {
        DebugTupleGNode ret = new DebugTupleGNode();
        ret.s = "";
        string prefix = "";
        for (int i = 0; i < n.top-n.tuple.size; i++)
        {
            ret.s += @"$(prefix)*";
            prefix = ", ";
        }
        foreach (int p in n.tuple)
        {
            ret.s += @"$(prefix)$(p)";
            prefix = ", ";
        }
        ret.s = @"[$(ret.s)]";
        return ret;
    }

    internal class DebugMessageForwarder : Object
    {
        public string n;
        public string x_macron;
        public int lvl;
        public int pos;
        public int p_id;
        public int msg_id;
        public string exclude_tuple_list;
        public string non_participant_tuple_list;
    }
    internal DebugMessageForwarder message_forwarder(PeerMessageForwarder mf)
    {
        DebugMessageForwarder ret = new DebugMessageForwarder();
        ret.n = tuple_node(mf.n).s;
        ret.x_macron = "null";
        if (mf.x_macron != null) ret.x_macron = tuple_node(mf.x_macron).s;
        ret.lvl = mf.lvl;
        ret.pos = mf.pos;
        ret.p_id = mf.p_id;
        ret.msg_id = mf.msg_id;

        ret.exclude_tuple_list = "";
        string prefix = "";
        foreach (PeerTupleGNode gn in mf.exclude_tuple_list)
        {
            string s_gn = tuple_gnode(gn).s;
            ret.exclude_tuple_list += @"$(prefix){$(s_gn)}";
            prefix = ", ";
        }
        ret.exclude_tuple_list = @"[$(ret.exclude_tuple_list)]";

        ret.non_participant_tuple_list = "";
        prefix = "";
        foreach (PeerTupleGNode gn in mf.non_participant_tuple_list)
        {
            string s_gn = tuple_gnode(gn).s;
            ret.non_participant_tuple_list += @"$(prefix){$(s_gn)}";
            prefix = ", ";
        }
        ret.non_participant_tuple_list = @"[$(ret.non_participant_tuple_list)]";

        return ret;
    }

    internal class DebugWaitingAnswer : Object
    {
        public string request;
        public string min_target;
        public string exclude_gnode;
        public string non_participant_gnode;
        public string respondant_node;
        public string response;
        public string refuse_message;
        public bool redo_from_start;
    }
    internal DebugWaitingAnswer waiting_answer(WaitingAnswer wa)
    {
        DebugWaitingAnswer ret = new DebugWaitingAnswer();
        ret.request = "null";
        if (wa.request != null) ret.request = "something...";
        ret.min_target = tuple_gnode(wa.min_target).s;
        ret.exclude_gnode = "null";
        if (wa.exclude_gnode != null) ret.exclude_gnode = tuple_gnode(wa.exclude_gnode).s;
        ret.non_participant_gnode = "null";
        if (wa.non_participant_gnode != null) ret.non_participant_gnode = tuple_gnode(wa.non_participant_gnode).s;
        ret.respondant_node = "null";
        if (wa.respondant_node != null) ret.respondant_node = tuple_node(wa.respondant_node).s;
        ret.response = "null";
        if (wa.response != null) ret.response = "something...";
        ret.refuse_message = "null";
        if (wa.refuse_message != null) ret.refuse_message = wa.refuse_message;
        ret.redo_from_start = wa.redo_from_start;
        return ret;
    }

}

