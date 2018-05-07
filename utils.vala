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
using TaskletSystem;

namespace Netsukuku.PeerServices.Utils
{
    internal string string_pos(Gee.List<int> pos)
    {
        string ret = @"$(pos[0])";
        for (int i = 1; i < pos.size; i++) ret += @",$(pos[i])";
        return @"[$(ret)]";
    }

    internal PeerTupleNode make_tuple_node(Gee.List<int> pos, HCoord h, int top)
    {
        // Returns a PeerTupleNode that represents h inside our g-node of level top. Actually h could be a g-node
        //  but the resulting PeerTupleNode is to be used in method 'dist'. Values of positions for indexes less than
        //  h.lvl are not important, they just have to be in ranges, so we set to 0.
        assert(top > h.lvl);
        ArrayList<int> tuple = new ArrayList<int>();
        int i = top;
        while (i > 0)
        {
            i--;
            if (i > h.lvl)
                tuple.insert(0, pos[i]);
            else if (i == h.lvl)
                tuple.insert(0, h.pos);
            else
                tuple.insert(0, 0);
        }
        return new PeerTupleNode(tuple);
    }

    internal PeerTupleGNode make_tuple_gnode(Gee.List<int> pos, HCoord h, int top)
    {
        // Returns a PeerTupleGNode that represents h inside our g-node of level top.
        assert(top > h.lvl);
        ArrayList<int> tuple = new ArrayList<int>();
        int i = top;
        while (true)
        {
            i--;
            if (i == h.lvl)
            {
                tuple.insert(0, h.pos);
                break;
            }
            else
            {
                tuple.insert(0, pos[i]);
            }
        }
        return new PeerTupleGNode(tuple, top);
    }

    internal void convert_tuple_gnode(Gee.List<int> pos, PeerTupleGNode t, out int @case, out HCoord ret)
    {
        /*
        Given t which represents a g-node h of level ε which lives inside one of my g-nodes,
        where ε = t.top - t.tuple.size,
        this methods returns the following informations:

        * int @case
           * Is 1 iff t represents one of my g-nodes.
           * Is 2 iff t represents a g-node visible in my topology.
           * Is 3 iff t represents a g-node not visible in my topology.
        * HCoord ret
           * The g-node in my map which h resides in.
           * In case 1  ret.lvl = ε. Also, pos[ret.lvl] = ret.pos.
           * In case 2  ret.lvl = ε. Also, pos[ret.lvl] ≠ ret.pos.
           * In case 3  ret.lvl > ε.
        */
        int lvl = t.top;
        int i = t.tuple.size;
        assert(i > 0);
        assert(i <= lvl);
        while (true)
        {
            lvl--;
            i--;
            if (pos[lvl] != t.tuple[i])
            {
                ret = new HCoord(lvl, t.tuple[i]);
                if (i == 0)
                    @case = 2;
                else
                    @case = 3;
                break;
            }
            if (i == 0)
            {
                ret = new HCoord(lvl, t.tuple[i]);
                @case = 1;
                break;
            }
        }
    }

    internal PeerTupleGNode tuple_node_to_tuple_gnode(PeerTupleNode t)
    {
        // Given t that represents a node, returns the same as
        //  a PeerTupleGNode with the same top.
        ArrayList<int> tuple0 = new ArrayList<int>();
        tuple0.add_all(t.tuple);
        return new PeerTupleGNode(tuple0, t.top);
    }

    internal PeerTupleGNode rebase_tuple_gnode(Gee.List<int> pos, PeerTupleGNode t, int new_top)
    {
        // Given t that represents a g-node inside my g-node at level t.top, returns
        //  a PeerTupleGNode with top=new_top that represents the same g-node.
        // Assert that t.top <= new_top.
        assert(t.top <= new_top);
        ArrayList<int> tuple = new ArrayList<int>();
        ArrayList<int> tuple0 = new ArrayList<int>();
        int i = new_top;
        while (true)
        {
            i--;
            if (i >= t.top)
                tuple.insert(0, pos[i]);
            else
            {
                tuple0.add_all(t.tuple);
                tuple0.add_all(tuple);
                break;
            }
        }
        return new PeerTupleGNode(tuple0, new_top);
    }

    internal PeerTupleGNode tuple_gnode_containing(Gee.List<int> pos, PeerTupleGNode t, int new_level)
    {
        // Given t that represents a g-node inside my g-node at level t.top, returns
        //  a PeerTupleGNode with same top that represents the g-node of level new_level that contains the g-node t.
        // Assert that t.top-t.tuple.size <= new_level.
        assert(t.top - t.tuple.size <= new_level);
        int remove = new_level - (t.top - t.tuple.size);
        Gee.List<int> tuple0 = t.tuple.slice(remove, t.tuple.size);
        return new PeerTupleGNode(tuple0, t.top);
    }

    internal PeerTupleNode rebase_tuple_node(Gee.List<int> pos, PeerTupleNode t, int new_top)
    {
        // Given t that represents a node inside my g-node at level t.top, returns
        //  a PeerTupleNode with top=new_top that represents the same node.
        // Assert that t.top <= new_top.
        assert(t.top <= new_top);
        ArrayList<int> tuple = new ArrayList<int>();
        ArrayList<int> tuple0 = new ArrayList<int>();
        int i = new_top;
        while (true)
        {
            i--;
            if (i >= t.top)
                tuple.insert(0, pos[i]);
            else
            {
                tuple0.add_all(t.tuple);
                tuple0.add_all(tuple);
                break;
            }
        }
        return new PeerTupleNode(tuple0);
    }

    internal bool visible_by_someone_inside_my_gnode(Gee.List<int> pos, PeerTupleGNode t, int lvl)
    {
        // Given a g-node decides if some node inside my g-node of level lvl has
        //  visibility of it.
        //  e.g. I am node 1.2.3.4.5.6
        //       if t represents 1.2.2.0
        //           could be tuple=0,2 top=4
        //           could be tuple=0,2,2 top=5
        //           could be tuple=0,2,2,1 top=6
        //       if lvl=6 answer is true
        //       if lvl=5 answer is true
        //       if lvl=4 answer is true
        //       if lvl=3 answer is false
        //  e.g. I am node 1.2.3.4.5.6
        //       if t represents 1.2.3.0
        //           could be tuple=0 top=3
        //           could be tuple=0,3 top=4
        //           could be tuple=0,3,2 top=5
        //           could be tuple=0,3,2,1 top=6
        //       for any lvl answer would be true
        int l = t.level;
        if (l >= lvl-1)
            l = l + 1;
        else
            l = lvl;
        if (t.top <= l)
            return true;
        PeerTupleGNode h = new PeerTupleGNode(t.tuple.slice(l-t.level, t.tuple.size), t.top);
        int @case;
        HCoord ret;
        convert_tuple_gnode(pos, h, out @case, out ret);
        if (@case == 1)
            return true;
        return false;
    }

    internal bool contains(PeerTupleGNode container, PeerTupleGNode contained)
    {
        // Returns True if <container> contains <contained>.
        // Requires that levels are the same.
        assert(container.top == contained.top);
        if (container.tuple.size <= contained.tuple.size)
        {
            for (int j = 0; j < container.tuple.size; j++)
            {
                int pos_container = container.tuple[container.tuple.size-1-j];
                int pos_contained = contained.tuple[contained.tuple.size-1-j];
                if (pos_container != pos_contained) return false;
            }
            return true;
        }
        return false;
    }
}
