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
}