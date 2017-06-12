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
using Netsukuku.PeerServices;

namespace Netsukuku.PeerServices.MessageRouting
{
    /* GnodeExists: Determine if a certain g-node exists in the network.
     */
    internal delegate bool GnodeExists(int lvl, int pos);

    internal class MessageRouting : Object
    {
        private int levels;
        private ArrayList<int> pos;
        private ArrayList<int> gsizes;
        private GnodeExists gnode_exists;

        public MessageRouting
        (Gee.List<int> pos,
         Gee.List<int> gsizes,
         owned GnodeExists gnode_exists
         )
        {
            this.pos = new ArrayList<int>();
            this.pos.add_all(pos);
            this.gsizes = new ArrayList<int>();
            this.gsizes.add_all(gsizes);
            assert(gsizes.size == pos.size);
            this.levels = pos.size;
            this.gnode_exists = (owned) gnode_exists;
        }

        public int dist(PeerTupleNode x_macron, PeerTupleNode x)
        {
            assert(x_macron.tuple.size == x.tuple.size);
            int distance = 0;
            for (int j = x.tuple.size-1; j >= 0; j--)
            {
                distance *= gsizes[j];
                distance += x.tuple[j] - x_macron.tuple[j];
                if (x_macron.tuple[j] > x.tuple[j])
                    distance += gsizes[j];
            }
            return distance;
        }

        internal HCoord? approximate(PeerTupleNode? x_macron,
                                     Gee.List<HCoord> _exclude_list)
        {
            // Make sure that exclude_list is searchable
            Gee.List<HCoord> exclude_list = new ArrayList<HCoord>((a, b) => a.equals(b));
            exclude_list.add_all(_exclude_list);
            // This function is x=Ht(x̄)
            if (x_macron == null)
            {
                // It's me or nobody
                HCoord x = new HCoord(0, pos[0]);
                if (!(x in exclude_list))
                    return x;
                else
                    return null;
            }
            // The search can be restricted to a certain g-node
            int valid_levels = x_macron.tuple.size;
            assert (valid_levels <= levels);
            HCoord? ret = null;
            int min_distance = -1;
            // for each known g-node other than me
            for (int l = 0; l < valid_levels; l++)
                for (int p = 0; p < gsizes[l]; p++)
                if (pos[l] != p)
                if (gnode_exists(l, p))
            {
                HCoord x = new HCoord(l, p);
                if (!(x in exclude_list))
                {
                    PeerTupleNode tuple_x = Utils.make_tuple_node(pos, x, valid_levels);
                    int distance = dist(x_macron, tuple_x);
                    if (min_distance == -1 || distance < min_distance)
                    {
                        ret = x;
                        min_distance = distance;
                    }
                }
            }
            // and then for me
            HCoord x = new HCoord(0, pos[0]);
            if (!(x in exclude_list))
            {
                PeerTupleNode tuple_x = Utils.make_tuple_node(pos, x, valid_levels);
                int distance = dist(x_macron, tuple_x);
                if (min_distance == -1 || distance < min_distance)
                {
                    ret = x;
                    min_distance = distance;
                }
            }
            // If null yet, then nobody participates.
            return ret;
        }
    }
}