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

namespace Netsukuku.PeerServices
{
    internal class PeerTupleGNodeContainer : Object
    {
        private ArrayList<PeerTupleGNode> _list;
        public Gee.List<PeerTupleGNode> list {
            owned get {
                return _list.read_only_view;
            }
        }
        public int top {get; private set;}

        public PeerTupleGNodeContainer(int top)
        {
            this.top = top;
            _list = new ArrayList<PeerTupleGNode>();
        }

        private int get_rev_pos(PeerTupleGNode g, int j)
        {
            // g.tuple has positions for levels from ε >= 0 to g.top-1.
            //  e.g.:
            //     g.top = 5
            //       that is, the g-node h that we live inside is at level 5.
            //     g.tuple.size = 3
            //     g.tuple = [2,1,3]
            //       that is g is g-node 3.1.2 inside h
            //       epsilon is 2, position for 2 is 2, position for 3 is 1, position for 4 is 3
            // get position for top-1-j.
            return g.tuple[g.tuple.size-1 - j];
        }

        public void add(PeerTupleGNode g)
        {
            assert(g.top == top);
            // If g is already contained in the list, return.
            if (contains(g)) return;
            // Cycle the list:
            int i = 0;
            while (i < _list.size)
            {
                PeerTupleGNode e = _list[i];
                assert(e.top == g.top);
                if (e.tuple.size > g.tuple.size)
                {
                    // If a gnode which is inside g is in the list, remove it.
                    bool mismatch = false;
                    for (int j = 0; j < g.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(g,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        _list.remove_at(i);
                        i--;
                    }
                }
                i++;
            }
            // Then add g.
            _list.add(g);
        }

        private bool contains(PeerTupleGNode g)
        {
            // Cycle the list:
            foreach (PeerTupleGNode e in _list)
            {
                assert(e.top == g.top);
                if (e.tuple.size <= g.tuple.size)
                {
                    // If g is already in the list, return true.
                    bool mismatch = false;
                    for (int j = 0; j < e.tuple.size; j++)
                    {
                        if (get_rev_pos(e,j) != get_rev_pos(g,j))
                        {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}