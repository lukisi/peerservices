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
                // If a gnode which is inside g is in the list, remove it.
                if (Utils.contains(g, e))
                {
                    _list.remove_at(i);
                    i--;
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
                if (Utils.contains(e, g)) return true;
            }
            return false;
        }
    }
}