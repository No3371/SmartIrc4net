/*
 * $Id: IrcUser.cs 198 2005-06-08 16:50:11Z meebey $
 * $URL: svn+ssh://svn.qnetp.net/svn/smartirc/SmartIrc4net/trunk/src/IrcClient/IrcUser.cs $
 * $Rev: 198 $
 * $Author: meebey $
 * $Date: 2005-06-08 18:50:11 +0200 (Wed, 08 Jun 2005) $
 *
 * SmartIrc4net - the IRC library for .NET/C# <http://smartirc4net.sf.net>
 *
 * Copyright (c) 2008 Mirco Bauer <meebey@meebey.net> <http://www.meebey.net>
 * 
 * Full LGPL License: <http://www.gnu.org/licenses/lgpl.txt>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */


using System;

namespace Meebey.SmartIrc4net
{
    public class WhoIsUserInfo
    {
        public string Ident { get; private set; }
        public string Host { get; private set; }
        public string Realname { get; private set; }
        public string Nick { get; private set; }

        private WhoIsUserInfo() {}
        
        public static WhoIsUserInfo Parse(IrcMessageData data)
        {
            WhoIsUserInfo info = new WhoIsUserInfo();
            info.Nick  = data.RawMessageArray[3];
            info.Ident    = data.RawMessageArray[4];
            info.Host     = data.RawMessageArray[5];
            info.Realname     = data.RawMessageArray[7];            
            return info;
        }
    }
}
