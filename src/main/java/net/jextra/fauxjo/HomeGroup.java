/*
 * Copyright (C) jextra.net.
 *
 * This file is part of the jextra.net software.
 *
 * The jextra software is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The jextra software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the jextra software; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA.
 */

package net.jextra.fauxjo;

import java.sql.*;
import java.util.*;

/**
 * Groups a number of Home objects together into to be used with a common Connection.
 */
public class HomeGroup implements AutoCloseable
{
    // ============================================================
    // Fields
    // ============================================================

    private Map<Class<?>, Home<?>> homes;
    private Connection conn;

    // ============================================================
    // Constructors
    // ============================================================

    public HomeGroup()
    {
        homes = new LinkedHashMap<>();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public void addHome( Class<?> homeClass, Home<?> home )
    {
        homes.put( homeClass, home );
    }

    public <T> T getHome( Class<T> homeClass )
    {
        return homeClass.cast( homes.get( homeClass ) );
    }

    public Connection getConnection()
    {
        return conn;
    }

    public void setConnection( Connection conn )
        throws SQLException
    {
        this.conn = conn;
        for ( Home<?> home : homes.values() )
        {
            home.setConnection( conn );
        }
    }

    @Override
    public void close()
        throws SQLException
    {
        if ( conn != null )
        {
            conn.close();
        }
    }

    public Collection<Home<?>> getHomes()
    {
        return homes.values();
    }
}
