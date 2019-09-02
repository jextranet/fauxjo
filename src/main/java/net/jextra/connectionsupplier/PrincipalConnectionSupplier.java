/*
 * Copyright (C) fauxjo.net.
 *
 * This file is part of the Fauxjo Library.
 *
 * The Fauxjo Library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The Fauxjo Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the Fauxjo Library; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA.
 */

package net.jextra.connectionsupplier;

import java.sql.*;
import java.util.*;
import javax.sql.*;

/**
 * Stored an independent {@link Connection} for each {@link Thread}.
 * <p>
 * Example setup:
 * <pre>
 * InitialContext cxt = new InitialContext();
 * DataSource ds = (DataSource) cxt.lookup( &quot;java:/comp/env/jdbc/xyz/abc&quot; );
 * connectionSupplier = new PrincipalConnectionSupplier( ds );
 * </pre>
 * Example cleanup:
 * <p>
 * <pre>
 * connectionSupplier.close();
 * </pre>
 */
public class PrincipalConnectionSupplier implements ConnectionSupplier
{
    // ============================================================
    // Fields
    // ============================================================

    private static final int TIMEOUT = 1000;

    private DataSource dataSource;
    private ThreadLocal<Connection> localConnection;
    private ThreadLocal<HashMap<String, PreparedStatement>> localPreparedStatements;
    private ThreadLocal<HashMap<String, CallableStatement>> localPreparedCalls;

    // ============================================================
    // Constructors
    // ============================================================

    public PrincipalConnectionSupplier()
    {
        localConnection = new ThreadLocal<>();
        localPreparedStatements = new ThreadLocal<>();
        localPreparedCalls = new ThreadLocal<>();
    }

    public PrincipalConnectionSupplier( DataSource ds )
        throws SQLException
    {
        this();
        setDataSource( ds );
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public DataSource getDataSource()
    {
        return dataSource;
    }

    public void setDataSource( DataSource dataSource )
        throws SQLException
    {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection()
        throws SQLException
    {
        Connection cnx;
        if ( localConnection.get() == null )
        {
            localConnection.set( getDataSource().getConnection() );
        }

        cnx = localConnection.get();

        return cnx;
    }

    @Override
    public PreparedStatement prepareStatement( String sql )
        throws SQLException
    {
        validateConnection();

        HashMap<String, PreparedStatement> map = localPreparedStatements.get();
        if ( map == null )
        {
            map = new HashMap<>();
            localPreparedStatements.set( map );
        }

        PreparedStatement statement = map.get( sql );

        if ( statement == null || statement.isClosed() )
        {
            if ( SQLInspector.isInsertStatement( sql ) )
            {
                statement = getConnection().prepareStatement( sql, Statement.RETURN_GENERATED_KEYS );
            }
            else
            {
                statement = getConnection().prepareStatement( sql );
            }

            map.put( sql, statement );
        }

        return statement;
    }

    @Override
    public PreparedStatement prepareCall( String sql )
        throws SQLException
    {
        validateConnection();

        HashMap<String, CallableStatement> map = localPreparedCalls.get();
        if ( map == null )
        {
            map = new HashMap<>();
            localPreparedCalls.set( map );
        }

        CallableStatement call = map.get( sql );

        if ( call == null || call.isClosed() )
        {
            call = getConnection().prepareCall( sql );

            map.put( sql, call );
        }

        return call;
    }

    @Override
    public boolean close()
        throws SQLException
    {
        HashMap<String, PreparedStatement> statementMap = localPreparedStatements.get();
        if ( statementMap != null )
        {
            for ( PreparedStatement statement : statementMap.values() )
            {
                if ( statement == null )
                {
                    continue;
                }

                if ( !statement.isClosed() )
                {
                    statement.close();
                }
            }

            localPreparedStatements.remove();
        }

        HashMap<String, CallableStatement> callMap = localPreparedCalls.get();
        if ( callMap != null )
        {
            for ( CallableStatement call : callMap.values() )
            {
                if ( call == null )
                {
                    continue;
                }

                if ( !call.isClosed() )
                {
                    call.close();
                }
            }

            localPreparedCalls.remove();
        }

        Connection cnx = localConnection.get();
        if ( cnx != null )
        {
            cnx.close();
            localConnection.remove();

            return true;
        }

        return false;
    }

    public void validateConnection()
        throws SQLException
    {
        Connection cnx = getConnection();

        if ( cnx != null && !cnx.isValid( TIMEOUT ) )
        {
            close();

            // Remove non-valid connection.
            localConnection.remove();
        }
    }
}
