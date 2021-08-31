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
 * Base implementation of a data access object.
 */
public class Home<T>
{
    // ============================================================
    // Fields
    // ============================================================

    private Connection conn;
    private boolean supportsGeneratedKeys = true;
    private Table<T> table;
    private BeanBuilder<T> beanBuilder;
    private boolean usePreparedStatementCache = true;
    private StatementCache statementCache;

    // ============================================================
    // Constructors
    // ============================================================

    public Home( String tableName, Class<T> beanClass )
    {
        table = new Table<>( tableName, beanClass );
        beanBuilder = new BeanBuilder<>( beanClass );
        beanBuilder.setAutoCloseResultSet( true );
    }

    public Home( Connection conn, String tableName, Class<T> beanClass )
        throws SQLException
    {
        table = new Table<>( tableName, beanClass );
        beanBuilder = new BeanBuilder<>( beanClass );
        beanBuilder.setAutoCloseResultSet( true );
        setConnection( conn );
    }

    public Home( Table<T> table, BeanBuilder<T> beanBuilder )
    {
        this.table = table;
        this.beanBuilder = beanBuilder;
        beanBuilder.setAutoCloseResultSet( true );
    }

    public Home( Connection conn, Table<T> table, BeanBuilder<T> beanBuilder )
        throws SQLException
    {
        this.table = table;
        this.beanBuilder = beanBuilder;
        beanBuilder.setAutoCloseResultSet( true );
        setConnection( conn );
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public boolean getUsePreparedStatementCache()
    {
        return usePreparedStatementCache;
    }

    public Home<T> setUsePreparedStatementCache( boolean value )
    {
        usePreparedStatementCache = value;

        return this;
    }

    public boolean getSupportsGeneratedKeys()
    {
        return supportsGeneratedKeys;
    }

    public Home<T> setSupportsGeneratedKeys( boolean supportsGeneratedKeys )
    {
        this.supportsGeneratedKeys = supportsGeneratedKeys;
        table.setSupportsGeneratedKeys( supportsGeneratedKeys );

        return this;
    }

    public Connection getConnection()
    {
        return conn;
    }

    public void setConnection( Connection conn )
        throws SQLException
    {
        if ( statementCache != null )
        {
            statementCache.clear();
            statementCache = null;
        }

        this.conn = conn;
        table.setConnection( conn );
        if ( conn != null )
        {
            statementCache = new StatementCache();
        }
    }

    public Table getTable()
    {
        return table;
    }

    public BeanBuilder<T> getBeanBuilder()
    {
        return beanBuilder;
    }

    public PreparedStatement prepareStatement( String sql )
        throws SQLException
    {
        if ( !usePreparedStatementCache )
        {
            PreparedStatement statement = null;
            if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( sql ) )
            {
                statement = conn.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS );
            }
            else
            {
                statement = conn.prepareStatement( sql );
            }

            return statement;
        }

        return statementCache.prepareStatement( conn, sql, supportsGeneratedKeys );
    }

    public String getSchemaName()
    {
        return table.getSchemaName();
    }

    public String getTableName()
    {
        return table.getTableName();
    }

    public String getFullTableName()
    {
        return table.getFullTableName();
    }

    /**
     * This method attaches the schema name to the front of the name passed in.
     *
     * @return String that represents the given short name.
     */
    public String getQualifiedName( String name )
    {
        if ( table.getSchemaName() == null || table.getSchemaName().equals( "" ) )
        {
            return name;
        }
        else
        {
            return table.getSchemaName() + "." + name;
        }
    }

    public int insert( T bean )
        throws SQLException
    {
        return table.insert( bean );
    }

    public int insert( Collection<T> beans )
        throws SQLException
    {
        return table.insert( beans );
    }

    public int update( T bean )
        throws SQLException
    {
        return table.update( bean );
    }

    public boolean delete( T bean )
        throws SQLException
    {
        return table.delete( bean );
    }

    public String buildBasicSelect( String clause )
    {
        return table.buildBasicSelectStatement( clause );
    }

    public T getFirst( ResultSet rs )
        throws SQLException
    {
        return beanBuilder.getFirst( rs );
    }

    public T getFirst( ResultSet rs, boolean errorIfEmpty )
        throws SQLException
    {
        return beanBuilder.getFirst( rs, errorIfEmpty );
    }

    public T getUnique( ResultSet rs )
        throws SQLException
    {
        return beanBuilder.getUnique( rs );
    }

    public T getUnique( ResultSet rs, boolean errorIfEmpty )
        throws SQLException
    {
        return beanBuilder.getUnique( rs, errorIfEmpty );
    }

    public List<T> getList( ResultSet rs )
        throws SQLException
    {
        return beanBuilder.getList( rs );
    }

    public List<T> getList( ResultSet rs, int maxNumRows )
        throws SQLException
    {
        return beanBuilder.getList( rs, maxNumRows );
    }

    public Set<T> getSet( ResultSet rs )
        throws SQLException
    {
        return beanBuilder.getSet( rs );
    }

    public Set<T> getSet( ResultSet rs, int maxNumRows )
        throws SQLException
    {
        return beanBuilder.getSet( rs, maxNumRows );
    }

    public ResultSetIterator<T> getIterator( ResultSet rs )
        throws SQLException
    {
        return beanBuilder.getIterator( rs );
    }
}
