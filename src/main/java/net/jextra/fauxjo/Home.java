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
import java.time.format.*;
import java.util.*;

/**
 * Base implementation of a data access object.<p>
 *
 * If using a jdbc driver with Statement caching (e.g. PG, Oracle, MySQL) or for
 * fine-grain control over memory use, consider calling setStatementCacheEnabled(false)
 * and closing Connections and their Statements with try-resource or finally.
 * If using HikariCP, set these properties:<ul>
 * <li>minimumIdle: set to count of services concurrently using this Home X 2.
 * <li>maximumPoolSize: set at least 2X larger than minIdleCount or db process count.
 * <li>idleTimeout: 600000 (millisec), max time before HikariCP evicts idle connections.</ul><p>
 *
 * Hikari docs: <q>It is imperative the app configures driver-level TCP socket timeout.
 * For Postgresql (PG), set socketTimeout to greater of 2-3X the longest query or 30 sec.</q><p>
 *
 * PG docs: <q>If reading from the server takes longer than this value, the connection is closed.</q>
 * An easy way configure PG socketTimeout when using HikariCP is to use its idleTimeout (in seconds):
 * <pre>hikariConf.addDataSourceProperty("socketTimeout", idleTimeout/1000)</pre><p>
 *
 * Hikari recovery notes (after network outage or RDS failover):
 * Recovery time should take the greater of idleTimeout and socket timeout on the driver.
 * During recovery, nobody should call getConnection allowing the pool to evict idle connections.
 * A longer time is more tolerant of long-running queries but also prolongs recovery.
 * See <a href="https://github.com/brettwooldridge/HikariCP/wiki/Rapid-Recovery">Hikari rapid recovery</a>
 * <a href="https://jdbc.postgresql.org/documentation/94/connect.html">Hikari rapid recovery 2</a><p>
 * @see #setStatementCacheEnabled(boolean)
 */
public class Home<T> implements AutoCloseable
{
    // ============================================================
    // Fields
    // ============================================================

    private boolean supportsGeneratedKeys = true;
    private Table<T> table;
    private BeanBuilder<T> beanBuilder;

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

    /** * @see {@link Table#setStatementCacheEnabled(boolean)} */
    public Home<T> setStatementCacheEnabled( boolean enabled )
    {
        table.setStatementCacheEnabled( enabled );

        return this;
    }

    /** * @see {@link Table#getStatementCacheEnabled()} */
    public boolean getStatementCacheEnabled()
    {
        return table.getStatementCacheEnabled();
    }

    /** * @see {@link Table#setStatementCacheConfig(List, Integer, Long)} */
    public Home<T> setStatementCacheConfig( List<StatementCacheListener> listeners, Integer perConCacheMaxEntries, Long perConCacheMaxAgeMillis )
    {
        table.setStatementCacheConfig( listeners, perConCacheMaxEntries, perConCacheMaxAgeMillis );

        return this;
    }

    public Connection getConnection()
    {
        return table.getConnection();
    }

    /** * @see {@link Table#setConnection(Connection)} */
    public boolean setConnection( Connection conn )
        throws SQLException
    {
        return table.setConnection( conn );
    }

    @Override
    public void close()
        throws SQLException
    {
        table.close();
    }

    public Table getTable()
    {
        return table;
    }

    public BeanBuilder<T> getBeanBuilder()
    {
        return beanBuilder;
    }

    /** * @see {@link Table#prepareStatement(String)} */
    public PreparedStatement prepareStatement( String sql )
        throws SQLException
    {
        return table.prepareStatement( sql );
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
     * This method attaches the schema name to the front of the name passed in.<p>
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

    /** * @see {@link Table#getStatementCacheCsvForPrepStmts(StringBuilder)} */
    public void getStatementCacheCsvForPrepStmts( StringBuilder sb )
        throws Exception
    {
        table.getStatementCacheCsvForPrepStmts( sb );
    }

    /** * @see {@link Table#getStatementCacheStats(StringBuilder, DateTimeFormatter)} */
    public StringBuilder getStatementCacheStats( StringBuilder strBldrToAppend, DateTimeFormatter optionalDtFormat )
        throws SQLException
    {
        table.getStatementCacheStats( strBldrToAppend, optionalDtFormat );
        return strBldrToAppend;
    }

}
