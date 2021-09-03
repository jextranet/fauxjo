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
 * Base implementation of a data access object.<p>
 *
 * If using a jdbc driver with Statement caching (e.g. PG, Oracle, MySQL), consider
 * calling setStatementCacheEnabled(false). If using HikariCP, set these properties:<ul>
 * <li>minimumIdle: set to count of services concurrently using this Home X 2.
 * <li>maximumPoolSize: set at least 2X larger than minIdleCount or db process count.
 * <li>idleTimeout: 600000 (millisec), max time before HikariCP evicts idle connections.</ul><p>
 *
 * Hikari docs: <q>It is imperative the app configures driver-level TCP socket timeout.
 * For Postgresql (PG), set socketTimeout to greater of 2-3X the longest query or 30 sec.</q><p>
 *
 * PG docs: <q>If reading from the server takes longer than this value, the connection is closed.</q>
 * An easy way configure PG socketTimeout when using HikariCP is to use Hikari idleTimeout (in seconds):
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
public class Home<T>
{
    // ============================================================
    // Fields
    // ============================================================

    private Connection conn;
    private Long connKey;
    private boolean supportsGeneratedKeys = true;
    private Table<T> table;
    private BeanBuilder<T> beanBuilder;
    private StatementCache statementCache;
    private List<StatementCacheListener> listeners;
    private Integer perConCache_MaxEntries;
    private Long perConCache_MaxTtl;
    private boolean stmtCacheEnabled;

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

    public void setSupportsGeneratedKeys( boolean supportsGeneratedKeys )
    {
        this.supportsGeneratedKeys = supportsGeneratedKeys;
        table.setSupportsGeneratedKeys( supportsGeneratedKeys );
    }

    /**
     * Sets the StatementCache enabled (default) if true.<p>
     *
     * StatementCaching is enabled by default to avoid breaking changes but should
     * be disabled when jdbc drivers that cache Statements. Per the Hikari author
     * who has expertise in statement caching, nobody can cache statements better
     * than the jdbc driver.
     * @param enabled should be set prior to setConnection
     * @see <a href="https://github.com/brettwooldridge/HikariCP/issues/488">https://github.com/brettwooldridge/HikariCP/issues/488</a>
     */
    public Home setStatementCacheEnabled(boolean enabled) {
        this.stmtCacheEnabled = enabled;
        return this;
    }

    /**
     * Sets configuration passed to StatementCache if it is or becomes enabled.<p>
     *
     * Enables logging and aids diagnosing affinity of cache, thread, connection and its sql statements.
     * The last two params guard against leaks caused by Prepared/CallableStatements
     * concatenating instead of using parameters.
     * @param listeners (optional) will be notified of key StatementCache events such as for logging
     * @param perConCache_MaxEntries (optional) the LRU Statement will be evicted after this many entries (1000 default)
     * @param perConCache_MaxTtl (optional) the LRU Statement will be evicted after this (30 minutes default)
     * @see #setConnection(Connection)
     */
    public Home setStatementCacheConfig(List<StatementCacheListener> listeners, Integer perConCache_MaxEntries, Long perConCache_MaxTtl)
    {
        this.listeners = listeners;
        this.perConCache_MaxEntries = perConCache_MaxEntries;
        this.perConCache_MaxTtl = perConCache_MaxTtl;

        if(statementCache != null)
        {
            if(listeners != null) for(StatementCacheListener l : listeners) statementCache.addListener( l );
            statementCache.setPerConCache_Maximums( perConCache_MaxEntries, perConCache_MaxTtl );
        }
        if(table != null)
        {
            table.setStatementCacheConfig( listeners, perConCache_MaxEntries, perConCache_MaxTtl );
        }
        return this;
    }

    public Connection getConnection()
    {
        return conn;
    }

    /**
     * Return true if stmtCache is enabled and the Connection is the same as last.<p>
     *
     * Otherwise, clears the StatementCache and passes the Connection to the Table
     * instance and then creates a new StatementCache. The internal connection
     * reference is set for both cases because Connection may be a new wrapped-Connection
     * such as if from HikariCP.getConnection.
     * net.jextra.fauxjo.StatementCache#getConnKey(Connection) is used to determine
     * if conn is the same as the last one set.
     * @param conn may be a real db Connection or one wrapped in a Connection facade
     * @see net.jextra.fauxjo.StatementCache#getConnKey(Connection)
     */
    public boolean setConnection( Connection conn )
        throws SQLException
    {
        if(!stmtCacheEnabled)
        {
            this.conn = conn; //Update conn in case it is a new connection wrapper.
            table.setConnection( conn );
            return false; //do not clear the statementCache.
        }
        Long connkey = StatementCache.getConnKey( conn );
        if(this.connKey != null && connkey.longValue() == this.connKey.longValue())
        {
            this.conn = conn; //Update conn in case it is a new connection wrapper.
            table.setConnection( conn );
            return true; //If same Connection reuse it. Do not clear the statementCache.
        }

        //Is a new unique Connection so release all resources
        if (statementCache != null )
        {
            statementCache.clear();
        }

        this.conn = conn;
        this.connKey = connkey;
        table.setConnection( conn );
        if (conn != null )
        {
            statementCache = new StatementCache().setCacheType(StatementCache.CacheType.Home);
            if(listeners != null) for(StatementCacheListener l : listeners) statementCache.addListener( l );
            statementCache.setPerConCache_Maximums( perConCache_MaxEntries, perConCache_MaxTtl );
        }
        return false;
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
        if( stmtCacheEnabled  && statementCache != null)
        {
            return statementCache.prepareStatement( conn, sql, supportsGeneratedKeys );
        }
        else if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( sql ) )
        {
            return conn.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS );
        }
        else
        {
            return conn.prepareStatement( sql );
        }
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

    /**
     * Append the contents of the StatementCache for the current Connection.
     * @param sb will have cache entries appended
     */
    public void getStatementCacheCsvForPrepStmts(StringBuilder sb)
        throws Exception
    {
        if( stmtCacheEnabled && statementCache != null)
        {
            statementCache.getDiagnosticCsv( conn, sb );
        }
        throw new SQLException("stmtCacheEnabled is false");
    }

}
