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

import java.lang.reflect.*;
import java.sql.*;
import java.text.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Caches PreparedStatements and CallableStatements per Connection, local to the Thread.<p>
 *
 * If using a modern jdbc driver that caches Statements (PG, Oracle, or MySQL),
 * consider disabling this via its Home object, drivers cache Statements better.
 *
 * Bottom-up, the containment hierarchy is as follows:<ul>
 * <li>A StmtCache (StatementCache.PerConnectionCache) is an LRU cache
 *     storing PreparedStatement or CallableStatements by their sql.</li>
 * <li>A StmtCacheMap is created if not exists from calls to prepareStatement
 *     or prepareCall and holds StmtCaches by their Connection key.</li>
 * <li>StmtCacheMap is a type of the static ThreadLocal created at startup
 *     so each thread gets its own StmtCacheMap.</li>
 * </ul>
 *
 * StmtCache guards against ever-changing PreparedStatement sql (not using params) by
 * evicting the LRU entry per call if StmtCache is full or entry has expired.
 *
 * @see <a href="https://github.com/brettwooldridge/HikariCP/issues/488">Let drivers cache PreparedStatements</a>
 * @see StatementCacheListener
 * @see #getConnKey(Connection)
 */
public class StatementCache
{

    //public enum StmtCacheConfig { LRU };

    // ============================================================
    // Fields
    // ============================================================

    //Instead of Connection, use Connection.hashcode in a Long to support wrapped Connecctions.
    //Using a Long enables future uses where hash collisions could be possible.
    private static final ThreadLocal<Map<Long, PerConnectionCache>> cache = new ThreadLocal<>();
    private Config config = new Config();
    private List<StatementCacheListener> listeners = new ArrayList<>();

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public Config getConfig()
    {
        return config.copy();
    }

    /**
     * Return maximum time for a Statement to live in the Connection cache (default is 30 minutes).
     * Guard against Statements that concatenate instead of use params.
     */
    public long getPerConCacheMaxTtl()
    {
        return config.getStmtAgeMaxMs();
    }

    public void setPerConCacheMaxTtl( Long perConCacheMaxTtl )
    {
        if ( perConCacheMaxTtl != null )
            config.setStmtAgeMaxMs( perConCacheMaxTtl );
    }

    /**
     * Return max Statements (unique sql) allowed in the cache (default is 1000)
     * before evicting the LRU. Guards against Statements concatenating instead
     * of using params.
     */
    public long getPerConCache_MaxEntries()
    {
        return config.getMaxEntries();
    }

    public void setPerConCacheMaxEntries( Long perConCacheMaxEntries )
    {
        if ( perConCacheMaxEntries != null )
            config.setMaxEntries( perConCacheMaxEntries );
    }

    /**
     * Set perConCache maximums.
     * @param perConCacheMaxEntries stmt max Statements before evict LRU (default is 1000)
     * @param perConCacheMaxTtl stmt max time to live before evict LRU (default is 30 minutes)
     */
    public void setPerConCache_Maximums( Integer perConCacheMaxEntries, Long perConCacheMaxTtl )
    {
        if ( perConCacheMaxEntries != null )
        {
            config.setMaxEntries( perConCacheMaxEntries );
            for ( StatementCacheListener l : listeners )
                l.setStmtCacheMaxEntries( config.copy(), Thread.currentThread(), perConCacheMaxEntries );
        }
        if ( perConCacheMaxTtl != null )
        {
            config.setStmtAgeMaxMs( perConCacheMaxTtl );
            for ( StatementCacheListener l : listeners )
                l.setStmtCacheMaxTtl( config.copy(), Thread.currentThread(), perConCacheMaxTtl );
        }
    }

    public PreparedStatement prepareStatement( Connection conn, String sql, boolean supportsGeneratedKeys )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        PreparedStatement statement = cc.getPreparedStatement( sql );
        if ( statement == null || statement.isClosed() )
        {
            if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( sql ) )
            {
                statement = conn.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS );
            }
            else
            {
                statement = conn.prepareStatement( sql );
            }

            statement = cc.setPreparedStatement( sql, statement );
            for ( StatementCacheListener l : listeners )
                l.preparedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return statement;
    }

    public PreparedStatement prepareStatement( Connection conn, String sql, int resultSetType, int resultSetConcurrency )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        PreparedStatement statement = cc.getPreparedStatement( sql );
        if ( statement == null || statement.isClosed() )
        {
            statement = conn.prepareStatement( sql, resultSetType, resultSetConcurrency );

            statement = cc.setPreparedStatement( sql, statement );
            for ( StatementCacheListener l : listeners )
                l.preparedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return statement;
    }

    public PreparedStatement prepareStatement( Connection conn, String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        PreparedStatement statement = cc.getPreparedStatement( sql );
        if ( statement == null || statement.isClosed() )
        {
            statement = conn.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability );

            statement = cc.setPreparedStatement( sql, statement );
            for ( StatementCacheListener l : listeners )
                l.preparedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return statement;
    }

    public PreparedStatement prepareStatement( Connection conn, String sql, int autoGeneratedKeys )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        PreparedStatement statement = cc.getPreparedStatement( sql );
        if ( statement == null || statement.isClosed() )
        {
            statement = conn.prepareStatement( sql, autoGeneratedKeys );

            statement = cc.setPreparedStatement( sql, statement );
            for ( StatementCacheListener l : listeners )
                l.preparedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return statement;
    }

    public PreparedStatement prepareStatement( Connection conn, String sql, int[] columnIndexes )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        PreparedStatement statement = cc.getPreparedStatement( sql );
        if ( statement == null || statement.isClosed() )
        {
            statement = conn.prepareStatement( sql, columnIndexes );

            statement = cc.setPreparedStatement( sql, statement );
            for ( StatementCacheListener l : listeners )
                l.preparedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return statement;
    }

    public PreparedStatement prepareStatement( Connection conn, String sql, String[] columnNames )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        PreparedStatement statement = cc.getPreparedStatement( sql );
        if ( statement == null || statement.isClosed() )
        {
            statement = conn.prepareStatement( sql, columnNames );

            statement = cc.setPreparedStatement( sql, statement );
            for ( StatementCacheListener l : listeners )
                l.preparedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return statement;
    }

    public CallableStatement prepareCall( Connection conn, String sql )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        CallableStatement call = cc.getPreparedCall( sql );
        if ( call == null || call.isClosed() )
        {
            call = conn.prepareCall( sql );

            call = cc.setPreparedCall( sql, call );
            for ( StatementCacheListener l : listeners )
                l.preparedCall( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return call;
    }

    public CallableStatement prepareCall( Connection conn, String sql, int resultSetType, int resultSetConcurrenc )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        CallableStatement call = cc.getPreparedCall( sql );
        if ( call == null || call.isClosed() )
        {
            call = conn.prepareCall( sql );

            call = cc.setPreparedCall( sql, call );
            for ( StatementCacheListener l : listeners )
                l.preparedCall( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return call;
    }

    public CallableStatement prepareCall( Connection conn, String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability )
        throws SQLException
    {
        PerConnectionCache cc = getConnCache( conn, sql );
        CallableStatement call = cc.getPreparedCall( sql );
        if ( call == null || call.isClosed() )
        {
            call = conn.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability );

            call = cc.setPreparedCall( sql, call );
            for ( StatementCacheListener l : listeners )
                l.preparedCall( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }
        else
        {
            for ( StatementCacheListener l : listeners )
                l.reusedStmt( config.copy(), Thread.currentThread(), cc.getConnectionKey() );
        }

        return call;
    }

    public boolean clear( Connection conn )
        throws SQLException
    {
        if ( conn == null )
        {
            return false;
        }

        Map<Long, PerConnectionCache> map = cache.get();
        if ( map == null )
        {
            return false;
        }

        Long cnKy = StatementCache.getConnKey( conn );
        boolean workDone = false;
        PerConnectionCache cc = map.get( cnKy );
        if ( cc != null )
        {
            workDone = cc.clear();
            map.remove( cnKy );
            for ( StatementCacheListener l : listeners )
                l.clearedStmtCacheForConn( config.copy(), Thread.currentThread(), cnKy );
        }

        return workDone;
    }

    /**
     * Closes and removes all Listeners, PreparedStatements and PreparedCalls for the active Thread.<p>
     *
     * @return true if any actual work was done and false if there was nothing to remove (most likely already closed).
     */
    public boolean clear()
        throws SQLException
    {
        boolean workDone = false;

        Map<Long, PerConnectionCache> map = cache.get();
        if ( map != null )
        {
            for ( Long connK : map.keySet().toArray( new Long[1] ) ) //avoid concurrentModification via array
            {
                PerConnectionCache cc = map.get( connK );
                if ( cc != null && cc.clear() )
                {
                    for ( StatementCacheListener l : listeners )
                        l.clearedStmtCacheForConn( config.copy(), Thread.currentThread(), connK );
                    workDone = true;
                }
            }
        }
        cache.remove();
        for ( StatementCacheListener l : listeners )
            l.clearedStmtCacheMapForThread( config.copy(), Thread.currentThread() );
        listeners.clear();
        return workDone;
    }

    /* Enable logging and aid diagnosing affinity of cache, thread, connection and sql statements. */
    public void addListener( StatementCacheListener listener )
    {
        if ( listener != null )
            listeners.add( listener );
    }

    public boolean removeListener( StatementCacheListener listener )
    {
        return ( listener == null ? false : listeners.remove( listener ) );
    }

    public void removeAllListeners()
    {
        listeners.clear();
    }

    public StatementCacheListener[] getListeners()
    {
        return listeners.toArray( new StatementCacheListener[1] );
    }

    /** * Append stats as json params to strBldrToAppend.  */
    public void getStats( StringBuilder strBldrToAppend, DateTimeFormatter optionalDtFormat )
        throws SQLException
    {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits( 2 );
        Map<Long, PerConnectionCache> map = cache.get();
        if ( map != null )
        {
            PerConnectionCache perConCache = null;
            int conns = 0;
            int connsWithStmts = 0;
            int stmtsPerConnMax = 0;
            int stmtsTotal = 0;
            for ( Long cnKy : map.keySet() )
            {
                if ( cnKy == null || ( perConCache = map.get( cnKy ) ) == null )
                    continue;
                int connStmtCn = perConCache.getPreparedStatementStats( null, optionalDtFormat );
                if ( connStmtCn > 0 )
                    connsWithStmts++;
                stmtsPerConnMax = Math.max( stmtsPerConnMax, connStmtCn );
                stmtsTotal += connStmtCn;
                conns++; //do not show conns w/o prepStmts (pending WeakHashMap GC)
            }
            strBldrToAppend.append( "conns: " + connsWithStmts );
            strBldrToAppend.append( ", stmtsTotal: " + stmtsTotal );
            if ( connsWithStmts != stmtsTotal )
            {
                strBldrToAppend.append( ", stmtsPerConnMax: " + stmtsPerConnMax );
                if ( connsWithStmts > 0 )
                    strBldrToAppend.append( ", stmtsPerConnAvg: " + nf.format( (float) stmtsTotal / (float) connsWithStmts ) );
            }
        }
    }

    /**
     * If connection is cached, append its contents to the string builder as csv rows.<p>
     *
     * @param conn represented by the cache
     * @param sb will have cache entries appended
     */
    public void getDiagnosticCsv( Connection conn, StringBuilder sb )
        throws Exception
    {
        Long cnKy = StatementCache.getConnKey( conn );

        Map<Long, PerConnectionCache> map = cache.get();
        if ( map != null && map.get( cnKy ) != null )
        {
            map.get( cnKy ).getStatementCacheCsvForPrepStmts( sb );
        }
    }

    // ----------
    // private
    // ----------

    /*
     * If no cache exists for this thread, creates it. Unwraps the Connection and if not
     * in the map, puts it in a new PerConnectionCache keyed to the unwrapped connection.
     * Using an always-new wrapped connection from a pool as cache key will leak memory.
     * @param conn is a real database Connection or a Connection wrapping a real one such as from a pool.
     * @param sql to be used in the returned PerConnectionCache (but is not added by this).
     */
    private PerConnectionCache getConnCache( Connection conn, String sql )
        throws SQLException
    {
        Map<Long, PerConnectionCache> map = cache.get();

        if ( map == null )
        {
            map = new WeakHashMap<>();
            cache.set( map );
            for ( StatementCacheListener l : listeners )
                l.newStmtCacheMapForNewThread( config.copy(), Thread.currentThread() );
        }

        Long cnKy = StatementCache.getConnKey( conn );
        PerConnectionCache cc = map.get( cnKy );
        if ( cc == null )
        {
            cc = new PerConnectionCache( cnKy );
            map.put( cnKy, cc );
            for ( StatementCacheListener l : listeners )
                l.newStmtCacheForNewConn( config.copy(), Thread.currentThread(), cnKy, sql );
        }

        return cc;
    }

    /**
     * Return the Long key to be used for caching the Stmt.<p>
     *
     * conn.getMetaData().getConnection().hashCode() is used in case conn
     * is a new pooled wrapper connection. Using an always-new wrapped
     * connection from a pool like Hikari as a cache key will leak memory.
     * @param conn used to create its cache key
     */
    public static Long getConnKey( Connection conn )
        throws SQLException
    {
        if ( conn == null )
            throw new SQLException( "!getConnKey, db conn is null" );
        if ( conn.isClosed() )
            throw new SQLException( "!getConnKey, db conn is closed" );
        if ( conn.getMetaData() == null )
            throw new SQLException( "!getConnKey, db conn metadata null" );
        if ( conn.getMetaData().getURL() == null )
            throw new SQLException( "!getConnKey, db conn metadata URL is null" );
        long key = conn.getMetaData().getConnection().hashCode(); //Get real connection in case conn is a new pooled wrapper connection.

        return key;
    }

    // ============================================================
    // Inner Classes
    // ============================================================

    /**
     * Block from actually doing close.
     * Wrap ResultSet in a WrapperResultSet so that getStatement method can return proxy Statement vs default non-wrapped Statement.
     */
    public static class WrappedPreparedStatement implements InvocationHandler
    {
        private PreparedStatement statement;

        public WrappedPreparedStatement( PreparedStatement statement )
        {
            this.statement = statement;
        }

        @Override
        public Object invoke( Object obj, Method method, Object[] args )
            throws Throwable
        {
            if ( "close".equals( method.getName() ) )
            {
                return Void.TYPE;
            }
            else if ( "closeWrapped".equals( method.getName() ) )
            {
                statement.close();

                return Void.TYPE;
            }

            Object retObj = method.invoke( statement, args );

            if ( ResultSet.class.equals( method.getReturnType() ) )
            {
                retObj = Proxy.newProxyInstance( getClass().getClassLoader(), new Class[] { ResultSet.class },
                    new WrappedResultSet( (PreparedStatement) obj, (ResultSet) retObj ) );
            }

            return retObj;
        }
    }

    /**
     * Override getStatement to return proxied Statement vs real Statement.
     */
    public static class WrappedResultSet implements InvocationHandler
    {
        private PreparedStatement parent;
        private ResultSet rs;

        public WrappedResultSet( PreparedStatement parent, ResultSet rs )
        {
            this.parent = parent;
            this.rs = rs;
        }

        @Override
        public Object invoke( Object obj, Method method, Object[] args )
            throws Throwable
        {
            if ( "getStatement".equals( method.getName() ) && Statement.class.equals( method.getReturnType() ) )
            {
                return parent;
            }

            return method.invoke( rs, args );
        }
    }

    public interface ProxyCloser
    {
        void closeWrapped();
    }

    /* Thin wrapper with entryDate to determine age and expiry. */
    private class PerConnectionCacheEntry<T extends Statement>
    {
        private T statement;
        private long entryDate;
        private long accessCn = 1L;

        public PerConnectionCacheEntry( T statement )
        {
            entryDate = System.currentTimeMillis();
            this.statement = statement;
        }

        public T getStatement()
        {
            accessCn++;
            return statement;
        }

        public void setStatement( T statement )
        {
            this.statement = statement;
        }

        public long getEntryDate()
        {
            return entryDate;
        }

        public void setEntryDate( long entryDate )
        {
            this.entryDate = entryDate;
        }

        public long getAgeMillis( long now )
        {
            return now - entryDate;
        }

        public long getAccessCn()
        {
            return accessCn;
        }
    }

    private class PerConnectionCache
    {
        private LruPrepStmt preparedStatementsLruCache;
        private LruCallableStmt callableStatementsLruCache;
        private Long connectionKey;

        public PerConnectionCache( Long connectionKey )
        {
            this.connectionKey = connectionKey;
            preparedStatementsLruCache = new LruPrepStmt( 1 << 4, 0.75f, true )
            { //ctor 16, 0.75 are HashMap defaults, initialCapcity must be power of two

                @Override //LinkedHashMap removes the eldestEntry if it is expired or the LRU is full.
                protected boolean removeEldestEntry( Map.Entry<String, PerConnectionCacheEntry<PreparedStatement>> eldest )
                {
                    if ( size() > config.getMaxEntries() )
                        doRemoveLru( eldest, StatementCacheListener.EvictType.MaxEntries );
                    else if ( eldest.getValue().getAgeMillis( System.currentTimeMillis() ) > config.getStmtAgeMaxMs() )
                    {
                        doRemoveLru( eldest, StatementCacheListener.EvictType.MaxTtl );
                    }
                    return false;
                }
            };

            callableStatementsLruCache = new LruCallableStmt( 1 << 4, 0.75f, true )
            { //ctor 16, 0.75 are HashMap defaults, initialCapcity must be power of two

                @Override //LinkedHashMap removes the eldestEntry if it is expired or the LRU is full.
                protected boolean removeEldestEntry( Map.Entry<String, PerConnectionCacheEntry<CallableStatement>> eldest )
                {
                    if ( size() > config.getMaxEntries() )
                        doRemoveLru( eldest, StatementCacheListener.EvictType.MaxEntries );
                    else if ( eldest.getValue().getAgeMillis( System.currentTimeMillis() ) > config.getStmtAgeMaxMs() )
                    {
                        doRemoveLru( eldest, StatementCacheListener.EvictType.MaxTtl );
                    }
                    return false; //per the spec, this block handled the removal, do not return true
                }
            };
        }

        public Long getConnectionKey()
        {
            return connectionKey;
        }

        public PreparedStatement getPreparedStatement( String sql )
        {
            PerConnectionCacheEntry<PreparedStatement> pse = preparedStatementsLruCache.get( sql );
            return ( pse != null ? pse.getStatement() : null );
        }

        public PreparedStatement setPreparedStatement( String sql, PreparedStatement statement )
        {
            PreparedStatement proxyStatement = (PreparedStatement) Proxy.newProxyInstance( getClass().getClassLoader(),
                new Class[] { PreparedStatement.class, ProxyCloser.class }, new WrappedPreparedStatement( statement ) );

            preparedStatementsLruCache.put( sql, new PerConnectionCacheEntry<>( proxyStatement ) );

            return proxyStatement;
        }

        public CallableStatement getPreparedCall( String sql )
        {
            PerConnectionCacheEntry<CallableStatement> cse = callableStatementsLruCache.get( sql );
            return ( cse != null ? cse.getStatement() : null );
        }

        public CallableStatement setPreparedCall( String sql, CallableStatement statement )
        {
            CallableStatement proxyCall = (CallableStatement) Proxy.newProxyInstance( CallableStatement.class.getClassLoader(),
                new Class[] { CallableStatement.class, ProxyCloser.class }, new WrappedPreparedStatement( statement ) );

            callableStatementsLruCache.put( sql, new PerConnectionCacheEntry<>( proxyCall ) );

            return proxyCall;
        }

        public boolean clear()
            throws SQLException
        {
            boolean workDone = false;
            for ( PerConnectionCacheEntry<PreparedStatement> prepStmtEntry : preparedStatementsLruCache.values() )
            {
                if ( prepStmtEntry != null && prepStmtEntry.getStatement() != null && !prepStmtEntry.getStatement().isClosed() )
                {
                    ( (ProxyCloser) prepStmtEntry.getStatement() ).closeWrapped();
                    workDone = true;
                }
            }
            preparedStatementsLruCache.clear();

            for ( PerConnectionCacheEntry<CallableStatement> callableStmtEntry : callableStatementsLruCache.values() )
            {
                if ( callableStmtEntry == null )
                {
                    continue;
                }

                if ( !callableStmtEntry.getStatement().isClosed() )
                {
                    ( (ProxyCloser) callableStmtEntry.getStatement() ).closeWrapped();
                    workDone = true;
                }
            }
            callableStatementsLruCache.clear();

            return workDone;
        }

        public int getPreparedStatementEntryCn()
        {
            return preparedStatementsLruCache.size();
        }

        public int getCallableStatementEntryCn()
        {
            return callableStatementsLruCache.size();
        }

        private void getStatementCacheCsvForPrepStmts( StringBuilder sb )
            throws Exception
        {
            String[] keys = new String[1];
            SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
            preparedStatementsLruCache.keySet().toArray( keys ); //avoid concurrent modification
            PerConnectionCacheEntry<PreparedStatement> entry;
            sb.append( "accessedCn,created,sql" );
            for ( String k : keys )
            {
                if ( ( entry = preparedStatementsLruCache.get( k ) ) == null )
                    continue;
                sb.append( entry.getAccessCn() ).append( "," );
                sb.append( sdf.format( new java.util.Date( entry.getEntryDate() ) ) );
                sb.append( ",\"" ).append( k.replace( '\n', ' ' ) ).append( "\"\n" );
            }
        }

        private int getPreparedStatementStats( StringBuilder sb, DateTimeFormatter df )
        {
            String[] keys = new String[1];
            preparedStatementsLruCache.keySet().toArray( keys ); //avoid concurrent modification
            PerConnectionCacheEntry<PreparedStatement> entry;
            int cn = 0;
            Long entryDateMin = null;
            Long entryDateMax = null;
            for ( String k : keys )
            {
                if ( ( entry = preparedStatementsLruCache.get( k ) ) == null )
                    continue;
                if ( entryDateMin == null || entry.getEntryDate() < entryDateMin )
                    entryDateMin = entry.getEntryDate();
                if ( entryDateMax == null || entry.getEntryDate() > entryDateMax )
                    entryDateMax = entry.getEntryDate();
                cn++;
            }
            if ( sb != null )
            {
                sb.append( "cn: " ).append( cn );
                if ( cn == 1 )
                {
                    sb.append( ", entryDt: \"" ).append( df != null ? df.format( Instant.ofEpochMilli( entryDateMin ) ) : entryDateMin.toString() )
                        .append( "\"" );
                }
                else if ( cn > 1 && entryDateMin != null && entryDateMax != null )
                {
                    sb.append( ", entryDtMin: \"" ).append( df != null ? df.format( Instant.ofEpochMilli( entryDateMin ) ) : entryDateMin.toString() )
                        .append( "\"" );
                    sb.append( ", entryDtMax: \"" ).append( df != null ? df.format( Instant.ofEpochMilli( entryDateMax ) ) : entryDateMax.toString() )
                        .append( "\"" );
                }
            }
            return cn;
        }

    }

    private class LruPrepStmt extends LinkedHashMap<String, PerConnectionCacheEntry<PreparedStatement>>
    {
        /**
         * Constructs an empty {@code LinkedHashMap} instance with the
         * specified initial capacity, load factor and ordering mode.
         *
         * @param  initialCapacity the initial capacity
         * @param  loadFactor      the load factor
         * @param  accessOrder     the ordering mode - {@code true} for
         *         access-order, {@code false} for insertion-order
         * @throws IllegalArgumentException if the initial capacity is negative
         *         or the load factor is nonpositive
         */
        public LruPrepStmt( int initialCapacity, float loadFactor, boolean accessOrder )
        {
            super( initialCapacity, loadFactor, accessOrder );
        }

        /** * Remove the entry from the map and attempt to close its Statement. */
        public void doRemoveLru( Map.Entry<String, PerConnectionCacheEntry<PreparedStatement>> eldest, StatementCacheListener.EvictType evictType )
        {
            StatementCacheListener.StmtType stmtType = StatementCacheListener.StmtType.Prepared;
            remove( eldest );
            try
            {
                if ( eldest != null && eldest.getValue() != null && !eldest.getValue().getStatement().isClosed() )
                {
                    eldest.getValue().getStatement().close();
                }
                if ( evictType == StatementCacheListener.EvictType.MaxEntries )
                {
                    for ( StatementCacheListener l : listeners )
                    {
                        l.evictedLruStmt_MaxEntries( config.copy(), stmtType, eldest.getKey(), eldest.getValue().getEntryDate(),
                            eldest.getValue().getAccessCn(), config.getMaxEntries() );
                    }
                }
                else if ( evictType == StatementCacheListener.EvictType.MaxTtl )
                {
                    for ( StatementCacheListener l : listeners )
                    {
                        l.evictedLruStmt_MaxTtl( config.copy(), stmtType, eldest.getKey(), eldest.getValue().getEntryDate(),
                            eldest.getValue().getAccessCn(), config.getStmtAgeMaxMs() );
                    }
                }
            }
            catch ( SQLException x )
            {
                for ( StatementCacheListener l : listeners )
                    l.evictLruStmtException( config.copy(), stmtType, x );
            }
        }
    }

    private class LruCallableStmt extends LinkedHashMap<String, PerConnectionCacheEntry<CallableStatement>>
    {
        /**
         * Constructs an empty {@code LinkedHashMap} instance with the
         * specified initial capacity, load factor and ordering mode.
         *
         * @param  initialCapacity the initial capacity
         * @param  loadFactor      the load factor
         * @param  accessOrder     the ordering mode - {@code true} for
         *         access-order, {@code false} for insertion-order
         * @throws IllegalArgumentException if the initial capacity is negative
         *         or the load factor is nonpositive
         */
        public LruCallableStmt( int initialCapacity, float loadFactor, boolean accessOrder )
        {
            super( initialCapacity, loadFactor, accessOrder );
        }

        /** * Remove the entry from the map and attempt to close its Statement. */
        public void doRemoveLru( Map.Entry<String, PerConnectionCacheEntry<CallableStatement>> eldest, StatementCacheListener.EvictType evictType )
        {
            StatementCacheListener.StmtType stmtType = StatementCacheListener.StmtType.Callable;
            remove( eldest );
            try
            {
                if ( eldest != null && eldest.getValue() != null && !eldest.getValue().getStatement().isClosed() )
                {
                    eldest.getValue().getStatement().close();
                }
                if ( evictType == StatementCacheListener.EvictType.MaxEntries )
                {
                    for ( StatementCacheListener l : listeners )
                    {
                        l.evictedLruStmt_MaxEntries( config.copy(), stmtType, eldest.getKey(), eldest.getValue().getEntryDate(),
                            eldest.getValue().getAccessCn(), config.getMaxEntries() );
                    }
                }
                else if ( evictType == StatementCacheListener.EvictType.MaxTtl )
                {
                    for ( StatementCacheListener l : listeners )
                    {
                        l.evictedLruStmt_MaxTtl( config.copy(), stmtType, eldest.getKey(), eldest.getValue().getEntryDate(),
                            eldest.getValue().getAccessCn(), config.getStmtAgeMaxMs() );
                    }
                }
            }
            catch ( SQLException x )
            {
                for ( StatementCacheListener l : listeners )
                    l.evictLruStmtException( config.copy(), stmtType, x );
            }
        }
    }

    public class Config
    {
        private long maxEntries = 1000L; //guard against ever-changing prep/call statements
        private long stmtAgeMaxMs = TimeUnit.MINUTES.toMillis( 30 );

        public Config()
        {
        }

        public Config( Config o )
        {
            maxEntries = o.getMaxEntries();
            stmtAgeMaxMs = o.getStmtAgeMaxMs();
        }

        /** * Return the max number of Statements before evicting the LRU when a new one is added. */
        public long getMaxEntries()
        {
            return maxEntries;
        }

        public void setMaxEntries( long maxEntries )
        {
            this.maxEntries = maxEntries;
        }

        /** * Return the max age of an LRU Statement to be evicted when a new one is added. */
        public long getStmtAgeMaxMs()
        {
            return stmtAgeMaxMs;
        }

        public void setStmtAgeMaxMs( long stmtAgeMaxMs )
        {
            this.stmtAgeMaxMs = stmtAgeMaxMs;
        }

        @Override
        public String toString()
        {
            return "maxStmts: " + maxEntries + ", maxStmtAgeMs: " + stmtAgeMaxMs + '}';
        }

        public Config copy()
        {
            return new Config( this );
        }
    }

}

