package net.jextra.fauxjo;

/**
 * Receive events related to caching PreparedStatements or CallableStatements.
 * Used to log and diagnose affinity of the caches to its threads, connections and statements.
 * <ul>
 * <li>A new ThreadLocal StmtCacheMap is created when a prepare/callStatement request is from a new Thread.</li>
 * <li>A new StmtCache (StatementCache.PerConnectionCache) is created and cached in the StmtCacheMap
 *     by its Connection when the prepare/callStatement request encounters a new Connection.</li>
 * <li>The least recently used Prepared/CallableStatement will be evicted from the StmtCache if expired or StmtCache
 *     fills up to guard against Prepared/CallableStatements with concatenated criteria instead of using parameters.</li>
 * </ul>
 */
public interface StatementCacheListener
{

    public enum StmtType
    {
        Prepared,
        Callable
    }

    public enum EvictType
    {
        MaxTtl,
        MaxEntries
    }

    /** Per prepare/callStatement, the eldest Statement is evicted if cache has this many entries. */
    public void setStmtCacheMaxEntries( StatementCache.Config ctype, Thread t, Integer maxStmts );

    /** Per prepare/callStatement call, the eldest Statement is evicted if older than this value. */
    public void setStmtCacheMaxTtl( StatementCache.Config ctype, Thread t, Long maxTTL );

    /** * The least recently used statement was evicted, StmtCache would have exceeded maxEntries. */
    public void evictedLruStmt_MaxEntries( StatementCache.Config ctype, StmtType type, String sql, long createdOnTs, long accessedCn,
        long maxCacheEntries );

    /** * The least recently used statement was evicted, it exceeded its maxTtl. */
    public void evictedLruStmt_MaxTtl( StatementCache.Config ctype, StmtType type, String sql, long createdOnTs, long accessedCn, long ageMillisMax );

    /** * An exception was thrown while trying to close an evicted Statement. */
    public void evictLruStmtException( StatementCache.Config ctype, StmtType type, Throwable throwable );

    /** * A new Thread was encountered resulting in a new ThreadLocal StmtCacheMap. */
    public void newStmtCacheMapForNewThread( StatementCache.Config ctype, Thread t );

    /** * An uncached Connection from prepare/callStatement request created a new StmtCache in StmtCacheMap. */
    public void newStmtCacheForNewConn( StatementCache.Config ctype, Thread t, Long dbConKey, String stmtSql );

    /** * StatementCache.clear was called so its listeners, StmtCacheMap and StmtCaches have been released. */
    public void clearedStmtCacheMapForThread( StatementCache.Config ctype, Thread t );

    /** * StatementCache.clear(Connection) was called so its StmtCache was released. */
    public void clearedStmtCacheForConn( StatementCache.Config ctype, Thread t, Long dbConKey );

    public void preparedStmt( StatementCache.Config ctype, Thread t, Long dbConKey );

    public void preparedCall( StatementCache.Config ctype, Thread t, Long dbConKey );

    public void reusedStmt( StatementCache.Config ctype, Thread t, Long dbConKey );

}
