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
import java.util.*;
import net.jextra.fauxjo.beandef.*;
import net.jextra.fauxjo.coercer.*;

/**
 * Java representation and helper methods for a database table.
 */
public class Table<T>
{
    // ============================================================
    // Fields
    // ============================================================

    private static final String SCHEMA_NAME = "TABLE_SCHEM";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String DATA_TYPE = "DATA_TYPE";

    private boolean supportsGeneratedKeys;
    private Connection conn;
    private Long connKey;
    private StatementCache statementCache;
    private boolean stmtCacheEnabled;
    private String fullTableName;
    private String schemaName;
    private String tableName;
    private Class<T> beanClass;
    private Coercer coercer;

    // Key = Lowercase column name (in source code, this is known as the "key").
    // Value = ColumnInfo object that specifies the type and real column name.
    private Map<String, ColumnInfo> columnInfos;

    private String updateSql;
    private String deleteSql;

    private List<StatementCacheListener> listeners;
    private Integer perConCache_MaxEntries;
    private Long perConCache_MaxTtl;

    // ============================================================
    // Constructors
    // ============================================================

    public Table( String fullTableName, Class<T> beanClass )
    {
        supportsGeneratedKeys = true;
        this.fullTableName = fullTableName;
        String[] words = fullTableName.toLowerCase().split( "\\." );
        if ( words.length == 1 )
        {
            this.schemaName = null;
            this.tableName = words[0];
        }
        else
        {
            this.schemaName = words[0];
            this.tableName = words[1];
        }

        this.beanClass = beanClass;
        coercer = new Coercer();
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

    public void setSupportsGeneratedKeys( boolean value )
    {
        this.supportsGeneratedKeys = value;
    }

    /**
     * Sets the StatementCache enabled (default) if true.<p>
     *
     * StatementCaching is enabled by default to avoid breaking changes but should
     * be disabled when using a modern jdbc driver. Per the Hikari author who has
     * deep domain expertise in statement caching, nobody can cache statements better
     * than the jdbc driver. Of the 5 most popular rdbms, only Microsoft SQLServer
     * jdbc driver does not fully support transparent, driver-side statement caching.
     * @param enabled should be set prior to setConnection
     * @see <a href="https://github.com/brettwooldridge/HikariCP/issues/488">https://github.com/brettwooldridge/HikariCP/issues/488</a>
     */
    public Table setStatementCacheEnabled(boolean enabled) {
        this.stmtCacheEnabled = enabled;
        return this;
    }

    /**
     * Sets configuration passed to StatementCache if it is or becomes enabled.<p>
     *
     * Enables logging and aids diagnosing affinity of cache, thread, connection and its sql statements.
     * @param listeners to receive key StatementCache events
     * @param perConCache_MaxEntries max Statements cached per Connection prevents sql concatenation leaks (default is 1000 entries)
     * @param perConCache_MaxTtl max time millis a Statement is cached per Connection prevents sql concatenation (default is 30 minutes)
     * @see #setConnection(Connection)
     */
    public Table setStatementCacheConfig(List<StatementCacheListener> listeners, Integer perConCache_MaxEntries, Long perConCache_MaxTtl)
    {
        this.listeners = listeners;
        this.perConCache_MaxEntries = perConCache_MaxEntries;
        this.perConCache_MaxTtl = perConCache_MaxTtl;
        if(statementCache != null)
        {
            if(listeners != null) for(StatementCacheListener l : listeners) statementCache.addListener( l );
            statementCache.setPerConCache_Maximums( perConCache_MaxEntries, perConCache_MaxTtl );
        }
        return this;
    }

    /**
     * Return true if stmtCache is enabled and the Connection is the same as last.<p>
     *
     * Otherwise, clears the StatementCache and creates a new StatementCache.
     * The internal connection reference is set for both cases because conn may be a
     * Connection facade such as if from HikariCP.getConnection.
     * net.jextra.fauxjo.StatementCache#getConnKey(Connection) is used to determine
     * if conn is the same as the last one set.
     * @param conn may be a real db Connection or one wrapped in a Connection facade
     * @see net.jextra.fauxjo.StatementCache#getConnKey(Connection)
     */
    public boolean setConnection( Connection conn )
        throws SQLException
    {
        if(!stmtCacheEnabled) {
            this.conn = conn; //Update conn in case it is a new connection wrapper.
            return false; //do not clear the statementCache.
        }
        Long connkey = StatementCache.getConnKey( conn );
        if(this.connKey != null && connkey.longValue() == this.connKey.longValue())
        {
            this.conn = conn; //Update conn in case it is a new connection wrapper.
            return true; //If same Connection reuse it. Do not clear the statementCache.
        }

        //Is a new unique Connection so release all resources
        if (statementCache != null )
        {
            statementCache.clear();
        }

        this.conn = conn;
        this.connKey = connkey;
        if (conn != null )
        {
            statementCache = new StatementCache().setCacheType(StatementCache.CacheType.Home);
            if(listeners != null) for(StatementCacheListener l : listeners) statementCache.addListener( l );
            statementCache.setPerConCache_Maximums( perConCache_MaxEntries, perConCache_MaxTtl );
        }
        return false;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getFullTableName()
    {
        return fullTableName;
    }

    public Coercer getCoercer()
    {
        return coercer;
    }

    public String buildBasicSelectStatement( String clause )
    {
        String trimmedClause = "";
        if ( clause != null && !clause.trim().isEmpty() )
        {
            trimmedClause = clause;
        }

        return String.format( "select * from %s %s", fullTableName, trimmedClause );
    }

    /**
     * Convert the bean into an insert statement and execute it.
     */
    public int insert( T bean )
        throws SQLException
    {
        InsertDef insertDef = getInsertDef( bean );
        PreparedStatement insStatement = null;
        if( stmtCacheEnabled && statementCache != null)
        {
            insStatement = statementCache.prepareStatement( conn, insertDef.getInsertSql(), supportsGeneratedKeys );
        }
        else if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( insertDef.getInsertSql() ) )
        {
            insStatement = conn.prepareStatement( insertDef.getInsertSql(), Statement.RETURN_GENERATED_KEYS );
        }
        else
        {
            insStatement = conn.prepareStatement( insertDef.getInsertSql() );
        }
        setInsertValues( insStatement, insertDef, 1, bean );
        int rows = insStatement.executeUpdate();
        retrieveGeneratedKeys( insStatement, insertDef, bean );

        return rows;
    }

    /**
     * Insert multiple beans into the database at the same time using a fast multi-row insert statement.
     */
    public int insert( Collection<T> beans )
        throws SQLException
    {
        if ( beans == null || beans.isEmpty() )
        {
            return 0;
        }

        InsertDef insertDef = getInsertDef( null );
        insertDef.setRowCount( beans.size() );
        PreparedStatement insStatement = null;
        if( stmtCacheEnabled && statementCache != null)
        {
            insStatement = statementCache.prepareStatement( conn, insertDef.getInsertSql(), supportsGeneratedKeys );
        }
        else if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( insertDef.getInsertSql() ) )
        {
            insStatement = conn.prepareStatement( insertDef.getInsertSql(), Statement.RETURN_GENERATED_KEYS );
        }
        else
        {
            insStatement = conn.prepareStatement( insertDef.getInsertSql() );
        }

        int paramIndex = 1;
        for ( T bean : beans )
        {
            paramIndex = setInsertValues( insStatement, insertDef, paramIndex, bean );
        }

        int rows = insStatement.executeUpdate();
        // TODO -- not sure how to deal with generated keys in a multi-insert
        //        retrieveGeneratedKeys( insertDef, bean );

        return rows;
    }

    public int[] insertBatch( Collection<T> beans )
        throws SQLException
    {
        if ( beans == null || beans.isEmpty() )
        {
            return new int[] {};
        }

        InsertDef insertDef = getInsertDef( null );
        PreparedStatement insStatement = null;
        if( stmtCacheEnabled && statementCache != null)
        {
            insStatement = statementCache.prepareStatement( conn, insertDef.getInsertSql(), supportsGeneratedKeys );
        }
        else if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( insertDef.getInsertSql() ) )
        {
            insStatement = conn.prepareStatement( insertDef.getInsertSql(), Statement.RETURN_GENERATED_KEYS );
        }
        else
        {
            insStatement = conn.prepareStatement( insertDef.getInsertSql() );
        }

        for ( T bean : beans )
        {
            setInsertValues( insStatement, insertDef, 1, bean );
            insStatement.addBatch();
        }

        int[] rows = insStatement.executeBatch();

        return rows;
    }

    /**
     * Convert the bean into an update statement and execute it.
     */
    public int update( T bean )
        throws SQLException
    {
        PreparedStatement statement = null;
        if( stmtCacheEnabled && statementCache != null) {
            statement = statementCache.prepareStatement( conn, getUpdateSql(), supportsGeneratedKeys );
        }
        else if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( getUpdateSql() ) )
        {
            statement = conn.prepareStatement( getUpdateSql(), Statement.RETURN_GENERATED_KEYS );
        }
        else
        {
            statement = conn.prepareStatement( getUpdateSql() );
        }
        setUpdateValues( statement, bean );

        return statement.executeUpdate();
    }

    /**
     * Convert the bean into an delete statement and execute it.
     */
    public boolean delete( T bean )
        throws SQLException
    {
        PreparedStatement statement = null;
        if( stmtCacheEnabled && statementCache != null) {
            statement = statementCache.prepareStatement( conn, getDeleteSql(), supportsGeneratedKeys );
        }
        else if ( supportsGeneratedKeys && SqlInspector.isInsertStatement( getDeleteSql() ) )
        {
            statement = conn.prepareStatement( getDeleteSql(), Statement.RETURN_GENERATED_KEYS );
        }
        else
        {
            statement = conn.prepareStatement( getDeleteSql() );
        }
        setDeleteValues( statement, bean );

        return statement.executeUpdate() > 0;
    }

    public String getUpdateSql()
        throws SQLException
    {
        if ( updateSql != null )
        {
            return updateSql;
        }

        StringBuilder setterClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();

        for ( String key : getColumnInfos().keySet() )
        {
            ColumnInfo columnInfo = getColumnInfos().get( key );

            FieldDef fieldDef = BeanDefCache.getFieldDefs( beanClass ).get( key );
            if ( fieldDef != null )
            {
                if ( fieldDef.isPrimaryKey() )
                {
                    if ( whereClause.length() > 0 )
                    {
                        whereClause.append( " and " );
                    }
                    whereClause.append( columnInfo.getRealName() );
                    whereClause.append( "=?" );
                }
                else
                {
                    if ( setterClause.length() > 0 )
                    {
                        setterClause.append( "," );
                    }
                    setterClause.append( columnInfo.getRealName() );
                    setterClause.append( "=?" );
                }
            }
        }

        if ( whereClause.length() == 0 )
        {
            throw new FauxjoException(
                "At least one field must be identified as a primary key in order to update rows in the table [" + fullTableName + "]" );
        }

        updateSql = String.format( "update %s set %s where %s", fullTableName, setterClause, whereClause );

        return updateSql;
    }

    public void setUpdateValues( PreparedStatement statement, T bean )
        throws SQLException
    {
        List<DataValue> values = new ArrayList<>();
        List<DataValue> keyValues = new ArrayList<>();

        Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
        for ( String key : getColumnInfos().keySet() )
        {
            ColumnInfo columnInfo = getColumnInfos().get( key );
            Object val = getFieldValueFromBean( bean, key, columnInfo );

            FieldDef fieldDef = beanFieldDefs.get( key );
            if ( fieldDef != null )
            {
                if ( fieldDef.isPrimaryKey() )
                {
                    keyValues.add( new DataValue( val, columnInfo.getSqlType() ) );
                }
                else
                {
                    values.add( new DataValue( val, columnInfo.getSqlType() ) );
                }
            }
        }

        int paramIndex = 1;
        for ( DataValue value : values )
        {
            Object coercedValue = coercer.convertTo( value.getValue(), SqlTypeMapping.getJavaClass( value.getSqlType() ) );
            statement.setObject( paramIndex, coercedValue, value.getSqlType() );

            paramIndex++;
        }

        for ( DataValue value : keyValues )
        {
            Object coercedValue = coercer.convertTo( value.getValue(), SqlTypeMapping.getJavaClass( value.getSqlType() ) );
            statement.setObject( paramIndex, coercedValue, value.getSqlType() );
            paramIndex++;
        }
    }

    public String getDeleteSql()
        throws SQLException
    {
        if ( deleteSql != null )
        {
            return deleteSql;
        }

        StringBuilder whereClause = new StringBuilder();

        Map<String, FieldDef> fieldDefs = BeanDefCache.getFieldDefs( beanClass );
        for ( String key : fieldDefs.keySet() )
        {
            FieldDef fieldDef = fieldDefs.get( key );
            if ( fieldDef == null || !fieldDef.isPrimaryKey() )
            {
                continue;
            }

            ColumnInfo columnInfo = getColumnInfos().get( key );

            if ( whereClause.length() > 0 )
            {
                whereClause.append( " and " );
            }
            whereClause.append( columnInfo.getRealName() );
            whereClause.append( "=?" );
        }

        if ( whereClause.length() == 0 )
        {
            throw new FauxjoException(
                "At least one field must be identified as a primary key in order to delete from the table [" + fullTableName + "]" );
        }

        deleteSql = String.format( "delete from %s where %s", fullTableName, whereClause );

        return deleteSql;
    }

    public void setDeleteValues( PreparedStatement statement, T bean )
        throws SQLException
    {
        List<DataValue> primaryKeyValues = new ArrayList<>();

        Map<String, FieldDef> fieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
        for ( String key : fieldDefs.keySet() )
        {
            FieldDef fieldDef = fieldDefs.get( key );
            if ( fieldDef == null || !fieldDef.isPrimaryKey() )
            {
                continue;
            }

            ColumnInfo columnInfo = getColumnInfos().get( key );
            Class<?> targetClass = SqlTypeMapping.getJavaClass( columnInfo.getSqlType() );

            Object val = readValue( bean, key );
            val = coercer.convertTo( val, targetClass );

            primaryKeyValues.add( new DataValue( val, columnInfo.getSqlType() ) );
        }

        int paramIndex = 1;
        for ( DataValue value : primaryKeyValues )
        {
            Object coercedValue = coercer.convertTo( value.getValue(), SqlTypeMapping.getJavaClass( value.getSqlType() ) );
            statement.setObject( paramIndex, coercedValue, value.getSqlType() );
            paramIndex++;
        }
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

    // ----------
    // protected
    // ----------

    /**
     * Optionally passing in a actual bean instant allows the insert statement to be exclude columns that can have defaulted values and are
     * also null in the bean.
     */
    protected InsertDef getInsertDef( T bean )
        throws SQLException
    {
        StringBuilder columns = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();

        Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( beanClass );
        Map<String, ColumnInfo> columnInfos = getColumnInfos();
        List<String> generatedColumns = new ArrayList<>();
        for ( String key : columnInfos.keySet() )
        {
            FieldDef fieldDef = beanFieldDefs.get( key );
            // If there is no field equivalent to the database column, ignore it.
            if ( fieldDef == null )
            {
                continue;
            }

            ColumnInfo columnInfo = columnInfos.get( key );
            boolean addColumn = true;

            // If the field is defaultable check to see if the value of the bean is indeed null and may need to be excluded.
            if ( bean != null && fieldDef.isDefaultable() )
            {
                Object value = readValue( bean, key );
                if ( value == null )
                {
                    generatedColumns.add( key );
                    addColumn = false;
                }
            }

            if ( addColumn )
            {
                if ( columns.length() > 0 )
                {
                    columns.append( "," );
                    questionMarks.append( "," );
                }

                columns.append( columnInfo.getRealName() );
                questionMarks.append( "?" );
            }
        }

        String insertSql = String.format( "insert into %s (%s) values ", fullTableName, columns );
        String valuesSql = String.format( "(%s)", questionMarks );

        return new InsertDef( insertSql, valuesSql, generatedColumns );
    }

    protected int setInsertValues( PreparedStatement insStatement, InsertDef insertDef, int paramIndex, T bean )
        throws SQLException
    {
        Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
        for ( String key : getColumnInfos().keySet() )
        {
            ColumnInfo columnInfo = getColumnInfos().get( key );
            FieldDef fieldDef = beanFieldDefs.get( key );
            if ( fieldDef == null )
            {
                continue;
            }

            Object val = getFieldValueFromBean( bean, key, columnInfo );

            // If the column was a generated column, a ? was not reserved for this column.
            if ( insertDef.getGeneratedKeys().contains( key ) )
            {
                continue;
            }

            int sqlType = columnInfo.getSqlType();

            if ( val == null )
            {
                insStatement.setNull( paramIndex, sqlType );
            }
            else
            {
                Object coercedValue = coercer.convertTo( val, SqlTypeMapping.getJavaClass( sqlType ) );
                insStatement.setObject( paramIndex, coercedValue, sqlType );
            }

            paramIndex++;
        }

        return paramIndex;
    }

    protected void retrieveGeneratedKeys( PreparedStatement insStatement, InsertDef insertDef, T bean )
        throws SQLException
    {
        if ( insertDef.getGeneratedKeys().isEmpty() )
        {
            return;
        }

        ResultSet rs = insStatement.getGeneratedKeys();
        if ( rs.next() )
        {
            Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
            for ( String key : insertDef.getGeneratedKeys() )
            {
                Object value = rs.getObject( key );
                if ( value != null )
                {
                    FieldDef fieldDef = beanFieldDefs.get( key );
                    value = coercer.convertTo( value, fieldDef.getValueClass() );
                }
                setBeanValue( bean, key, value );
            }
        }
        rs.close();
    }

    // ----------
    // private
    // ----------

    private Map<String, ColumnInfo> getColumnInfos()
        throws SQLException
    {
        if ( columnInfos == null )
        {
            cacheColumnInfos();
        }

        return columnInfos;
    }

    /**
     * This is a really slow method to call when it actually gets the meta data.
     */
    private void cacheColumnInfos()
        throws SQLException
    {
        RealTableName real = getRealTableName( tableName );

        //
        // If the table does not actually exist optionally throw exception.
        //
        if ( real == null )
        {
            throw new FauxjoException( String.format( "Table %s does not exist.", fullTableName ) );
        }

        HashMap<String, ColumnInfo> map = new HashMap<>();
        ResultSet rs = conn.getMetaData().getColumns( null, real.schemaName, real.tableName, null );
        while ( rs.next() )
        {
            String realName = rs.getString( COLUMN_NAME );
            Integer type = rs.getInt( DATA_TYPE );

            map.put( realName.toLowerCase(), new ColumnInfo( realName, type ) );
        }
        rs.close();

        // Only set field if all went well
        columnInfos = map;
    }

    /**
     * This takes a case insensitive tableName and searches for it in the connection's meta data to find the connections case sensitive tableName.
     */
    private RealTableName getRealTableName( String tableName )
        throws SQLException
    {
        ArrayList<String> tableTypes = new ArrayList<>();

        ResultSet rs = conn.getMetaData().getTableTypes();
        while ( rs.next() )
        {
            if ( rs.getString( 1 ).toLowerCase().contains( "table" ) )
            {
                tableTypes.add( rs.getString( 1 ) );
            }
        }
        rs.close();

        RealTableName bean = searchForTable( tableTypes, schemaName, tableName );
        if ( bean != null )
        {
            return bean;
        }

        if ( schemaName == null )
        {
            return null;
        }

        // Try schema all lowercase
        bean = searchForTable( tableTypes, schemaName.toLowerCase(), tableName );
        if ( bean != null )
        {
            return bean;
        }

        // Try schema all uppercase
        return searchForTable( tableTypes, schemaName.toUpperCase(), tableName );
    }

    private RealTableName searchForTable( List<String> tableTypes, String schemaName, String tableName )
        throws SQLException
    {
        ResultSet rs = conn.getMetaData().getTables( null, schemaName, null, tableTypes.toArray( new String[tableTypes.size()] ) );
        while ( rs.next() )
        {
            if ( rs.getString( TABLE_NAME ).equalsIgnoreCase( tableName ) )
            {
                RealTableName bean = new RealTableName();
                bean.schemaName = rs.getString( SCHEMA_NAME );
                bean.tableName = rs.getString( TABLE_NAME );
                rs.close();
                return bean;
            }
        }
        rs.close();

        return null;
    }

    private Object getFieldValueFromBean( Object bean, String key, ColumnInfo columnInfo )
        throws FauxjoException
    {
        Class<?> targetClass = SqlTypeMapping.getJavaClass( columnInfo.getSqlType() );

        Object val = readValue( bean, key );
        try
        {
            val = coercer.convertTo( val, targetClass );
        }
        catch ( FauxjoException ex )
        {
            throw new FauxjoException(
                "Failed to coerce " + fullTableName + "." + columnInfo.getRealName() + " for insert: " + key + ":" + columnInfo.getRealName(), ex );
        }

        return val;
    }

    private Object readValue( Object bean, String key )
        throws FauxjoException
    {
        try
        {
            BeanDef beanDef = BeanDefCache.getBeanDef( bean.getClass() );
            FieldDef fieldDef = beanDef.getFieldDef( key );
            if ( fieldDef == null )
            {
                throw new FauxjoException( "Unable to find FieldDef [" + key + "]" );
            }

            Field field = fieldDef.getField();
            if ( field != null )
            {
                field.setAccessible( true );

                return field.get( bean );
            }

            Method readMethod = fieldDef.getReadMethod();
            if ( readMethod != null )
            {
                return readMethod.invoke( bean );
            }
        }
        catch ( Exception ex )
        {
            if ( ex instanceof FauxjoException )
            {
                throw (FauxjoException) ex;
            }

            throw new FauxjoException( ex );
        }

        throw new FauxjoException( "Unable to find FieldDef [" + key + "]" );
    }

    private boolean setBeanValue( T bean, String key, Object value )
        throws FauxjoException
    {
        BeanDef beanDef = BeanDefCache.getBeanDef( bean.getClass() );
        FieldDef fieldDef = beanDef.getFieldDef( key );
        if ( fieldDef == null )
        {
            return false;
        }

        Field field = fieldDef.getField();
        if ( field != null )
        {
            try
            {
                field.setAccessible( true );
                field.set( bean, value );

                return true;
            }
            catch ( Exception ex )
            {
                throw new FauxjoException( "Unable to write to field [" + field.getName() + "]", ex );
            }
        }

        Method writeMethod = fieldDef.getWriteMethod();
        if ( writeMethod != null )
        {
            try
            {
                writeMethod.invoke( bean, value );
            }
            catch ( Exception ex )
            {
                throw new FauxjoException( "Unable to invoke write method [" + writeMethod.getName() + "]", ex );
            }
        }

        return true;
    }

    // ============================================================
    // Inner Classes
    // ============================================================

    public static class ColumnInfo
    {
        private String realName;
        private int sqlType;

        public ColumnInfo( String realName, int sqlType )
        {
            this.realName = realName;
            this.sqlType = sqlType;
        }

        public String getRealName()
        {
            return realName;
        }

        public void setRealName( String realName )
        {
            this.realName = realName;
        }

        public int getSqlType()
        {
            return sqlType;
        }

        public void setSqlType( int sqlType )
        {
            this.sqlType = sqlType;
        }
    }

    private class DataValue
    {
        private Object value;
        private int sqlType;

        public DataValue( Object value, int sqlType )
        {
            this.value = value;
            this.sqlType = sqlType;
        }

        public Object getValue()
        {
            return value;
        }

        public int getSqlType()
        {
            return sqlType;
        }
    }

    private class InsertDef
    {
        private String insertPart;
        private String valuesPart;
        // Number of questionmark sets to add to statement.
        private int rowCount;
        private Collection<String> generatedKeys;

        public InsertDef( String insertPart, String valuesPart, Collection<String> generatedKeys )
        {
            this.insertPart = insertPart;
            this.valuesPart = valuesPart;
            this.generatedKeys = generatedKeys;
            rowCount = 1;
        }

        public String getInsertPart()
        {
            return insertPart;
        }

        public void setInsertPart( String insertPart )
        {
            this.insertPart = insertPart;
        }

        public String getValuesPart()
        {
            return valuesPart;
        }

        public void setValuesPart( String valuesPart )
        {
            this.valuesPart = valuesPart;
        }

        public int getRowCount()
        {
            return rowCount;
        }

        public void setRowCount( int rowCount )
        {
            this.rowCount = rowCount;
        }

        public String getInsertSql()
        {
            return toString();
        }

        public Collection<String> getGeneratedKeys()
        {
            return generatedKeys;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder( insertPart );
            for ( int i = 0; i < rowCount; i++ )
            {
                builder.append( i == 0 ? "\n" : ",\n" );
                builder.append( valuesPart );
            }

            return builder.toString();
        }
    }

    private static class RealTableName
    {
        private String schemaName;
        private String tableName;
    }
}
