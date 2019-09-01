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

    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String DATA_TYPE = "DATA_TYPE";

    private Connection conn;
    private StatementCache statementCache;
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

    // ============================================================
    // Constructors
    // ============================================================

    public Table( String fullTableName, Class<T> beanClass )
    {
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

    public void setConnection( Connection conn )
        throws SQLException
    {
        if ( statementCache != null )
        {
            statementCache.clear();
            statementCache = null;
        }

        this.conn = conn;
        if ( conn != null )
        {
            statementCache = new StatementCache();
        }
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
        PreparedStatement statement = statementCache.prepareStatement( conn, insertDef.getInsertSql() );

        setInsertValues( insertDef, 1, bean );
        int rows = statement.executeUpdate();
        retrieveGeneratedKeys( insertDef, bean );

        return rows;
    }

    /**
     * Insert multiple beans into the database at the same time using a fast mult-row insert statement.
     */
    public int insert( Collection<T> beans )
        throws SQLException
    {
        InsertDef insertDef = getInsertDef( null );
        insertDef.setRowCount( beans.size() );
        PreparedStatement statement = statementCache.prepareStatement( conn, insertDef.getInsertSql() );

        int paramIndex = 1;
        for ( T bean : beans )
        {
            paramIndex = setInsertValues( insertDef, paramIndex, bean );
        }

        int rows = statement.executeUpdate();
        // TODO -- not sure how to deal with generated keys in a multi-insert
        //        retrieveGeneratedKeys( insertDef, bean );

        return rows;
    }

    /**
     * Convert the bean into an update statement and execute it.
     */
    public int update( T bean )
        throws SQLException
    {
        PreparedStatement statement = statementCache.prepareStatement( conn, getUpdateSql() );
        setUpdateValues( statement, bean );

        return statement.executeUpdate();
    }

    /**
     * Convert the bean into an delete statement and execute it.
     */
    public boolean delete( T bean )
        throws SQLException
    {
        PreparedStatement statement = statementCache.prepareStatement( conn, getDeleteSql() );
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

            /* if ( value.getSqlType() == java.sql.Types.ARRAY )
            {
                if ( value.getValue() == null )
                {
                    statement.setNull( propIndex, value.getSqlType() );
                }
                else
                {
                    Array array = statement.getConnection().createArrayOf( getTypeName( value.getValue() ), (Object[]) value.getValue() );
                    statement.setArray( propIndex, array );
                }
            }
            else
            {
                statement.setObject( propIndex, value.getValue(), value.getSqlType() );
            }*/

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

    protected int setInsertValues( InsertDef insertDef, int paramIndex, T bean )
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
                insertDef.getStatement().setNull( paramIndex, sqlType );
            }
            else
            {
                Object coercedValue = coercer.convertTo( val, SqlTypeMapping.getJavaClass( sqlType ) );
                insertDef.getStatement().setObject( paramIndex, coercedValue, sqlType );

                /*if ( sqlType == Types.ARRAY )
                {
                    Array array = statement.getConnection().createArrayOf( getTypeName( val ), (Object[]) val );
                    statement.setArray( paramIndex, array );
                }
                // TODO
                else if ( val instanceof Instant )
                {
                    statement.setObject( paramIndex, Timestamp.from( (Instant) val ), sqlType );
                }
                else
                {
                    Object coercedValue = coercer.convertTo( val, SQLTypeMapping.getJavaClass( sqlType ) );
                    statement.setObject( paramIndex, coercedValue, sqlType );
                }*/
            }

            paramIndex++;
        }

        return paramIndex;
    }

    protected void retrieveGeneratedKeys( InsertDef insertDef, T bean )
        throws SQLException
    {
        if ( insertDef.getGeneratedKeys().isEmpty() )
        {
            return;
        }

        ResultSet rs = insertDef.getStatement().getGeneratedKeys();
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
        String realTableName = getRealTableName( tableName );

        //
        // If the table does not actually exist optionally throw exception.
        //
        if ( realTableName == null )
        {
            throw new FauxjoException( String.format( "Table %s does not exist.", fullTableName ) );
        }

        HashMap<String, ColumnInfo> map = new HashMap<>();

        ResultSet rs = conn.getMetaData().getColumns( null, schemaName, realTableName, null );
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
    private String getRealTableName( String tableName )
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

        rs = conn.getMetaData().getTables( null, schemaName, null, tableTypes.toArray( new String[tableTypes.size()] ) );

        while ( rs.next() )
        {
            if ( rs.getString( TABLE_NAME ).equalsIgnoreCase( tableName ) )
            {
                String name = rs.getString( TABLE_NAME );
                rs.close();
                return name;
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

            Field field = beanDef.getField( key );
            if ( field != null )
            {
                field.setAccessible( true );

                return field.get( bean );
            }

            Method readMethod = beanDef.getReadMethod( key );
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

        return null;
    }

    private void setBeanValue( T bean, String key, Object value )
        throws FauxjoException
    {
        try
        {
            BeanDef beanDef = BeanDefCache.getBeanDef( bean.getClass() );

            Field field = beanDef.getField( key );
            if ( field != null )
            {
                try
                {
                    field.setAccessible( true );
                    field.set( bean, value );

                    return;
                }
                catch ( Exception ex )
                {
                    throw new FauxjoException( "Unable to write to field [" + field.getName() + "]", ex );
                }
            }

            Method writeMethod = beanDef.getWriteMethod( key );
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
        }
        catch ( Exception ex )
        {
            if ( ex instanceof FauxjoException )
            {
                throw (FauxjoException) ex;
            }

            throw new FauxjoException( ex );
        }
    }

    //    private String getTypeName( Object val )
    //    {
    //        String typeName = "varchar";
    //        Class klass = val.getClass().getComponentType();
    //        if ( klass == UUID.class )
    //        {
    //            typeName = "uuid";
    //        }
    //        else if ( klass == Date.class || klass == java.util.Date.class )
    //        {
    //            typeName = "timestamptz";
    //        }
    //        else if ( klass == Integer.class )
    //        {
    //            typeName = "int";
    //        }
    //        else if ( klass == Double.class )
    //        {
    //            typeName = "float8";
    //        }
    //        else if ( klass == Boolean.class )
    //        {
    //            typeName = "boolean";
    //        }
    //
    //        return typeName;
    //    }

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
        private PreparedStatement statement;
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

        public Connection getConnection()
            throws SQLException
        {
            return statement.getConnection();
        }

        public PreparedStatement getStatement()
        {
            return statement;
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
}
