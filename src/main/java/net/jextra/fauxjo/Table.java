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

package net.jextra.fauxjo;

import java.lang.reflect.*;
import java.sql.Array;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import net.jextra.connectionsupplier.*;
import net.jextra.fauxjo.beandef.*;
import net.jextra.fauxjo.coercer.*;

/**
 * Java representation of a database table.
 */
public class Table<T>
{
    // ============================================================
    // Fields
    // ============================================================

    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String DATA_TYPE = "DATA_TYPE";

    private String fullTableName;
    private String schemaName;
    private String tableName;
    private Class<T> beanClass;
    private Coercer coercer;

    // Key = Lowercase column name (in source code, this is known as the "key").
    // Value = ColumnInfo object that specifies the type and real column name.
    private Map<String, ColumnInfo> dbColumnInfos;

    private String updateSql;
    private String deleteSql;
    private String[] generatedColumns;

    // ============================================================
    // Constructors
    // ============================================================

    public Table( String fullTableName, Class<T> beanClass )
    {
        this.fullTableName = fullTableName;
        String[] words = fullTableName.split( "\\." );
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

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public Coercer getCoercer()
    {
        return coercer;
    }

    /**
     * Convert the bean into an insert statement and execute it.
     */
    public int insert( ConnectionSupplier cs, T bean )
        throws SQLException
    {
        PreparedStatement statement = getInsertStatement( cs, bean );

        setInsertValues( statement, bean );
        int rows = statement.executeUpdate();
        retrieveGeneratedKeys( statement, bean );

        return rows;
    }

    /**
     * Convert the bean into an update statement and execute it.
     */
    public int update( ConnectionSupplier cs, T bean )
        throws SQLException
    {
        PreparedStatement statement = getUpdateStatement( cs );
        setUpdateValues( statement, bean );

        return statement.executeUpdate();
    }

    /**
     * Convert the bean into an delete statement and execute it.
     */
    public boolean delete( ConnectionSupplier cs, T bean )
        throws SQLException
    {
        PreparedStatement statement = getDeleteStatement( cs );
        setDeleteValues( statement, bean );

        return statement.executeUpdate() > 0;
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

    public PreparedStatement getInsertStatement( ConnectionSupplier cs, T bean )
        throws SQLException
    {
        StringBuilder columns = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();

        Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
        Map<String, ColumnInfo> dbColumnInfos = getDBColumnInfos( cs.getConnection() );
        List<String> generatedKeyColumns = new ArrayList<>();
        for ( String key : dbColumnInfos.keySet() )
        {
            FieldDef fieldDef = beanFieldDefs.get( key );
            if ( fieldDef != null )
            {
                ColumnInfo columnInfo = dbColumnInfos.get( key );
                boolean addColumn = true;
                if ( fieldDef.isDefaultable() )
                {
                    Object value = getFieldValueFromBean( bean, key, columnInfo );
                    if ( value == null )
                    {
                        generatedKeyColumns.add( columnInfo.getRealName() );
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
        }

        String insertSQL = String.format( "insert into %s (%s) values (%s)", fullTableName, columns, questionMarks );
        generatedColumns = generatedKeyColumns.toArray( new String[generatedKeyColumns.size()] );

        return cs.prepareStatement( insertSQL );
    }

    public void setInsertValues( PreparedStatement statement, T bean )
        throws SQLException
    {
        Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
        int propIndex = 1;
        for ( String key : getDBColumnInfos( statement.getConnection() ).keySet() )
        {
            ColumnInfo columnInfo = getDBColumnInfos( statement.getConnection() ).get( key );
            FieldDef fieldDef = beanFieldDefs.get( key );
            if ( fieldDef != null )
            {
                Object val = getFieldValueFromBean( bean, key, columnInfo );

                if ( !fieldDef.isDefaultable() || val != null )
                {
                    //
                    // Set in statement
                    //
                    int sqlType = columnInfo.getSqlType();

                    if ( sqlType == Types.ARRAY )
                    {
                        if ( val == null )
                        {
                            statement.setNull( propIndex, sqlType );
                        }
                        else
                        {
                            Array array = statement.getConnection().createArrayOf( getTypeName( val ), (Object[]) val );
                            statement.setArray( propIndex, array );
                        }
                    }
                    else
                    {
                        statement.setObject( propIndex, val, sqlType );
                    }

                    propIndex++;
                }
            }
        }
    }

    public PreparedStatement getUpdateStatement( ConnectionSupplier cs )
        throws SQLException
    {
        if ( updateSql != null )
        {
            return cs.prepareStatement( updateSql );
        }

        StringBuilder setterClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();

        for ( String key : getDBColumnInfos( cs.getConnection() ).keySet() )
        {
            ColumnInfo columnInfo = getDBColumnInfos( cs.getConnection() ).get( key );

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

        return cs.prepareStatement( updateSql );
    }

    public void setUpdateValues( PreparedStatement statement, T bean )
        throws SQLException
    {
        List<DataValue> values = new ArrayList<>();
        List<DataValue> keyValues = new ArrayList<>();

        Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
        for ( String key : getDBColumnInfos( statement.getConnection() ).keySet() )
        {
            ColumnInfo columnInfo = getDBColumnInfos( statement.getConnection() ).get( key );
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

        int propIndex = 1;
        for ( DataValue value : values )
        {
            if ( value.getSqlType() == java.sql.Types.ARRAY )
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
            }
            propIndex++;
        }
        for ( DataValue value : keyValues )
        {
            statement.setObject( propIndex, value.getValue(), value.getSqlType() );
            propIndex++;
        }
    }

    public PreparedStatement getDeleteStatement( ConnectionSupplier cs )
        throws SQLException
    {
        if ( deleteSql != null )
        {
            return cs.prepareStatement( deleteSql );
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

            ColumnInfo columnInfo = getDBColumnInfos( cs.getConnection() ).get( key );

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

        return cs.prepareStatement( deleteSql );
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

            ColumnInfo columnInfo = getDBColumnInfos( statement.getConnection() ).get( key );
            Class<?> destClass = SQLTypeMapping.getInstance().getJavaClass( columnInfo.getSqlType() );

            Object val = readValue( bean, key );
            val = coercer.coerce( val, destClass );

            primaryKeyValues.add( new DataValue( val, columnInfo.getSqlType() ) );
        }

        int propIndex = 1;
        for ( DataValue value : primaryKeyValues )
        {
            statement.setObject( propIndex, value.getValue(), value.getSqlType() );
            propIndex++;
        }
    }

    protected void retrieveGeneratedKeys( PreparedStatement statement, Object bean )
        throws SQLException
    {
        if ( generatedColumns == null || generatedColumns.length == 0 )
        {
            return;
        }

        ResultSet rsKeys = statement.getGeneratedKeys();
        if ( rsKeys.next() )
        {
            Map<String, FieldDef> beanFieldDefs = BeanDefCache.getFieldDefs( bean.getClass() );
            for ( String column : generatedColumns )
            {
                try
                {
                    Object value = rsKeys.getObject( column );
                    if ( value != null )
                    {
                        FieldDef fieldDef = beanFieldDefs.get( column );
                        value = coercer.coerce( value, fieldDef.getValueClass() );
                    }
                    writeValue( bean, column, value );
                }
                catch ( FauxjoException e )
                {
                    throw new FauxjoException( "Failed to coerce " + column, e );
                }
            }
        }
    }

    // ----------
    // private
    // ----------

    private Map<String, ColumnInfo> getDBColumnInfos( Connection conn )
        throws SQLException
    {
        if ( dbColumnInfos == null )
        {
            cacheColumnInfos( conn );
        }

        return dbColumnInfos;
    }

    /**
     * This is a really slow method to call when it actually gets the meta data.
     */
    private void cacheColumnInfos( Connection conn )
        throws SQLException
    {
        String realTableName = getRealTableName( conn, tableName );

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
        dbColumnInfos = map;
    }

    /**
     * This takes a case insensitive tableName and searches for it in the connection's meta data to find the connections case sensitive tableName.
     */
    private String getRealTableName( Connection conn, String tableName )
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
        Class<?> destClass = SQLTypeMapping.getInstance().getJavaClass( columnInfo.getSqlType() );

        Object val = readValue( bean, key );
        try
        {
            val = coercer.coerce( val, destClass );
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

    private void writeValue( Object bean, String key, Object value )
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

    private String getTypeName( Object val )
    {
        String typeName = "varchar";
        Class klass = val.getClass().getComponentType();
        if ( klass == UUID.class )
        {
            typeName = "uuid";
        }
        else if ( klass == Date.class || klass == java.util.Date.class )
        {
            typeName = "timestamptz";
        }
        else if ( klass == Integer.class )
        {
            typeName = "int";
        }
        else if ( klass == Double.class )
        {
            typeName = "float8";
        }
        else if ( klass == Boolean.class )
        {
            typeName = "boolean";
        }

        return typeName;
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
}
