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
import java.sql.Array;
import java.sql.*;
import java.util.*;
import net.jextra.fauxjo.bean.*;
import net.jextra.fauxjo.beandef.*;
import net.jextra.fauxjo.coercer.*;

/**
 * Converts a ResultSet into {@link Fauxjo} beans.
 */
public class BeanBuilder<T> implements ResultSetIterator.Builder<T>
{
    // ============================================================
    // Fields
    // ============================================================

    private Class<T> beanClass;
    private Coercer coercer;
    private boolean allowMissingColumns;

    // Key = Lowercase column name (in code known as the "key").
    // Value = Information about the bean property.
    private Map<String, FieldDef> fieldDefs;
    private boolean autoCloseResultSet;

    // ============================================================
    // Constructors
    // ============================================================

    public BeanBuilder( Class<T> beanClass, boolean autoCloseResultSet )
    {
        this.beanClass = beanClass;
        this.autoCloseResultSet = autoCloseResultSet;
        coercer = new Coercer();
    }

    public BeanBuilder( Class<T> beanClass )
    {
        this( beanClass, false );
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public boolean getAllowMissingColumns()
    {
        return allowMissingColumns;
    }

    public void setAllowMissingColumns( boolean allowMissingColumns )
    {
        this.allowMissingColumns = allowMissingColumns;
    }

    public boolean getAutoCloseResultSet()
    {
        return autoCloseResultSet;
    }

    public void setAutoCloseResultSet( boolean autoCloseResultSet )
    {
        this.autoCloseResultSet = autoCloseResultSet;
    }

    public T getFirst( ResultSet rs )
        throws SQLException
    {
        return getFirst( rs, false, false );
    }

    public T getFirst( ResultSet rs, boolean errorIfEmpty )
        throws SQLException
    {
        return getFirst( rs, errorIfEmpty, false );
    }

    public T getUnique( ResultSet rs )
        throws SQLException
    {
        return getFirst( rs, false, true );
    }

    public T getUnique( ResultSet rs, boolean errorIfEmpty )
        throws SQLException
    {
        return getFirst( rs, errorIfEmpty, true );
    }

    /**
     * Reads the first item in the given ResultSet and converts it into a fauxjo bean.
     */
    public T getFirst( ResultSet rs, boolean errorIfEmpty, boolean errorIfMoreThanOne )
        throws SQLException
    {
        ArrayList<T> beans = new ArrayList<>();
        buildBeans( beans, rs, 2 );
        if ( autoCloseResultSet )
        {
            rs.getStatement().close();
            rs.close();
        }

        if ( beans.isEmpty() )
        {
            if ( errorIfEmpty )
            {
                throw new FauxjoException( "ResultSet is improperly empty." );
            }

            return null;
        }

        if ( errorIfMoreThanOne && beans.size() != 1 )
        {
            throw new FauxjoException( "ResultSet improperly contained more than one item." );
        }

        return beans.get( 0 );
    }

    /**
     * WARNING: This classes the passed in ResultSet.
     */
    public List<T> getList( ResultSet rs )
        throws SQLException
    {
        return getList( rs, -1 );
    }

    /**
     * Reads the first maxNumItems (-1 = all) ResultSet rows and converts them into a list of fauxjo beans.
     */
    public List<T> getList( ResultSet rs, int maxNumItems )
        throws SQLException
    {
        ArrayList<T> result = new ArrayList<>();
        buildBeans( result, rs, maxNumItems );
        if ( autoCloseResultSet )
        {
            rs.getStatement().close();
            rs.close();
        }

        return result;
    }

    /**
     * WARNING: This classes the passed in ResultSet.
     */
    public Set<T> getSet( ResultSet rs )
        throws SQLException
    {
        return getSet( rs, -1 );
    }

    public Set<T> getSet( ResultSet rs, int maxNumItems )
        throws SQLException
    {
        LinkedHashSet<T> result = new LinkedHashSet<>();
        buildBeans( result, rs, maxNumItems );
        if ( autoCloseResultSet )
        {
            rs.getStatement().close();
            rs.close();
        }

        return result;
    }

    public ResultSetIterator<T> getIterator( ResultSet rs )
        throws SQLException
    {
        return new ResultSetIterator<>( rs, this );
    }

    public void buildBeans( Collection<T> beans, ResultSet rs )
        throws SQLException
    {
        buildBeans( beans, rs, -1 );
    }

    public void buildBeans( Collection<T> beans, ResultSet rs, int numRows )
        throws SQLException
    {
        int counter = 0;
        while ( rs.next() && ( numRows < 0 || counter < numRows ) )
        {
            beans.add( buildBean( rs ) );
            counter++;
        }
    }

    public T buildBean( ResultSet rs )
        throws SQLException
    {
        try
        {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            Map<String, Object> record = new HashMap<>();
            for ( int i = 1; i <= columnCount; i++ )
            {
                if ( meta.getColumnType( i ) == java.sql.Types.ARRAY )
                {
                    Array array = rs.getArray( i );
                    Object actualArray = null;
                    if ( array != null )
                    {
                        actualArray = array.getArray();
                    }
                    record.put( meta.getColumnName( i ).toLowerCase(), actualArray );
                }
                else
                {
                    record.put( meta.getColumnName( i ).toLowerCase(), rs.getObject( i ) );
                }
            }

            return buildBean( record );
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

    // ----------
    // protected
    // ----------

    protected T buildBean( Map<String, Object> record )
        throws SQLException
    {
        T bean;

        try
        {
            bean = beanClass.newInstance();
        }
        catch ( Exception ex )
        {
            throw new FauxjoException( ex );
        }

        Map<String, FieldDef> fieldDefs = new HashMap<>( getFieldDefs() );
        for ( String key : record.keySet() )
        {
            FieldDef fieldDef = fieldDefs.get( key );

            // Remove key from fieldDefs in order to take inventory to check later that all were used.
            fieldDefs.remove( key );

            // If column in database but not in bean, assumed OK, ignore.
            if ( fieldDef != null )
            {
                Object value = record.get( key );

                try
                {
                    if ( value != null )
                    {
                        Class<?> targetClass = fieldDef.getValueClass();
                        value = coercer.convertTo( value, targetClass );
                    }
                }
                catch ( FauxjoException ex )
                {
                    throw new FauxjoException( "Failed to coerce " + key, ex );
                }

                setBeanValue( bean, key, value );
            }
        }

        // If any of the columns was not accounted for, throw an Exception
        if ( !allowMissingColumns && !fieldDefs.isEmpty() )
        {
            throw new FauxjoException(
                "Missing column [" + fieldDefs.keySet().iterator().next() + "] in ResultSet for fauxjo [" + beanClass.getCanonicalName() + "]" );
        }

        return bean;
    }

    protected void setBeanValue( T bean, String key, Object value )
        throws FauxjoException
    {
        BeanDef beanDef = BeanDefCache.getBeanDef( beanClass );

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

    protected Map<String, FieldDef> getFieldDefs()
        throws FauxjoException
    {
        if ( fieldDefs == null )
        {
            fieldDefs = BeanDefCache.getFieldDefs( beanClass );
        }

        return fieldDefs;
    }
}
