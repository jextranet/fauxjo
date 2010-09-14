//
// Home
//
// Copyright (C) jextra.net.
//
//  This file is part of the Fauxjo Library.
//
//  The Fauxjo Library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 2.1 of the License, or (at your option) any later version.
//
//  The Fauxjo Library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//  Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with the Fauxjo Library; if not, write to the Free
//  Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
//  02111-1307 USA.
//

package net.fauxjo;

import java.sql.*;
import java.util.*;

/**
 * <p>
 * Default implementation of a Home object that represents a single table in the database.
 * </p><p>
 * This implementation assumes for the {@link #save(Fauxjo)} method that to test if the object
 * is already in the database or not that {@link #isInDatabase(Fauxjo)} just returns the
 * inverse value calculated via {@link #hasEmptyPrimaryKey(Fauxjo)}.
 * </p>
 */
public class Home < T extends Fauxjo > 
{
    // ============================================================
    // Fields
    // ============================================================

    private Schema _schema;
    private Class<T> _beanClass;
    private String _tableName;
    private SQLProcessor<T> _sqlProcessor;

    // ============================================================
    // Constructors
    // ============================================================

    public Home( Schema schema, Class<T> beanClass, String tableName )
        throws SQLException
    {
        _schema = schema;
        _beanClass = beanClass;
        _tableName = tableName;
        _sqlProcessor = new SQLProcessor<T>( _schema, _tableName, _beanClass );
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // protected
    // ----------

    public PreparedStatement prepareStatement( String sql )
        throws SQLException
    {
        return _schema.prepareStatement( sql );
    }

    public String getTableName()
    {
        return _tableName;
    }

    public String getQualifiedTableName()
    {
        return getQualifiedName( getTableName() );
    }

    public String getQualifiedName( String name )
    {
        return _schema.getQualifiedName( name );
    }

    public String getSchemaName()
    {
        return _schema.getSchemaName();
    }

    public boolean insert( T bean )
        throws SQLException
    {
        return _sqlProcessor.insert( bean );
    }

    public int update( T bean )
        throws SQLException
    {
        return _sqlProcessor.update( bean );
    }

    public boolean delete( T bean )
        throws SQLException
    {
        return _sqlProcessor.delete( bean );
    }

    public boolean save( T bean )
        throws SQLException
    {
        // If has empty PK, assumed to be new.
        if ( !isInDatabase( bean ) )
        {
            return insert( bean );
        }
        else
        {
            // Attempt to do an update
            int numRowsUpdated = update( bean );

            // If no rows were actually updated, assume must actually be new.
            if ( numRowsUpdated == 0 )
            {
                return insert( bean );
            }
        }

        return true;
    }

    /**
     * @return true if object is already in the database. The default behavior is to return the
     * inverse value of {@link #hasEmptyPrimaryKey(Fauxjo)}. This should be overridden if that
     * is not accurate for this Fauxjo bean.
     */
    public boolean isInDatabase( T bean )
        throws SQLException
    {
        return !hasEmptyPrimaryKey( bean );
    }

    // ----------
    // protected
    // ----------

    protected Schema getSchema()
    {
        return _schema;
    }

    protected Connection getConnection()
        throws SQLException
    {
        return _schema.getConnection();
    }

    protected SQLProcessor<T> getSQLProcessor()
    {
        return _sqlProcessor;
    }

    protected T getOne( ResultSet rs )
        throws SQLException
    {
        return _sqlProcessor.getOne( rs );
    }

    protected T getOnlyOne( ResultSet rs )
        throws SQLException
    {
        return _sqlProcessor.getOnlyOne( rs );
    }

    protected T getFirst( ResultSet rs )
        throws SQLException
    {
        return _sqlProcessor.getFirst( rs );
    }

    protected T getOnlyFirst( ResultSet rs )
        throws SQLException
    {
        return _sqlProcessor.getOnlyFirst( rs );
    }

    protected List<T> getList( ResultSet rs )
        throws SQLException
    {
        return _sqlProcessor.getList( rs );
    }

    protected List<T> getList( ResultSet rs, int maxNumRows )
        throws SQLException
    {
        return _sqlProcessor.getList( rs, maxNumRows );
    }

    protected ResultSetIterator<T> getIterator( ResultSet rs )
        throws SQLException
    {
        return _sqlProcessor.getIterator( rs );
    }

    protected String buildBasicSelect( String clause )
    {
        String c = "";
        if ( clause != null && !clause.trim().isEmpty() )
        {
            c = clause;
        }
        return "select * from " + getQualifiedTableName() + " " + c;
    }

    /**
     * @return false if any of the primary key values are null. This is implied that it is
     * not actually in the database.
     */
    protected boolean hasEmptyPrimaryKey( T bean )
        throws SQLException
    {
        List<Object> keys = bean.getPrimaryKeys();

        // If no values, assume empty.
        if ( keys == null || keys.isEmpty() )
        {
            return true;
        }

        // If any value is null, empty.
        for ( Object key : keys )
        {
            if ( key == null )
            {
                return true;
            }
        }

        return false;
    }
}

