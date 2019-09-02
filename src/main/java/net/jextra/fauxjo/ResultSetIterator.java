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
 * Iterator of a {@link ResultSet} that knows how to convert each row in the ResultSet to a
 * Fauxjo bean. This is used primarily to iterate over a large number records without having to
 * load them all into memory.
 */
public class ResultSetIterator<T> implements Iterator<T>, Iterable<T>
{
    // ============================================================
    // Fields
    // ============================================================

    private Builder<T> builder;
    private ResultSet resultSet;
    private boolean hasNext;

    // ============================================================
    // Constructors
    // ============================================================

    public ResultSetIterator( ResultSet resultSet, Builder<T> builder )
        throws SQLException
    {
        this.builder = builder;
        this.resultSet = resultSet;
        hasNext = resultSet.next();
    }

    public ResultSetIterator( ResultSet resultSet, Class<T> clzz )
        throws SQLException
    {
        this.builder = new BeanBuilder<T>( clzz, true );
        this.resultSet = resultSet;
        hasNext = resultSet.next();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public T next()
    {
        if ( !hasNext )
        {
            return null;
        }

        try
        {
            T bean = builder.buildBean( resultSet );
            hasNext = resultSet.next();
            if ( !hasNext )
            {
                close();
            }

            return bean;
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( "Remove is not supported for " + "ResultSetIterators." );
    }

    public void close()
        throws SQLException
    {
        if ( resultSet != null )
        {
            resultSet.getStatement().close();
            resultSet.close();
            resultSet = null;
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return this;
    }

    // ============================================================
    // Inner Classes
    // ============================================================

    public interface Builder<T>
    {
        T buildBean( ResultSet rs )
            throws SQLException;
    }
}
