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

package net.jextra.fauxjo.bean;

import java.util.*;
import net.jextra.fauxjo.*;
import net.jextra.fauxjo.beandef.*;

/**
 * Base class for a fauxjo (database bean).
 * <p>
 * Note: This implementation overrides the {@code hashCode} and {@code equals} methods in order to properly compare Fauxjo's properly (e.g. same
 * primary key) when placed in Collections, etc.
 */
public abstract class Fauxjo
{
    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public int hashCode()
    {
        try
        {
            List<Object> keys = getPrimaryKeyValues();

            // Any null keys equate to default of zero.
            if ( keys == null )
            {
                return 0;
            }

            // Just sum up the key item hashCodes for a final hashCode
            int hashCode = 0;
            for ( Object item : keys )
            {
                hashCode += item == null ? 0 : item.hashCode();
            }

            return hashCode;
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

    @Override
    public boolean equals( Object otherObj )
    {
        try
        {
            // If same object, just quickly return true.
            if ( this == otherObj )
            {
                return true;
            }

            // If other object is not same class as this object, quickly return false.
            if ( otherObj == null || !otherObj.getClass().equals( getClass() ) )
            {
                return false;
            }

            Fauxjo other = (Fauxjo) otherObj;

            List<Object> keys1 = getPrimaryKeyValues();
            List<Object> keys2 = other.getPrimaryKeyValues();

            // If the primary keys somehow are different lengths, they must not be the same.
            if ( keys1.size() != keys2.size() )
            {
                return false;
            }

            // Check each key item, if ever different, return false;
            for ( int i = 0; i < keys1.size(); i++ )
            {
                Object item1 = keys1.get( i );
                Object item2 = keys2.get( i );

                if ( item1 == null && item2 != null )
                {
                    return false;
                }
                else if ( !item1.equals( item2 ) )
                {
                    return false;
                }
            }
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }

        return true;
    }

    public String toString()
    {
        return getClass().getName() + buildKeyValueString();
    }

    // ----------
    // protected
    // ----------

    protected String buildKeyValueString()
    {
        StringBuilder builder = new StringBuilder();

        try
        {
            Map<String, FieldDef> map = BeanDefCache.getFieldDefs( getClass() );
            for ( String key : map.keySet() )
            {
                FieldDef def = map.get( key );
                if ( def.isPrimaryKey() )
                {
                    if ( def.getField() != null )
                    {
                        def.getField().setAccessible( true );
                        builder.append( String.format( " %s:%s", key, def.getField().get( this ) ) );
                    }
                    else
                    {
                        builder.append( String.format( " %s:%s", key, def.getReadMethod().invoke( this, new Object[0] ) ) );
                    }
                }
            }
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }

        return builder.toString();
    }

    /**
     * @return Values of primary keys in a consistent order so that it can be compared to other Fauxjo beans.
     */
    protected List<Object> getPrimaryKeyValues()
        throws FauxjoException
    {
        try
        {
            // Arbitrarily ordered by keys.
            TreeMap<String, Object> keys = new TreeMap<>();

            Map<String, FieldDef> map = BeanDefCache.getFieldDefs( getClass() );
            for ( String key : map.keySet() )
            {
                FieldDef def = map.get( key );
                if ( def.isPrimaryKey() )
                {
                    if ( def.getField() != null )
                    {
                        def.getField().setAccessible( true );
                        keys.put( key, def.getField().get( this ) );
                    }
                    else
                    {
                        keys.put( key, def.getReadMethod().invoke( this, new Object[0] ) );
                    }
                }
            }

            if ( keys.size() == 0 )
            {
                return null;
            }

            return new ArrayList<>( keys.values() );
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
}
