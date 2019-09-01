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

package net.jextra.fauxjo.coercer;

import java.math.*;
import java.time.*;
import java.util.*;
import net.jextra.fauxjo.*;

/**
 * General use tool that coerces one value type to another value type. For example from a
 * String to an Integer.
 */
public class Coercer
{
    // ============================================================
    // Fields
    // ============================================================

    private Map<Class<?>, TypeCoercer<?>> coercerMap = new HashMap<>();

    // ============================================================
    // Constructors
    // ============================================================

    public Coercer()
    {
        coercerMap.put( Object.class, new ObjectCoercer() );
        coercerMap.put( String.class, new StringCoercer() );
        coercerMap.put( Byte.class, new ByteCoercer() );
        coercerMap.put( Short.class, new ShortCoercer() );
        coercerMap.put( Integer.class, new IntegerCoercer() );
        coercerMap.put( Long.class, new LongCoercer() );
        coercerMap.put( BigInteger.class, new BigIntegerCoercer() );
        coercerMap.put( BigDecimal.class, new BigDecimalCoercer() );
        coercerMap.put( Float.class, new FloatCoercer() );
        coercerMap.put( Double.class, new DoubleCoercer() );
        coercerMap.put( java.util.Date.class, new UtilDateCoercer() );
        coercerMap.put( java.sql.Date.class, new DateCoercer() );
        coercerMap.put( java.sql.Timestamp.class, new TimestampCoercer() );
        coercerMap.put( Instant.class, new InstantCoercer() );
        coercerMap.put( UUID.class, new UUIDCoercer() );
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public <T> void addTypeCoercer( Class<T> coercerClass, TypeCoercer<T> coercer )
    {
        coercerMap.put( (Class<?>) coercerClass, (TypeCoercer<?>) coercer );
    }

    @SuppressWarnings( "unchecked" )
    public <T> TypeCoercer<T> getTypeCoercer( Class<T> coercerClass )
    {
        return (TypeCoercer<T>) coercerMap.get( coercerClass );
    }

    @SuppressWarnings( "unchecked" )
    public <T> Object convertTo( T value, Class<?> targetClass )
        throws FauxjoException
    {
        // Null values are just null values.
        if ( value == null )
        {
            return null;
        }

        if ( targetClass.isPrimitive() )
        {
            if ( targetClass == Boolean.TYPE )
            {
                targetClass = Boolean.class;
            }
            else if ( targetClass == Byte.TYPE )
            {
                targetClass = Byte.class;
            }
            else if ( targetClass == Character.TYPE )
            {
                targetClass = Character.class;
            }
            else if ( targetClass == Double.TYPE )
            {
                targetClass = Double.class;
            }
            else if ( targetClass == Float.TYPE )
            {
                targetClass = Float.class;
            }
            else if ( targetClass == Integer.TYPE )
            {
                targetClass = Integer.class;
            }
            else if ( targetClass == Long.TYPE )
            {
                targetClass = Long.class;
            }
            else if ( targetClass == Short.TYPE )
            {
                targetClass = Short.class;
            }
        }

        // Short-circuit if given value is the same as the target class
        if ( value.getClass().equals( targetClass ) )
        {
            return value;
        }

        TypeCoercer<T> coercer = (TypeCoercer<T>) coercerMap.get( value.getClass() );

        // Use default coercer if none is found
        if ( coercer == null )
        {
            coercer = (TypeCoercer<T>) coercerMap.get( Object.class );
        }

        return coercer.convertTo( value, targetClass );
    }
}
