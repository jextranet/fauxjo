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

import java.sql.*;
import java.time.*;
import net.jextra.fauxjo.*;

public class StringCoercer implements TypeCoercer<String>
{
    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public Object convertTo( String value, Class<?> targetClass )
        throws FauxjoException
    {
        if ( targetClass.equals( Boolean.class ) )
        {
            return Boolean.parseBoolean( value );
        }
        else if ( targetClass.equals( Integer.class ) )
        {
            return Integer.parseInt( value );
        }
        else if ( targetClass.equals( Long.class ) )
        {
            return Long.parseLong( value );
        }
        else if ( targetClass.equals( Float.class ) )
        {
            return Float.parseFloat( value );
        }
        else if ( targetClass.equals( Double.class ) )
        {
            return Double.parseDouble( value );
        }
        else if ( targetClass.equals( java.sql.Date.class ) )
        {
            return java.sql.Date.valueOf( value );
        }
        else if ( targetClass.equals( Timestamp.class ) )
        {
            return Timestamp.valueOf( value );
        }
        else if ( targetClass.equals( ZoneId.class ) )
        {
            return ZoneId.of( value );
        }
        else if ( targetClass.equals( ZoneOffset.class ) )
        {
            return ZoneOffset.of( value );
        }
        else if ( targetClass.equals( Instant.class ) )
        {
            return Timestamp.valueOf( value ).toInstant();
        }
        else if ( targetClass.isEnum() )
        {
            @SuppressWarnings( "unchecked" )
            Class<? extends Enum> clss = (Class<? extends Enum>) targetClass;
            @SuppressWarnings( "unchecked" )
            Object obj = Enum.valueOf( clss, value );

            return obj;
        }
        else if ( targetClass.equals( Object.class ) )
        {
            return value;
        }

        throw new FauxjoException( String.format( ERROR_MSG, getClass().getName(), targetClass ) );
    }
}
