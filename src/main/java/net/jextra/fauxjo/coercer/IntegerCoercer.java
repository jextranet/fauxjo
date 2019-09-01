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

import net.jextra.fauxjo.*;

public class IntegerCoercer implements TypeCoercer<Integer>
{
    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public Object convertTo( Integer value, Class<?> targetClass )
        throws FauxjoException
    {
        if ( targetClass.equals( Byte.class ) )
        {
            assert value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE;
            return value.byteValue();
        }
        else if ( targetClass.equals( Short.class ) )
        {
            assert value <= Short.MAX_VALUE && value >= Short.MIN_VALUE;
            return value.shortValue();
        }
        else if ( targetClass.equals( Long.class ) )
        {
            return value.longValue();
        }
        else if ( targetClass.equals( Float.class ) )
        {
            return (float) value;
        }
        else if ( targetClass.equals( Double.class ) )
        {
            return (double) value;
        }
        else if ( targetClass.equals( String.class ) )
        {
            return value.toString();
        }

        throw new FauxjoException( String.format( ERROR_MSG, getClass().getName(), targetClass ) );
    }

}
