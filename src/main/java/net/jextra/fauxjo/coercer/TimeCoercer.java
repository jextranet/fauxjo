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

public class TimeCoercer implements TypeCoercer<Time>
{
    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public Object convertTo( Time value, Class<?> targetClass )
        throws FauxjoException
    {
        if ( targetClass.equals( LocalTime.class ) )
        {
            return value.toLocalTime();
        }
        else if ( targetClass.equals( Long.class ) )
        {
            return value.getTime();
        }
        else if ( targetClass.equals( String.class ) )
        {
            return value.toString();
        }

        throw new FauxjoException( String.format( ERROR_MSG, getClass().getName(), targetClass ) );
    }
}
