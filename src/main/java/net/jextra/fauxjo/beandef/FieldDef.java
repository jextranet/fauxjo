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

package net.jextra.fauxjo.beandef;

import java.lang.reflect.*;
import net.jextra.fauxjo.*;

public class FieldDef
{
    // ============================================================
    // Fields
    // ============================================================

    private Field field;
    private Method writeMethod;
    private Method readMethod;
    private Class<?> valueClass;
    private boolean primaryKey;
    private boolean defaultable;

    // ============================================================
    // Constructors
    // ============================================================

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public Field getField()
    {
        return field;
    }

    public void setField( Field field )
        throws FauxjoException
    {
        this.field = field;

        // Calculate the valueClass from the field.
        if ( valueClass == null )
        {
            valueClass = field.getType();
        }
        else
        {
            if ( !valueClass.equals( field.getType() ) )
            {
                throw new FauxjoException( "Field [" + field.getName() + "] must have type of [" + valueClass.getCanonicalName() + "]" );
            }
        }
    }

    public Method getWriteMethod()
    {
        return writeMethod;
    }

    public void setWriteMethod( Method writeMethod )
        throws FauxjoException
    {
        this.writeMethod = writeMethod;

        // Calculate the valueClass from the write method.
        if ( valueClass == null )
        {
            valueClass = writeMethod.getParameterTypes()[0];
        }
        else
        {
            if ( !valueClass.equals( writeMethod.getParameterTypes()[0] ) )
            {
                throw new FauxjoException(
                    "Write method [" + writeMethod.getName() + "] must have first argument of type [" + valueClass.getCanonicalName() + "]" );
            }
        }
    }

    public Method getReadMethod()
    {
        return readMethod;
    }

    public void setReadMethod( Method readMethod )
        throws FauxjoException
    {
        this.readMethod = readMethod;

        // Calculate the valueClass from the read method.
        if ( valueClass == null )
        {
            valueClass = readMethod.getReturnType();
        }
        else
        {
            if ( !valueClass.equals( readMethod.getReturnType() ) )
            {
                throw new FauxjoException(
                    "Read method [" + readMethod.getName() + "] must have return type of [" + valueClass.getCanonicalName() + "]" );
            }
        }
    }

    public boolean isPrimaryKey()
    {
        return primaryKey;
    }

    public void setPrimaryKey( boolean primaryKey )
    {
        this.primaryKey = primaryKey;
    }

    public Class<?> getValueClass()
    {
        return valueClass;
    }

    public boolean isDefaultable()
    {
        return defaultable;
    }

    public void setDefaultable( boolean defaultable )
    {
        this.defaultable = defaultable;
    }
}
