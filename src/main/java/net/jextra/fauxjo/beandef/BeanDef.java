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
import java.util.*;
import net.jextra.fauxjo.*;

/**
 * Represents the result of processing the annotations on the Fauxjo bean class.
 * <p>
 * Processing the annotations on a class is rather slow. This object stores the results so that they can be placed in a cache.
 */
public class BeanDef
{
    // ============================================================
    // Fields
    // ============================================================

    private Map<String, FieldDef> fieldDefs;

    // ============================================================
    // Constructors
    // ============================================================

    public BeanDef()
    {
        fieldDefs = new TreeMap<>();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public FieldDef addField( String key, Field field )
        throws FauxjoException
    {
        final FieldDef fieldDef = getFieldDef( key );
        fieldDef.setField( field );
        return fieldDef;
    }

    public Field getField( String key )
    {
        return getFieldDef( key ).getField();
    }

    public void addReadMethod( String key, Method method )
        throws FauxjoException
    {
        getFieldDef( key ).setReadMethod( method );
    }

    public Method getReadMethod( String key )
    {
        return getFieldDef( key ).getReadMethod();
    }

    public void addWriteMethod( String key, Method method )
        throws FauxjoException
    {
        getFieldDef( key ).setWriteMethod( method );
    }

    public Method getWriteMethod( String key )
    {
        return getFieldDef( key ).getWriteMethod();
    }

    public Map<String, FieldDef> getFieldDefs()
    {
        TreeMap<String, FieldDef> map = new TreeMap<>();

        for ( String key : fieldDefs.keySet() )
        {
            map.put( key, getFieldDef( key ) );
        }

        return map;
    }

    public FieldDef getFieldDef( String key )
    {
        FieldDef def = fieldDefs.get( key.toLowerCase() );
        if ( def == null )
        {
            def = new FieldDef();
            fieldDefs.put( key.toLowerCase(), def );
        }

        return def;
    }
}
