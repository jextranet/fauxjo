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

import java.util.*;

/**
 * Delegate class to be used in a {@link HomeGroup} class to
 * manage its instances.
 */
public class HomeGroupCache<T extends HomeGroup>
{
    // ============================================================
    // Fields
    // ============================================================

    public static String DEFAULT_INSTANCE = "_default_";

    private Map<String, T> instances;
    private Class<T> clazz;

    // ============================================================
    // Constructors
    // ============================================================

    public HomeGroupCache( Class<T> clazz )
    {
        this.clazz = clazz;
        instances = new HashMap<>();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public T use( String instanceName )
        throws FauxjoException
    {
        if ( instanceName == null )
        {
            instanceName = DEFAULT_INSTANCE;
        }

        T homeGroup = instances.get( instanceName );
        if ( homeGroup == null )
        {
            try
            {
                homeGroup = clazz.newInstance();
                instances.put( instanceName, homeGroup );
            }
            catch ( Exception e )
            {
                throw new FauxjoException( e );
            }

            if ( homeGroup == null )
            {
                throw new FauxjoException( String.format( "Unable to create HomeGroup for class %s", clazz.getName() ) );
            }
        }
        return homeGroup;
    }

    public T use()
        throws FauxjoException
    {
        return use( DEFAULT_INSTANCE );
    }

    public void setInstance( String instanceName, T schema )
    {
        instances.put( instanceName, schema );
    }

    public void setInstance( T schema )
    {
        setInstance( DEFAULT_INSTANCE, schema );
    }
}
