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

import java.beans.*;
import java.lang.reflect.*;
import java.util.*;
import net.jextra.fauxjo.*;
import net.jextra.fauxjo.bean.*;

public class BeanDefCache
{
    // ============================================================
    // Fields
    // ============================================================

    private static HashMap<Class<?>, BeanDef> beanDefCache;

    // ============================================================
    // Constructors
    // ============================================================

    static
    {
        beanDefCache = new HashMap<>();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public static Map<String, FieldDef> getFieldDefs( Class<?> fauxjoClass )
        throws FauxjoException
    {
        try
        {
            BeanDef beanDef = getBeanDef( fauxjoClass );

            return beanDef.getFieldDefs();
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

    public static BeanDef getBeanDef( Class<?> fauxjoClass )
        throws FauxjoException
    {
        try
        {
            BeanDef beanDef = beanDefCache.get( fauxjoClass );
            if ( beanDef != null )
            {
                return beanDef;
            }

            //
            // Was not cached, collect information.
            //
            beanDef = new BeanDef();

            for ( Field field : getFauxjoFields( fauxjoClass ) )
            {
                FauxjoField ann = field.getAnnotation( FauxjoField.class );
                String key = ann.value();

                FieldDef fieldDef = beanDef.addField( key, field );
                fieldDef.setDefaultable( ann.defaultable() );

                //
                // Check if FauxjoPrimaryKey.
                //
                if ( field.isAnnotationPresent( FauxjoPrimaryKey.class ) )
                {
                    fieldDef.setPrimaryKey( true );
                }
            }

            BeanInfo info = Introspector.getBeanInfo( fauxjoClass );
            for ( PropertyDescriptor prop : info.getPropertyDescriptors() )
            {
                if ( prop.getWriteMethod() != null )
                {
                    FauxjoSetter ann = prop.getWriteMethod().getAnnotation( FauxjoSetter.class );
                    if ( ann != null )
                    {
                        String key = ann.value();

                        Field field = beanDef.getField( key );
                        if ( field != null )
                        {
                            throw new FauxjoException(
                                "FauxjoSetter defined on method where a FauxjoField " + "already defines the link to the column [" + key + "]" );
                        }

                        beanDef.addWriteMethod( key, prop.getWriteMethod() );
                    }
                }

                if ( prop.getReadMethod() != null )
                {
                    FauxjoGetter ann = prop.getReadMethod().getAnnotation( FauxjoGetter.class );
                    if ( ann != null )
                    {
                        String key = ann.value();

                        Field field = beanDef.getField( key );
                        if ( field != null )
                        {
                            throw new FauxjoException(
                                "FauxjoGetter defined on method where a FauxjoField " + "already defines the link to the column [" + key + "]" );
                        }

                        beanDef.addReadMethod( key, prop.getReadMethod() );

                        //
                        // Check if FauxjoPrimaryKey.
                        //
                        if ( prop.getReadMethod().isAnnotationPresent( FauxjoPrimaryKey.class ) )
                        {
                            beanDef.getFieldDef( key ).setPrimaryKey( true );
                        }
                    }
                }
            }

            // Put in cacche
            beanDefCache.put( fauxjoClass, beanDef );

            return beanDef;
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

    // ----------
    // private
    // ----------

    private static Collection<Field> getFauxjoFields( Class<?> cls )
    {
        ArrayList<Field> list = new ArrayList<Field>();

        if ( cls == null )
        {
            return list;
        }

        // Add super-classes fields first.
        list.addAll( getFauxjoFields( cls.getSuperclass() ) );

        for ( Field field : cls.getDeclaredFields() )
        {
            if ( field.isAnnotationPresent( FauxjoField.class ) )
            {
                list.add( field );
            }
        }

        return list;
    }
}
