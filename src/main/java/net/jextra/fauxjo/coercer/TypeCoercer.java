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

/**
 * <p>
 * Coerces one value type to another value type. These are used when the column type from the
 * databas does not "exactly" match the bean's field type and vice versa.
 * </p>
 */
public interface TypeCoercer<T>
{
    // ============================================================
    // Fields
    // ============================================================

    public static String ERROR_MSG = "The %s does not know how to convert to type [%s]";

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    Object convertTo( T value, Class<?> targetClass )
        throws FauxjoException;
}
