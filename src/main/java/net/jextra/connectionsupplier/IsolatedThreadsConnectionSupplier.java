/*
 * Copyright (C) fauxjo.net.
 *
 * This file is part of the Fauxjo Library.
 *
 * The Fauxjo Library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The Fauxjo Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the Fauxjo Library; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA.
 */

package net.jextra.connectionsupplier;

import java.sql.*;
import javax.sql.*;

/**
 * Same as {@link PrincipalConnectionSupplier} except that there is a different {@link DataSource} for each {@link Thread}.
 */
public class IsolatedThreadsConnectionSupplier extends PrincipalConnectionSupplier {
    // ============================================================
    // Fields
    // ============================================================

    private ThreadLocal<DataSource> dataSourceLocal;

    // ============================================================
    // Constructors
    // ============================================================

    public IsolatedThreadsConnectionSupplier() {
        dataSourceLocal = new ThreadLocal<>();
    }

    public IsolatedThreadsConnectionSupplier(DataSource ds) throws SQLException {
        super(ds);
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public DataSource getDataSource() {
        if (dataSourceLocal == null) {
            dataSourceLocal = new ThreadLocal<>();
        }

        return dataSourceLocal.get();
    }

    public void setDataSource(DataSource ds) throws SQLException {
        // Make sure to clear all of the cached information because if the DataSource changes it call all be wrong.
        close();

        if (dataSourceLocal == null) {
            dataSourceLocal = new ThreadLocal<>();
        }

        dataSourceLocal.set(ds);
    }
}
