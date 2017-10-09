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

package net.jextra.fauxjo;

import java.sql.*;
import java.util.*;
import net.jextra.connectionsupplier.*;

/**
 * An object to group a number of Home objects together to a common ConnectionSupplier.
 */
public class HomeGroup implements ConnectionSupplier {
    // ============================================================
    // Fields
    // ============================================================

    private HashMap<Class<?>,Home<?>> homes;
    private ConnectionSupplier cs;

    // ============================================================
    // Constructors
    // ============================================================

    public HomeGroup() {
        homes = new HashMap<>();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public Connection getConnection() throws SQLException {
        validateConnectionSupplier();

        return cs.getConnection();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        validateConnectionSupplier();

        return cs.prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareCall(String sql) throws SQLException {
        validateConnectionSupplier();

        return cs.prepareCall(sql);
    }

    @Override
    public void close() throws SQLException {
        validateConnectionSupplier();

        cs.close();
    }

    public ConnectionSupplier getConnectionSupplier() {
        return cs;
    }

    public void setConnectionSupplier(ConnectionSupplier cs) {
        this.cs = cs;
    }

    public void addHome(Class<?> homeClass, Home<?> home) {
        homes.put(homeClass, home);
    }

    public <T> T getHome(Class<T> homeClass) {
        return homeClass.cast(homes.get(homeClass));
    }

    // ----------
    // private
    // ----------

    private void validateConnectionSupplier() {
        if (cs == null) {
            throw new RuntimeException("The ConnectionSupplier for a Module must be set before it is used");
        }
    }
}
