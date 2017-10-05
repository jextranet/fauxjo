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
import net.jextra.fauxjo.bean.*;

/**
 * Base implementation of a data access object that represents a table in the database.
 */
public class Home<T extends Fauxjo> {
    // ============================================================
    // Fields
    // ============================================================

    private ConnectionSupplier cs;
    private Table<T> table;
    private BeanBuilder<T> beanBuilder;

    // ============================================================
    // Constructors
    // ============================================================

    public Home(ConnectionSupplier cs, String tableName, Class<T> beanClass) {
        this.cs = cs;
        table = new Table<>(tableName, beanClass);
        beanBuilder = new BeanBuilder<>(beanClass);
        beanBuilder.setAutoCloseResultSet(true);
    }

    public Home(ConnectionSupplier cs, Table<T> table, BeanBuilder<T> beanBuilder) throws SQLException {
        this.cs = cs;
        this.table = table;
        this.beanBuilder = beanBuilder;
        beanBuilder.setAutoCloseResultSet(true);
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public ConnectionSupplier getConnectionSupplier() {
        return cs;
    }

    public Table getTable() {
        return table;
    }

    public BeanBuilder<T> getBeanBuilder() {
        return beanBuilder;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return cs.prepareStatement(sql);
    }

    public String getSchemaName() {
        return table.getSchemaName();
    }

    public String getTableName() {
        return table.getTableName();
    }

    /**
     * This method attaches the schema name to the front of the name passed in.
     *
     * @return String that represents the given short name.
     */
    public String getQualifiedName(String name) {
        if (table.getSchemaName() == null || table.getSchemaName().equals("")) {
            return name;
        } else {
            return table.getSchemaName() + "." + name;
        }
    }

    public int insert(T bean) throws SQLException {
        return table.insert(cs, bean);
    }

    public int update(T bean) throws SQLException {
        return table.update(cs, bean);
    }

    public boolean delete(T bean) throws SQLException {
        return table.delete(cs, bean);
    }

    public String buildBasicSelect(String clause) {
        return table.buildBasicSelectStatement(clause);
    }

    public T getFirst(ResultSet rs) throws SQLException {
        return beanBuilder.getFirst(rs);
    }

    public T getFirst(ResultSet rs, boolean errorIfEmpty) throws SQLException {
        return beanBuilder.getFirst(rs, errorIfEmpty);
    }

    public T getUnique(ResultSet rs) throws SQLException {
        return beanBuilder.getUnique(rs);
    }

    public T getUnique(ResultSet rs, boolean errorIfEmpty) throws SQLException {
        return beanBuilder.getUnique(rs, errorIfEmpty);
    }

    public List<T> getList(ResultSet rs) throws SQLException {
        return beanBuilder.getList(rs);
    }

    public List<T> getList(ResultSet rs, int maxNumRows) throws SQLException {
        return beanBuilder.getList(rs, maxNumRows);
    }

    public Set<T> getSet(ResultSet rs) throws SQLException {
        return beanBuilder.getSet(rs);
    }

    public Set<T> getSet(ResultSet rs, int maxNumRows) throws SQLException {
        return beanBuilder.getSet(rs, maxNumRows);
    }

    public ResultSetIterator<T> getIterator(ResultSet rs) throws SQLException {
        return beanBuilder.getIterator(rs);
    }
}
