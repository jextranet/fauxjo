/*
 * Copyright (C) Elsevier
 * All Rights Reserved.
 */

package net.jextra.connectionsupplier;

import java.io.*;
import java.sql.*;

public class StatementBuilder {
    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public PreparedStatement loadResource(ConnectionSupplier cs, Class<?> relativeClass, String resourceName) throws IOException, SQLException {
        return cs.prepareStatement(loadResourceString(relativeClass, resourceName));
    }

    public String loadResourceString(Class<?> relativeClass, String resourceName) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        InputStream in = relativeClass.getResourceAsStream(resourceName);
        while ((length = in.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        in.close();

        return result.toString("UTF-8");
    }

    public PreparedStatement loadInputStream(ConnectionSupplier cs, InputStream in) throws IOException, SQLException {
        return cs.prepareStatement(loadInputStreamString(in));
    }

    public String loadInputStreamString(InputStream in) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = in.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        in.close();

        return result.toString("UTF-8");
    }
}
