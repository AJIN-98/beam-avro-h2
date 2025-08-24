package com.example.beam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public final class DbInit {
  private DbInit() {}

  public static void createTable(String jdbcUrl) {
    try {
      Class.forName("org.h2.Driver");
      try (Connection c = DriverManager.getConnection(jdbcUrl, "sa", "");
           Statement st = c.createStatement()) {
        st.executeUpdate("CREATE TABLE IF NOT EXISTS people_enc (" +
            "id BIGINT PRIMARY KEY, " +
            "name_enc VARCHAR(2048), " +
            "email_enc VARCHAR(2048))");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
