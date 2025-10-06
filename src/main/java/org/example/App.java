package org.example;

import java.sql.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.math.BigDecimal;

public class App {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Properties props = new Properties();
            props.load(new FileInputStream("src/main/resources/application.properties"));

            String url = props.getProperty("db.url");
            String user = props.getProperty("db.user");
            String password = props.getProperty("db.password");

            // Загрузка драйвера (не всегда нужно, но для надежности)
            Class.forName("org.postgresql.Driver");

            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);

            // 1. Проверка/Добавление товара "Масло подсолнечное"
            if (!existsInTable(conn, "product", "description", "Масло подсолнечное")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO product (description, price, quantity, category) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, "Масло подсолнечное");
                    ps.setBigDecimal(2, new BigDecimal("150"));
                    ps.setInt(3, 50);
                    ps.setString(4, "Масла");
                    ps.executeUpdate();
                }
            }

            // 2. Проверка/Добавление клиента с телефоном 89876543210
            int customerId;
            if (!existsInTable(conn, "customer", "phone", "89876543210")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO customer (first_name, last_name, phone, email) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                    ps.setString(1, "Петр");
                    ps.setString(2, "Иванов");
                    ps.setString(3, "89876543210");
                    ps.setString(4, "peter@mail.com");
                    ps.executeUpdate();
                    ResultSet rsKeys = ps.getGeneratedKeys();
                    rsKeys.next();
                    customerId = rsKeys.getInt(1);
                }
            } else {
                // получаем ID существующего клиента
                try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM customer WHERE phone = ?")) {
                    ps.setString(1, "89876543210");
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        customerId = rs.getInt("id");
                    } else {
                        throw new SQLException("Не удалось найти клиента с этим номером после проверки");
                    }
                }
            }

            // 3. Создаем заказ (если такой еще нет)
            if (!existsOrderForCustomerAndProduct(conn, customerId, "Масло подсолнечное", 2)) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.\"order\" (product_id, customer_id, order_date, quantity, status_id) VALUES (?, ?, CURRENT_TIMESTAMP, ?, (SELECT id FROM order_status WHERE name='Обработка'))")) {
                    ps.setInt(1, getProductIdByDescription(conn, "Масло подсолнечное"));
                    ps.setInt(2, customerId);
                    ps.setInt(3, 2);
                    ps.executeUpdate();
                }
            }

            // 4. Вывести последние 5 заказов
            System.out.println("\n--- Последние 5 заказов ---");
            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(
                    "SELECT o.id, c.first_name, c.last_name, p.description, o.order_date, o.quantity, s.name AS status "
                            + "FROM public.\"order\" o "
                            + "JOIN customer c ON o.customer_id = c.id "
                            + "JOIN product p ON o.product_id = p.id "
                            + "JOIN order_status s ON o.status_id = s.id "
                            + "ORDER BY o.order_date DESC LIMIT 5"
            )) {
                while (rs.next()) {
                    System.out.printf("Заказ ID=%d, клиент=%s %s, товар=%s, дата=%s, кол=%d, статус=%s\n",
                            rs.getInt("id"), rs.getString("first_name"), rs.getString("last_name"),
                            rs.getString("description"), rs.getTimestamp("order_date"),
                            rs.getInt("quantity"), rs.getString("status"));
                }
            }

            // 5. Обновление цены "Молоко 1л"
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE product SET price = ? WHERE description = ?")) {
                ps.setBigDecimal(1, new BigDecimal("55"));
                ps.setString(2, "Молоко 1л");
                ps.executeUpdate();
            }

            // 6. Обновление количества "Молоко 1л"
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE product SET quantity = quantity + ? WHERE description = ?")) {
                ps.setInt(1, 10);
                ps.setString(2, "Молоко 1л");
                ps.executeUpdate();
            }

            // 7. Удаление заказа по ID=9999 (если есть)
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM public.\"order\" WHERE id = ?")) {
                ps.setInt(1, 9999);
                ps.executeUpdate();
            }

            // 8. Вставка нового клиента, если нет
            if (!existsInTable(conn, "customer", "phone", "89991112233")) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO customer (first_name, last_name, phone, email) VALUES (?, ?, ?, ?)")) {
                    ps.setString(1, "Петр");
                    ps.setString(2, "Иванов");
                    ps.setString(3, "89991112233");
                    ps.setString(4, "peter@mail.com");
                    ps.executeUpdate();
                }
            }

            // 9. Создать заказ для этого клиента и товара "Кофе растворимый"
            int newCustomerId;
            if (!existsInTable(conn, "customer", "phone", "89991112233")) {
                // Создаем клиента, если нет
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO customer (first_name, last_name, phone, email) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                    ps.setString(1, "Петр");
                    ps.setString(2, "Иванов");
                    ps.setString(3, "89991112233");
                    ps.setString(4, "peter@mail.com");
                    ps.executeUpdate();
                    ResultSet rs = ps.getGeneratedKeys();
                    rs.next();
                    newCustomerId = rs.getInt(1);
                }
            } else {
                try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM customer WHERE phone = ?")) {
                    ps.setString(1, "89991112233");
                    ResultSet rs = ps.executeQuery();
                    rs.next();
                    newCustomerId = rs.getInt("id");
                }
            }
            // Создаем заказ на этого клиента и товар "Кофе растворимый"
            if (!existsOrderForCustomerAndProduct(conn, newCustomerId, "Кофе растворимый", 2)) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.\"order\" (product_id, customer_id, order_date, quantity, status_id) VALUES (?, ?, CURRENT_TIMESTAMP, ?, (SELECT id FROM order_status WHERE name='Обработка'))")) {
                    ps.setInt(1, getProductIdByDescription(conn, "Кофе растворимый"));
                    ps.setInt(2, newCustomerId);
                    ps.setInt(3, 2);
                    ps.executeUpdate();
                }
            }

            conn.commit();

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (conn != null) conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            if (conn != null) try { conn.close(); } catch (Exception e) { e.printStackTrace(); }
        }
    }

    // Вспомогательные функции:
    private static boolean existsInTable(Connection conn, String table, String column, String value) throws SQLException {
        String sql = "SELECT 1 FROM " + table + " WHERE " + column + " = ? LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, value);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        }
    }

    private static int getProductIdByDescription(Connection conn, String description) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM product WHERE description = ?")) {
            ps.setString(1, description);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return rs.getInt("id");
            throw new SQLException("Product not found: " + description);
        }
    }

    private static boolean existsOrderForCustomerAndProduct(Connection conn, int customerId, String productDescription, int quantity) throws SQLException {
        String sql = "SELECT 1 FROM public.\"order\" o "
                + "JOIN product p ON o.product_id=p.id "
                + "WHERE o.customer_id=? AND p.description=? AND o.quantity=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, customerId);
            ps.setString(2, productDescription);
            ps.setInt(3, quantity);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        }
    }
}