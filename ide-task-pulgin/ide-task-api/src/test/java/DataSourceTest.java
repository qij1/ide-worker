import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Properties;

public class DataSourceTest {


    @Test
    public void testOracleConnect() {
        try {
            URL[] urls = new URL[]{new URL("file:C:\\Users\\12415\\Desktop\\drivers\\ojdbc8-19.3.0.0.jar")};
            URLClassLoader classLoader = new URLClassLoader(urls);
            Class<?> driverClass = classLoader.loadClass("oracle.jdbc.OracleDriver");
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(driver);
            Properties properties = new Properties();
            properties.setProperty("user", "ide");
            properties.setProperty("password", "ide");
            Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:ORCL", properties);

//            Connection conn = driver.connect("jdbc:oracle:thin:@localhost:1521:ORCL", properties);
            System.out.println(conn.isClosed());
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
