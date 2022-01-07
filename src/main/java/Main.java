import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        // Load properties
        String rootPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).getPath();
        String astraConfigPath = rootPath + "astra.properties";

        Properties astraProp = new Properties();
        try {
            astraProp.load(new FileInputStream(astraConfigPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Configure these properties in "astra.properties" located in project resources
        String secureConnectBundle = astraProp.getProperty("secure_connect_bundle");
        String clientId = astraProp.getProperty("client_id");
        String clientSecret = astraProp.getProperty("client_secret");
        String keyspace = astraProp.getProperty("keyspace");

        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(secureConnectBundle))
                .withAuthCredentials(clientId,clientSecret)
                .withKeyspace(keyspace)
                .build()) {

            // Update query for your schema
            String query = "select * from consistency.t1";
            Statement<SimpleStatement> statement = SimpleStatement.newInstance(query).setTracing(true);

            // Execute query and print results
            ResultSet rs = session.execute(statement);

            for (Row row : rs.all()) {
                if (row != null) {
                    System.out.println(row.getFormattedContents());
                } else {
                    System.out.println("An error occurred.");
                }
            }

            // Tracing
            ExecutionInfo executionInfo = rs.getExecutionInfo();
            QueryTrace trace = executionInfo.getQueryTrace();
            TraceEventFormatter traceEventFormatter = new TraceEventFormatter(trace);
            System.out.println(traceEventFormatter);
        }
    }
}
