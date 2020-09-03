package io.debezium.connector.postgresql.priva;

import org.junit.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig;

public class Config {

    @Test
    public void TestPostgresConfig() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        PostgresConnectorConfig connectorConfig = (PostgresConnectorConfig) Class
                .forName(String.valueOf(PostgresConnectorConfig.class)).newInstance();
    }

}
