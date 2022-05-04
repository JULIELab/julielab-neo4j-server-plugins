package de.julielab.neo4j.plugins.auxiliaries;

import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;

import static org.neo4j.logging.FormattedLogFormat.PLAIN;

public class LogUtilities {
    private static Log4jLogProvider log4jLogProvider = new Log4jLogProvider(LogConfig.createBuilder(System.out, Level.INFO)
            .withFormat(PLAIN)
            .withCategory(false)
            .build());

    public static Log getLogger(Class<?> cls) {
        return log4jLogProvider.getLog(cls);
    }
}
