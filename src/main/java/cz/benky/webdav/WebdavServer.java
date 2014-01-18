package cz.benky.webdav;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import cz.benky.webdav.util.CassandraUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class WebdavServer {

    private static final Log logger = LogFactory.getLog(WebdavServer.class);
    private static final String USAGE = "WebdavServer";
    private static final String HEADER = "Cassandra-WebDav PoC";

    private final Server webServer;

    public WebdavServer(int port) throws Exception {
        logger.info("Initializing webdav server");

        webServer = new Server(port);

        Context root = new Context(webServer, "/", Context.SESSIONS);
        root.addServlet(new ServletHolder(new WebdavServlet()), "/*");
    }

    public void start() throws Exception {
        webServer.start();
    }

    public static void createModel() throws ConnectionException {
        final Keyspace keyspace = CassandraUtils.getConnection();

        //    CREATE COLUMN FAMILY storage WITH comparator = UTF8Type AND key_validation_class=UTF8Type

        keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                        .put("replication_factor", "1")
                        .build())
                .put("strategy_class", "SimpleStrategy")
                .build()
        );

        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("CREATE TABLE directory(pathId uuid, child varchar, childId uuid, PRIMARY KEY (pathId, child)) WITH COMPACT STORAGE;")
                .asPreparedStatement()
                .execute();

        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("CREATE TABLE file(file uuid PRIMARY KEY, size bigint, storageType varchar, storageData varchar);")
                .asPreparedStatement()
                .execute();

        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("insert into directory(pathid, child, childid) VALUES (?, '.', ?);")
                .asPreparedStatement()
                .withUUIDValue(CassandraUtils.ROOT_UUID)
                .withUUIDValue(CassandraUtils.ROOT_UUID)
                .execute();

        final ColumnFamily<String, String> cf = ColumnFamily.newColumnFamily("storage", StringSerializer.get(), StringSerializer.get());
        keyspace.createColumnFamily(cf, ImmutableMap.<String, Object>builder()
                .put("key_validation_class", "UTF8Type")
                .put("comparator", "UTF8Type")
                .build());
    }

    public static void main(String[] args) throws Exception {
        final Options options = new Options();
        options.addOption("p", "port", true, "port to bind to");
        options.addOption("m", "create-model", false, "creates model in cassandra");
        options.addOption("h", "help", false, "print usage information");
        final CommandLineParser parser = new GnuParser();
        final CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h")) {
            new HelpFormatter().printHelp(USAGE, HEADER, options, "");
            return;
        }
        if (cmd.hasOption("m")) {
            createModel();
            return;
        }
        int port = Integer.parseInt(cmd.getOptionValue("port", "8080"));

        final WebdavServer server = new WebdavServer(port);
        logger.info("Starting webdav server");
        server.start();
    }

}
