package org.frank.flume.sink;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.ContentHandler;


/**
 * Author: Frank.Zheng
 * Date: 16-07-11
 * Time: 11:43 AM
 */
public class PostgresSink extends AbstractSink implements Configurable {

	public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DB_NAME = "db";
    public static final String ENCODING = "encoding";
	public static final String NAME_PREFIX = "Postgres_";
	public static final String BATCH_SIZE = "batch";
	public static final String NUMOFCOLUMNS = "num.columns";
	public static final String TYPEOFCOLUMNS = "column.type";
	public static final String NAMEOFCOLUMNS = "column.name";
	public static final String TIMESTAMP_FIELD = "time";
	public static final String ID_FIELD = "time";
	public static final String LOGUSER_FIELD = "log_usr";
	public static final String MODEL = "model";
	public static final String TABLE_NAME_BY = "table_name_by";
	public static final String TABLE_NAME = "table_name";
	
	public static final String DEFAULT_HOST = "localhost";
    public static final int	   DEFAULT_PORT = 5432;
    public static final String DEFAULT_USERNAME = "root";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_DB = "postgres";
    public static final String DEFAULT_TABLE = "PostgresSink";
    public static final String DEFAULT_ENCODING = "UTF-8";
	public static final int    DEFAULT_BATCH = 100;
	public static final String DEFAULT_MODE = "auto";
	public static final String DEFAULT_TABLE_NAME_BY = "log_usr";
	public static final String DEFAULT_TABLE_NAME = "postgresqlSink";
	public static final String EXTRA_FIELDS_PREFIX = "extraFields.";
	public static final char   NAMESPACE_SEPARATOR = '.';
	
	private String host;
    private int port;
    private String username;
    private String password;
	private String database;
	private String url;
	
	private TableNameModel model;
	private String table_name_by;
	private String table_name;
	
	private Charset encoding = null;
	private Connection client = null;
	private PreparedStatement insert = null;
	private String sqlStatement = null;
	private int numColumns = 0;
	private int[] types = null;
	private String[] names = null;
	
	private int batchSize = 0;
	
	private CounterGroup counterGroup = new CounterGroup();
	private static AtomicInteger counter = new AtomicInteger();
	private static Logger logger = LoggerFactory.getLogger(PostgresSink.class);

	@Override
	public void configure(Context context) {
		setName(NAME_PREFIX + counter.getAndIncrement()); 
		
		try {
			Class.forName("org.postgresql.Driver");
			logger.info("Loaded postgres JDBC driver.");
		} catch (ClassNotFoundException e) {
			throw new FlumeException("Postgres JDBC driver not on classpath.");
		}
		
		host = context.getString(HOST, DEFAULT_HOST);
        port = context.getInteger(PORT, DEFAULT_PORT);
        username = context.getString(USERNAME, DEFAULT_USERNAME);
        password = context.getString(PASSWORD, DEFAULT_PASSWORD);
		database = context.getString(DB_NAME, DEFAULT_DB);
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH);	
		numColumns = context.getInteger(NUMOFCOLUMNS);
		model = TableNameModel.valueOf(context.getString(MODEL, TableNameModel.auto.name()));
		table_name_by = context.getString(TABLE_NAME_BY, DEFAULT_TABLE_NAME_BY);
		table_name = context.getString(TABLE_NAME, DEFAULT_TABLE_NAME);
		
		types = new int[numColumns];
		names = new String[numColumns];

		for (int i = 1; i <= numColumns; ++i) {
			types[i - 1] = getColumnType(context.getString(TYPEOFCOLUMNS +  i));
			if (types[i - 1] == Types.NULL) {
				throw new FlumeException("Type " + context.getString(TYPEOFCOLUMNS + i) + " not supported.");
			}
			
			names[i - 1] = context.getString(NAMEOFCOLUMNS + i);
			if (names[i - 1] == null || names[i - 1] == "" ) {
				throw new FlumeException("Nams " + context.getString(NAMEOFCOLUMNS + i) + " not found.");
			}
			logger.debug(String.valueOf(i));
			logger.debug("============= columnName:{}	columnType:{}", names[i - 1], types[i - 1]);
		}
		
		sqlStatement = "INSERT INTO %s (";
		
		for (int i = 0; i < numColumns; ++i) {
			sqlStatement += names[i];
			sqlStatement += ",";
		}
		
		sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1);
		
		sqlStatement += ") VALUES (";
		for (int i = 0; i < numColumns; ++i) {
			sqlStatement += "?,";
		}

		sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1);
		sqlStatement += ");";
		
		url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
		
		logger.info("PostgresSink {} context { host:{}, port:{}, username:{}, password:{}, database:{}, batch: {}, numcolumns: {}}",
			new Object[]{getName(), host, port, username, password, database, batchSize, numColumns});
		logger.info("Connection url of postgresql is '{}'", url);	
	}

	@Override
	public Status process() throws EventDeliveryException {
		logger.debug("{} start to process event", getName());

        Status status = Status.READY;
        try {
            status = parseEvents();
        } catch (Exception e) {
            logger.error("can't process events.", e);
        }
        logger.debug("{} processed event.", getName());
        return status;
	}
	
	/** processing events*/
	private Status parseEvents() throws EventDeliveryException {
		Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
		
		Map<String, List<JSONObject>> eventMap = new HashMap<String, List<JSONObject>>();
		
        try {
            transaction = channel.getTransaction();
            transaction.begin();

            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
					counterGroup.incrementAndGet("batch.underflow");
                    break;
                } else {
					pretreatmentOfEvent(eventMap, event);
                } 
            }
			
			saveEvents(eventMap);
			
			counterGroup.incrementAndGet("statements.commit");
            transaction.commit();
        } catch (Exception e) {
            logger.error("can't process events, drop it!", e);
            if (transaction != null) {
                transaction.commit();
            }
            throw new EventDeliveryException(e);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return status;
	}
	
	 private List<JSONObject> addEventToList(List<JSONObject> documents, JSONObject jsonObj) {
        if (documents == null) {
            documents = new ArrayList<JSONObject>(batchSize);
        }
		documents.add(jsonObj);
        return documents;
    }
	
	/** do your business in pretreatment of event*/
	private void pretreatmentOfEvent(Map<String, List<JSONObject>> eventMap, Event event) {
		byte[] body = event.getBody();
		JSONParser parser = new JSONParser();
		JSONObject eventJson = null;
		try{
			Object obj = parser.parse(new String(body));
			eventJson = (JSONObject) obj;
			if (eventJson == null) {
				logger.error("Event obj is empty!");
			}
		} catch (ParseException pe) {
			logger.error("not a json format, drop it!", pe);
		}
		
		String date = DEFAULT_TABLE; 
		String eventTime = (String)((JSONObject)eventJson.get("data")).get(TIMESTAMP_FIELD);
		if (eventTime != null) {
			date = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date( Long.parseLong(eventTime) * 1000) ); 
			((JSONObject)eventJson.get("data")).put(TIMESTAMP_FIELD, date);
		} else {
			logger.warn("This event has no field: {}, may be error to write to database", TIMESTAMP_FIELD);
		}
		
		String tableName = "";
		switch (model) {
            case auto:
				tableName = (String)((JSONObject)eventJson.get("data")).get(LOGUSER_FIELD);
				
                break;
            case by_column_name:
				tableName = (String)((JSONObject)eventJson.get("data")).get(table_name_by);
				
                break;
			case custom:
                tableName = table_name; //unsed model, done it by yourself
				
                break;
            default:
                logger.error("can't support model: {}, please check configuration.", model);
        }
		
		if ( tableName != null && tableName != "") {
			logger.debug("logUserName: {}", tableName);
		} else {
			logger.info("Table name is null, drop this event!");
			return;
		}
		
        String eventSchma;
        eventSchma = tableName;
        if (!eventMap.containsKey(eventSchma)) {
            eventMap.put(eventSchma, new ArrayList<JSONObject>());
        }

        List<JSONObject> docs = eventMap.get(eventSchma);
        addEventToList(docs, eventJson);
    }
	
	/** save envent data to postgresql*/
	private void saveEvents(Map<String, List<JSONObject>> eventMap) {
		Status status = Status.READY;
        if (eventMap.isEmpty()) {
			counterGroup.incrementAndGet("batch.empty");
			status = Status.BACKOFF;
            logger.debug("eventMap is empty.");
            return;
        }
		
	
		for (String logUser : eventMap.keySet()) {
			try {
				verifyConnection();
			} catch (SQLException e) {
				throw new FlumeException(e);
			}
		
            List<JSONObject> docs = eventMap.get(logUser);
			logger.debug("********************************************************");
            logger.debug("collection: {}, length: {}", logUser, docs.size());
			String sqls = String.format(sqlStatement, logUser);
			logger.debug(sqls);
			try {
				insert = client.prepareStatement(sqls);
			} catch (SQLException e) {
				throw new FlumeException(e);
			}
			
            for (JSONObject doc : docs) {
				for ( int i = 1; i <= numColumns; i++) {
					String value = (String)((JSONObject)doc.get("data")).get(names[i - 1]);
					switch (types[i - 1]) {
					case Types.VARCHAR:
					case Types.CHAR:
						try {
							insert.setString(i, value);
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.NUMERIC:
						try {
							insert.setBigDecimal(i, new BigDecimal(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.INTEGER:
						try {
							insert.setInt(i, Integer.parseInt(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.SMALLINT:
						try {
							insert.setShort(i, Short.parseShort(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.BIGINT:
						try {
							insert.setLong(i, Long.parseLong(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.REAL:
					case Types.FLOAT:
						try {
							insert.setFloat(i, Float.parseFloat(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.DOUBLE:
						try {
							insert.setDouble(i, Double.parseDouble(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.DATE:
						try {
							insert.setDate(i, Date.valueOf(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.TIME:
						try {
							insert.setTime(i, Time.valueOf(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					case Types.TIMESTAMP:
						try {
							insert.setTimestamp(i, Timestamp.valueOf(value));
						} catch (SQLException e) {
							throw new FlumeException(e);
						}
						break;
					default:
						throw new FlumeException("Type for " + value + " is not yet implemented");
					}
				}
				
				try {
					insert.executeUpdate();
				} catch (SQLException e) {
					throw new FlumeException(e);
				}	
			}
			try {
				client.commit();
			} catch (SQLException e) {
				throw new FlumeException(e);
			}
			counterGroup.incrementAndGet("batch.success");
		}
	}
	
	@Override
	public synchronized void start() {
		try {
			openConnection();
		} catch (SQLException e) {
			throw new FlumeException(e);
		}
		super.start();
		logger.info("Postgres Sink started");
	}

	@Override
	public synchronized void stop() {
		try {
			closeConnection();
		} catch (SQLException e) {
			throw new FlumeException(e);
		}
		super.stop();
		logger.info("Postgres Sink stopped");
	}

	/** make sure you can get the effective connection of database index by userId*/
	private void verifyConnection() throws SQLException {
		if (client == null) {
			openConnection();
		} else if (client.isClosed()) {
			closeConnection();
			openConnection();
		}
	}

	/** open the connection of database index by userId */
	private void openConnection() throws SQLException {
		Properties props = new Properties();

		if (username != null && password != null) {
			props.setProperty("user", username);
			props.setProperty("password", password);
		} else if (username != null ^ password != null) {
			logger.warn("User or password is set without the other. Continuing with no login auth.");
		}

		client = DriverManager.getConnection(url, props);
		client.setAutoCommit(false);
		//insert = client.prepareStatement(sqlStatement);
		logger.info("Opened client connection and prepared insert statement.");
	}

	/** close the connection of database index by userId */
	private void closeConnection() throws SQLException {
		if (client != null) {
			client.close();
			client = null;
			logger.info("Closed client connection");
			logger.info("Counters: {}", counterGroup);
		}
	}

	/** get the data type of column*/
	private int getColumnType(String string) {
		if ( string == null || string == "") {
			throw new FlumeException("Cloumn type string is empty or null.");
		}
		
		if (string.equalsIgnoreCase("VARCHAR")) {
			return Types.VARCHAR;
		} else if (string.equalsIgnoreCase("TEXT")) {
			return Types.VARCHAR;
		} else if (string.equalsIgnoreCase("CHAR")) {
			return Types.CHAR;
		} else if (string.equalsIgnoreCase("NUMERIC")) {
			return Types.NUMERIC;
		} else if (string.equalsIgnoreCase("INTEGER")) {
			return Types.INTEGER;
		} else if (string.equalsIgnoreCase("INT")) {
			return Types.INTEGER;
		} else if (string.equalsIgnoreCase("SMALLINT")) {
			return Types.BIGINT;
		} else if (string.equalsIgnoreCase("BIGINT")) {
			return Types.REAL;
		} else if (string.equalsIgnoreCase("REAL")) {
			return Types.FLOAT;
		} else if (string.equalsIgnoreCase("FLOAT")) {
			return Types.FLOAT;
		} else if (string.equalsIgnoreCase("DOUBLE")) {
			return Types.DOUBLE;
		} else if (string.equalsIgnoreCase("DOUBLE PRECISION")) {
			return Types.DOUBLE;
		} else if (string.equalsIgnoreCase("DATE")) {
			return Types.DATE;
		} else if (string.equalsIgnoreCase("TIME")) {
			return Types.TIME;
		} else if (string.equalsIgnoreCase("TIMESTAMP")) {
			return Types.TIMESTAMP;
		} 

		return Types.NULL;
	}
    
	public enum TableNameModel {
        auto, by_column_name, custom
    }
}
