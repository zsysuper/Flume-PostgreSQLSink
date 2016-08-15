Flume-PostgreSQLSink
=============
A library of Flume sinks for postgreSQL

##Getting Started
----
1. Clone the repository
2. Install latest Maven and build source by 'mvn package'
3. Generate classpath by 'mvn dependency:build-classpath'
4. Append classpath in $FLUME_HOME/conf/flume-env.sh
5. Add the sink definition according to **Configuration**

##Source data format:
    Source data format must be JSON string, for example:
    
    {
        "data": {
            "field_name1": value1,
            "field_name2": value2,
            "field_name3": value3
        } 
    }
    
## Configuration
- - - 
    type: org.frank.flume.sink.PostgresSink
    host: db host IP [localhost]
    port: db port [5432]
    db  : postgresql db name
    username: = db username 	[]
    password: = db user password []
    batch: batch size of insert opertion [100]
    model: = auto/by_field/custom
             [auto]: Means every event will specify the table name by the field 'log_usr' in the event JSON string.
             [by_column_name]: Means every event will specify the table name by the field which specify by 'table_name_by'.
             [custom]: Means every event will specify the table name as you like.
    table_name_by: Required  when "model" is 'by_field', and the table name will be specifed by the value of this field.
    table_name: Required when "model" is 'custom', you can specifed table name as you like.
    columns: The number of columns write to postgresql. 
    nameN (N = 1,2,3...): The column name.
    typeN (N = 1,2,3...): The column type. The value can be: {text, char, numberic, integer, int, smallint, bigint, real, float, double, double precison, date, time, timestamp}
   
## flume.conf sample
- - - 
    agent.sources = source1  
    agent.sinks = sink1  
    agent.channels = channel1  
     
    agent.sources.source1.type = netcat  
    agent.sources.source1.bind = 192.168.1.2
    agent.sources.source1.port = 9000  
      
    agent.sinks.sink1.type = org.frank.flume.sink.PostgresSink
    agent.sinks.sink1.host = 192.168.1.1
    agent.sinks.sink1.port = 5432
    agent.sinks.sink1.batch = 100
    agent.sinks.sink1.db = postgres
    agent.sinks.sink1.username = test	
    agent.sinks.sink1.password = testpassword
    agent.sinks.sink1.model = by_column_name
    agent.sinks.sink1.table_name_by = log_usr
      
    agent.sinks.sink1.num.columns = 3
    agent.sinks.sink1.column.name1 = time
    agent.sinks.sink1.column.type1 = timestamp
    agent.sinks.sink1.column.name2 = log_usr
    agent.sinks.sink1.column.type2 = varchar
    agent.sinks.sink1.column.name3 = src_ip
    agent.sinks.sink1.column.type3 = bigint
      
    agent.channels.channel1.type = memory  
    agent.channels.channel1.capacity = 1000  
    agent.channels.channel1.transactionCapacity = 100 
      
    agent.sources.source1.channels = channel1  
    agent.sinks.sink1.channel = channel1  
    
## About the director jar_libs
- - - 
    Copy the json-simple-1.1.1.jar and postgresql-9.4.1208.jre7.jar to the path 'xxx/flume/lib/'.And you can update these jar as necessary.
