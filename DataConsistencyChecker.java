import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.text.SimpleDateFormat;
import org.yaml.snakeyaml.Yaml;

/**
 * Oracle and GaussDB Data Consistency Checker
 * Compare data consistency between two databases using checksum algorithm
 */
public class DataConsistencyChecker {
    
    private static final String CONFIG_FILE = "config.yml";
    private static final String REPORT_DIR = "reports";
    
    // 配置参数
    private String oracleUrl;
    private String oracleUser;
    private String oraclePassword;
    private String oracleDriverJar;
    private String gaussUrl;
    private String gaussUser;
    private String gaussPassword;
    private String gaussDriverJar;
    private int threadCount;
    private List<String> tableList;
    private List<String> schemaList;  // Schema list for auto table discovery
    private List<String> excludeTableList;  // Exclude table list
    private List<CustomSql> customSqlList;
    private Map<String, String> schemaMapping;
    private boolean initializeOracleFunctions;
    private String createScriptPath;
    private boolean dropOracleFunctions;
    private String dropScriptPath;
    
    // Result storage
    private Map<String, ChecksumResult> oracleResults = new ConcurrentHashMap<>();
    private Map<String, ChecksumResult> gaussResults = new ConcurrentHashMap<>();
    private Map<String, Exception> errors = new ConcurrentHashMap<>();
    private Map<String, String> sqlGenErrors = new ConcurrentHashMap<>(); // SQL generation errors
    private Map<String, String> oracleSqlMap = new ConcurrentHashMap<>(); // Store Oracle SQL for each task
    private Map<String, String> gaussSqlMap = new ConcurrentHashMap<>(); // Store GaussDB SQL for each task
    private Map<String, Long> oracleExecutionTimes = new ConcurrentHashMap<>(); // Store Oracle execution times
    private Map<String, Long> gaussExecutionTimes = new ConcurrentHashMap<>(); // Store GaussDB execution times
    
    public static void main(String[] args) {
        DataConsistencyChecker checker = new DataConsistencyChecker();
        try {
            // Allow specifying config file via command line
            String configFile = args.length > 0 ? args[0] : CONFIG_FILE;
            checker.loadConfig(configFile);
            checker.run();
        } catch (Exception e) {
            System.err.println("Program execution failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Load YAML configuration file
     */
    private void loadConfig() throws IOException {
        loadConfig(CONFIG_FILE);
    }
    
    /**
     * Load YAML configuration file with specified filename
     */
    private void loadConfig(String configFile) throws IOException {
        File configFileObj = new File(configFile);
        if (!configFileObj.exists()) {
            throw new FileNotFoundException("Config file not found: " + configFile);
        }
        
        System.out.println("Using YAML config file: " + configFile);
        
        // 使用SnakeYAML解析配置文件
        Yaml yaml = new Yaml();
        Map<String, Object> config;
        try (FileInputStream fis = new FileInputStream(configFile)) {
            config = yaml.load(fis);
        }
        
        // Parse database configuration
        @SuppressWarnings("unchecked")
        Map<String, Object> databases = (Map<String, Object>) config.get("databases");
        if (databases != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> oracle = (Map<String, Object>) databases.get("oracle");
            @SuppressWarnings("unchecked")
            Map<String, Object> gauss = (Map<String, Object>) databases.get("gauss");
            
            if (oracle != null) {
                oracleUrl = (String) oracle.get("url");
                oracleUser = (String) oracle.get("user");
                oraclePassword = (String) oracle.get("password");
                oracleDriverJar = (String) oracle.get("driver_jar");
            }
            
            if (gauss != null) {
                gaussUrl = (String) gauss.get("url");
                gaussUser = (String) gauss.get("user");
                gaussPassword = (String) gauss.get("password");
                gaussDriverJar = (String) gauss.get("driver_jar");
            }
        }
        
        // Parse performance configuration
        @SuppressWarnings("unchecked")
        Map<String, Object> performance = (Map<String, Object>) config.get("performance");
        if (performance != null) {
            Object threadCountObj = performance.get("thread_count");
            if (threadCountObj != null) {
                threadCount = (Integer) threadCountObj;
            }
        } else {
            threadCount = 4; // Default value
        }
        
        
        
        // Parse check scope configuration
        @SuppressWarnings("unchecked")
        Map<String, Object> checkScope = (Map<String, Object>) config.get("check_scope");
        if (checkScope != null) {
            // Load schema mapping
            schemaMapping = new HashMap<>();
            @SuppressWarnings("unchecked")
            Map<String, String> schemaMappingConfig = (Map<String, String>) checkScope.get("schema_mapping");
            if (schemaMappingConfig != null) {
                schemaMapping.putAll(schemaMappingConfig);
                System.out.println("Loaded schema mapping config: " + schemaMapping);
            }
            
            // Load table list
            tableList = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<String> tables = (List<String>) checkScope.get("tables");
            if (tables != null) {
                tableList.addAll(tables);
            }
            
            // Load schema list for auto table discovery
            schemaList = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<String> schemas = (List<String>) checkScope.get("schemas");
            if (schemas != null) {
                schemaList.addAll(schemas);
                System.out.println("Loaded schema list for auto discovery: " + schemaList);
            }
            
            // Load exclude table list
            excludeTableList = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<String> excludeTables = (List<String>) checkScope.get("exclude_tables");
            if (excludeTables != null) {
                excludeTableList.addAll(excludeTables);
                System.out.println("Loaded exclude table list: " + excludeTableList);
            }
            
            // Load custom SQL list
            customSqlList = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> customSqls = (List<Map<String, Object>>) checkScope.get("custom_sqls");
            if (customSqls != null) {
                for (Map<String, Object> sqlConfig : customSqls) {
                    String name = (String) sqlConfig.get("name");
                    String sql = (String) sqlConfig.get("sql");
                    if (name != null && sql != null && !sql.trim().isEmpty()) {
                        customSqlList.add(new CustomSql(name, sql.trim()));
                    }
                }
            }
        } else {
            tableList = new ArrayList<>();
            schemaList = new ArrayList<>();
            excludeTableList = new ArrayList<>();
            customSqlList = new ArrayList<>();
            schemaMapping = new HashMap<>();
        }
        
        validateConfig();
    }
    
    /**
     * Validate configuration
     */
    private void validateConfig() {
        if (oracleUrl == null || oracleUser == null || oraclePassword == null) {
            throw new IllegalArgumentException("Oracle database configuration incomplete");
        }
        if (gaussUrl == null || gaussUser == null || gaussPassword == null) {
            throw new IllegalArgumentException("GaussDB database configuration incomplete");
        }
        if (tableList.isEmpty() && customSqlList.isEmpty()) {
            throw new IllegalArgumentException("Must configure at least one table name or custom SQL");
        }
    }
    
    /**
     * Main execution flow
     */
    private void run() throws Exception {
        System.out.println("=== Oracle and GaussDB Data Consistency Checker ===");
        System.out.println("Start time: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
        
        // Create report directory
        new File(REPORT_DIR).mkdirs();
        
       
        
        // 2. Expand schema list to table list (if configured)
        if (!schemaList.isEmpty()) {
            System.out.println("Step 2: Expanding schema list to table list...");
            expandSchemaListToTables();
        }
        
        // 3. Apply exclude table filters
        if (!excludeTableList.isEmpty()) {
            System.out.println("Step " + (schemaList.isEmpty() ? "2" : "3") + ": Applying exclude table filters...");
            applyExcludeTableFilters();
        }
        
        // 4. Get table size and sort
        System.out.println("Step " + (getStepNumber() + 1) + ": Getting table statistics...");
        List<String> sortedTables = getSortedTablesBySize();
        
        // 5. Generate check SQL list
        System.out.println("Step " + (getStepNumber() + 2) + ": Generating check SQL...");
        List<CheckTask> checkTasks = generateCheckTasks(sortedTables);
        
        // 6. Execute checks concurrently
        System.out.println("Step " + (getStepNumber() + 3) + ": Executing data validation concurrently...");
        executeChecks(checkTasks);
        
        // 7. Generate reports
        System.out.println("Step " + (getStepNumber() + 4) + ": Generating validation reports...");
        generateReports();
        
        
        System.out.println("Validation completed at: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
    }
    
    /**
     * Get current step number based on enabled features
     */
    private int getStepNumber() {
        int step = 1; // Start from 1 (initialization is always step 1)
        
        if (!schemaList.isEmpty()) {
            step++; // Schema expansion
        }
        
        if (!excludeTableList.isEmpty()) {
            step++; // Exclude filters
        }
        
        return step;
    }
    
  
    /**
     * Check if a table should be excluded based on exclude patterns
     */
    private boolean shouldExcludeTable(String tableName) {
        if (excludeTableList.isEmpty()) {
            return false;
        }
        
        for (String excludePattern : excludeTableList) {
            // Exact match
            if (tableName.equalsIgnoreCase(excludePattern)) {
                return true;
            }
            
            // Wildcard match
            if (excludePattern.contains("*")) {
                String regex = excludePattern
                    .replace(".", "\\.")  // Escape dots
                    .replace("*", ".*");  // Convert * to .*
                if (tableName.matches(regex)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    /**
     * Apply exclude table filters to table list
     */
    private void applyExcludeTableFilters() {
        if (excludeTableList.isEmpty()) {
            return;
        }
        
        int originalSize = tableList.size();
        tableList.removeIf(this::shouldExcludeTable);
        int excludedCount = originalSize - tableList.size();
        
        if (excludedCount > 0) {
            System.out.println("Excluded " + excludedCount + " tables based on exclude patterns");
        }
    }
    
    /**
     * Expand schema list to table list by querying Oracle database
     */
    private void expandSchemaListToTables() throws SQLException {
        if (schemaList.isEmpty()) {
            return;
        }
        
        System.out.println("Expanding schema list to table list...");
        try (Connection conn = getOracleConnection()) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT owner||'.'||table_name as full_table_name FROM dba_tables WHERE ");
            
            // Build WHERE conditions for schemas
            List<String> conditions = new ArrayList<>();
            for (String schema : schemaList) {
                conditions.add("owner = '" + schema.toUpperCase() + "'");
            }
            
            sql.append("(").append(String.join(" OR ", conditions)).append(")");
            sql.append(" ORDER BY owner, table_name");
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql.toString())) {
                
                int tableCount = 0;
                int excludedCount = 0;
                while (rs.next()) {
                    String fullTableName = rs.getString("full_table_name");
                    
                    // Check if table should be excluded
                    if (shouldExcludeTable(fullTableName)) {
                        excludedCount++;
                        continue;
                    }
                    
                    if (!tableList.contains(fullTableName)) {
                        tableList.add(fullTableName);
                        tableCount++;
                    }
                }
                System.out.println("Added " + tableCount + " tables from schema list to table list");
                if (excludedCount > 0) {
                    System.out.println("Excluded " + excludedCount + " tables based on exclude patterns during schema expansion");
                }
                
            } catch (SQLException e) {
                System.err.println("Error: Failed to query tables from schemas: " + e.getMessage());
                throw e;
            }
        } catch (SQLException e) {
            System.err.println("Error: Failed to connect for schema expansion: " + e.getMessage());
            throw e;
        }
    }
    
    /**
     * Get sorted table list by size
     */
    private List<String> getSortedTablesBySize() throws SQLException {
        List<String> result = new ArrayList<>();
        if (tableList.isEmpty()) {
            return result;
        }
        
        try (Connection conn = getOracleConnection()) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT owner||'.'||table_name as full_table_name, num_rows FROM dba_tables WHERE ");
            
            // Build WHERE conditions, support both schema.table and table formats
            List<String> conditions = new ArrayList<>();
            for (String table : tableList) {
                if (table.contains(".")) {
                    String[] parts = table.split("\\.", 2);
                    String schema = parts[0].toUpperCase();
                    String tableName = parts[1].toUpperCase();
                    conditions.add("(owner = '" + schema + "' AND table_name = '" + tableName + "')");
                } else {
                    // If only table name, search in current user and all accessible schemas
                    conditions.add("table_name = '" + table.toUpperCase() + "'");
                }
            }
            
            sql.append("(").append(String.join(" OR ", conditions)).append(")");
            sql.append(" ORDER BY NVL(num_rows, 0) DESC");
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql.toString())) {
                while (rs.next()) {
                    String foundTable = rs.getString("full_table_name");
                    result.add(foundTable);
                    System.out.println("Found table in statistics: " + foundTable);
                }
            } catch (SQLException e) {
                System.err.println("Warning: Failed to query table statistics: " + e.getMessage());
                System.err.println("Will proceed with configured tables in original order");
            }
        } catch (SQLException e) {
            System.err.println("Warning: Failed to connect for table statistics: " + e.getMessage());
            System.err.println("Will proceed with configured tables in original order");
        }
        
        // Add tables from config that are not in statistics (keep original format)
        Set<String> foundTables = new HashSet<>();
        for (String r : result) {
            foundTables.add(r.toUpperCase());
        }
        
        for (String table : tableList) {
            String normalizedTable = table.toUpperCase();
            boolean found = false;
            
            // Check if this table was found in statistics
            for (String foundTable : foundTables) {
                if (foundTable.equals(normalizedTable) || 
                    (table.contains(".") && foundTable.equals(normalizedTable)) ||
                    (!table.contains(".") && foundTable.endsWith("." + normalizedTable))) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                result.add(table);
                System.out.println("Adding configured table not found in statistics: " + table);
            }
        }
        
        System.out.println("Total tables to process: " + result.size());
        return result;
    }
    
    /**
     * Generate check task list
     */
    private List<CheckTask> generateCheckTasks(List<String> sortedTables) throws SQLException {
        List<CheckTask> tasks = new ArrayList<>();
        
        try (Connection conn = getOracleConnection()) {
            // Generate check tasks for tables
            for (String tableName : sortedTables) {
                try {
                    String baseSql = "SELECT * FROM " + tableName;
                    String[] checkSqls = generateFormattedSql(conn, baseSql);
                    if (checkSqls[0] != null && checkSqls[1] != null) {
                        tasks.add(new CheckTask("TABLE:" + tableName, checkSqls[0], checkSqls[1]));
                        System.out.println("Generated check task for table: " + tableName);
                    } else {
                        System.err.println("Warning: Failed to generate SQL for table " + tableName + " - skipping");
                    }
                } catch (Exception e) {
                    System.err.println("Error: Failed to process table " + tableName + ": " + e.getMessage());
                    sqlGenErrors.put("TABLE:" + tableName, e.getMessage());
                    // Continue with next table instead of failing
                }
            }
            
            // Generate check tasks for custom SQL
            for (CustomSql customSql : customSqlList) {
                try {
                    String[] checkSqls = generateFormattedSql(conn, customSql.sql);
                    if (checkSqls[0] != null && checkSqls[1] != null) {
                        tasks.add(new CheckTask("CUSTOM:" + customSql.name, checkSqls[0], checkSqls[1]));
                        System.out.println("Generated check task for custom SQL: " + customSql.name);
                    } else {
                        System.err.println("Warning: Failed to generate SQL for custom query " + customSql.name + " - skipping");
                    }
                } catch (Exception e) {
                    System.err.println("Error: Failed to process custom SQL " + customSql.name + ": " + e.getMessage());
                    sqlGenErrors.put("CUSTOM:" + customSql.name, e.getMessage());
                    // Continue with next custom SQL instead of failing
                }
            }
        }
        
        if (tasks.isEmpty()) {
            System.err.println("Warning: No valid check tasks generated. Please check your configuration.");
        } else {
            System.out.println("Total check tasks generated: " + tasks.size());
        }
        
        return tasks;
    }
    
    /**
     * Generate formatted SQL
     */
    private String[] generateFormattedSql(Connection conn, String baseSql) throws SQLException {
        String[] result = new String[2]; // [0]=Oracle SQL, [1]=Gauss SQL
        
        try (CallableStatement cstmt = conn.prepareCall(" declare\n" +
            "i_sql varchar2(32767):=?;\n" + 
            "    G_RTRIM_CHAR varchar2(1):='Y';\n" + 
            "    l_col_name_str VARCHAR2(32767); --字段名串\n" + 
            "    l_curid        INTEGER;\n" + 
            "    l_cnt          NUMBER;\n" + 
            "    l_desctab      dbms_sql.desc_tab;\n" + 
            "    l_col_name     VARCHAR2(255);\n" + 
            "    l_format_col   VARCHAR2(255);\n" + 
            "    l_col_type     VARCHAR2(255);\n" + 
            "  -- PRAGMA AUTONOMOUS_TRANSACTION;\n" + 
            "  BEGIN\n" + 
            "    l_curid := dbms_sql.open_cursor();\n" + 
            "    dbms_sql.parse(l_curid, i_sql, dbms_sql.native);\n" + 
            "    dbms_sql.describe_columns(l_curid, l_cnt, l_desctab);\n" + 
            "    FOR i IN 1 .. l_desctab.count LOOP\n" + 
            "      l_col_name := l_desctab(i).col_name;\n" + 
            "      l_col_type := l_desctab(i).col_type;\n" + 
            "\n" + 
            "      IF l_col_type NOT IN (DBMS_SQL.Raw_Type,\n" + 
            "                        DBMS_SQL.Long_Raw_Type,\n" + 
            "                        DBMS_SQL.MLSLabel_Type,\n" + 
            "                        DBMS_SQL.User_Defined_Type,\n" + 
            "                        DBMS_SQL.Ref_Type,\n" + 
            "                        DBMS_SQL.Clob_Type,\n" + 
            "                        DBMS_SQL.Blob_Type,\n" + 
            "                        DBMS_SQL.Interval_Year_to_Month_Type,\n" + 
            "                        DBMS_SQL.Interval_Day_To_Second_Type,\n" + 
            "                        DBMS_SQL.Urowid_Type) THEN\n" + 
            "\n" + 
            "      if l_col_type in (dbms_Sql.Date_Type) THEN\n" + 
            "        l_format_col := 'to_char(' || l_col_name || ',''yyyymmddhh24miss'')||''000000''';\n" + 
            "      ELSIF l_col_type IN\n" + 
            "            (dbms_Sql.Timestamp_Type,\n" + 
            "             dbms_Sql.Timestamp_With_TZ_Type,\n" + 
            "             dbms_Sql.Timestamp_With_Local_TZ_type) then\n" + 
            "        l_format_col := 'to_char(' || l_col_name ||\n" + 
            "                      ',''yyyymmddhh24missff6'')';\n" + 
            "      elsif l_col_type in (dbms_sql.Number_Type,\n" + 
            "                           dbms_sql.Binary_Float_Type,\n" + 
            "                           dbms_Sql.Binary_Double_Type) then\n" + 
            "        l_format_col := 'to_char(' || l_col_name ||\n" + 
            "                      ',''fm99999999999999999999999999999.00000000'')';\n" + 
            "      elsif G_RTRIM_CHAR='Y' and l_col_type in (dbms_Sql.Char_Type ) then\n" + 
            "        l_format_col:='rtrim('||l_col_name||')';\n" + 
            "      else\n" + 
            "        l_format_col := l_col_name;\n" + 
            "      end if;\n" + 
            "      END IF;\n" + 
            "\n" + 
            "      l_col_name_str := l_col_name_str || CASE\n" + 
            "                          WHEN l_col_name_str IS NULL THEN\n" + 
            "                           NULL\n" + 
            "                          ELSE\n" + 
            "                           ','\n" + 
            "                        END || l_format_col ||' AS '||'\"'||l_desctab(i).col_name||'\"';\n" + 
            "    END LOOP;\n" + 
            "    dbms_sql.close_cursor(l_curid);\n" + 
            "    if l_col_name_str is not null then\n" + 
            "\n" + 
            "      ? := 'select count(1) as cnt,sum((''x''||substr(a,1,8))::bit(32)::int4::numeric/4 +'||\n" + 
            "                   '(''x''||substr(a,9,8))::bit(32)::int4::numeric/4 +'||\n" + 
            "                   '(''x''||substr(a,17,8))::bit(32)::int4::numeric/4 +'||\n" + 
            "                   '(''x''||substr(a,25,8))::bit(32)::int4::numeric/4 '||\n" + 
            "                   ') as cksum from (select /*+no_expand*/ md5(row_to_json(t)::text) a from (select '\n" + 
            "                   || l_col_name_str||' from (' || i_sql || ') )t )';\n" + 
            "      ? := 'with function uf_raw2int(input raw,pos number,len number) return number is\n" +
            "begin\n" +
            "  return utl_raw.cast_to_binary_integer(utl_raw.substr(input,pos,len));\n" +
            "end;\n" +
            "select count(1) as cnt,sum(uf_raw2int(a,0,4)/4+'||\n" + 
            "        'uf_raw2int(a,5,4)/4+'||\n" + 
            "        'uf_raw2int(a,9,4)/4+'||\n" + 
            "        'uf_raw2int(a,13,4)/4) as cksum from'||\n" + 
            "        '(select  dbms_crypto.hash(JSON_OBJECT(T.* RETURNING blob),2) a from (select '\n" + 
            "                || l_col_name_str||' from (' || i_sql || ') )t )';\n" + 
            "    end if;\n" + 
            "  END; ")) {
            cstmt.setString(1, baseSql);
            cstmt.registerOutParameter(2, Types.VARCHAR);
            cstmt.registerOutParameter(3, Types.VARCHAR);
            cstmt.execute();
            
            result[0] = cstmt.getString(3); // Oracle SQL (using GAUSS_CHECKSUM)
            String gaussSql = cstmt.getString(2); // Gauss SQL (using checksum)
            
            // Apply schema mapping to GaussDB SQL
            result[1] = applySchemaMapping(gaussSql);
            
            // Validate results
            if (result[0] == null || result[0].trim().isEmpty()) {
                throw new SQLException("Oracle SQL generation returned null or empty result");
            }
            if (result[1] == null || result[1].trim().isEmpty()) {
                throw new SQLException("GaussDB SQL generation returned null or empty result");
            }
            
        } catch (SQLException e) {
            // Log the specific SQL that caused the error
            System.err.println("SQL generation failed for: " + baseSql);
            System.err.println("Error details: " + e.getMessage());
            throw e; // Re-throw to be caught by caller
        }
        
        return result;
    }
    
    /**
     * Apply schema mapping to SQL, replace Oracle schema names with GaussDB schema names
     */
    private String applySchemaMapping(String sql) {
        if (sql == null || schemaMapping == null || schemaMapping.isEmpty()) {
            return sql;
        }
        
        String result = sql;
        for (Map.Entry<String, String> entry : schemaMapping.entrySet()) {
            String oracleSchema = entry.getKey().toLowerCase();
            String gaussSchema = entry.getValue().toLowerCase();
            
            // Replace schema part in schema.table format
            // Support case-insensitive replacement
            result = result.replaceAll("(?i)\\b" + oracleSchema + "\\.", gaussSchema + ".");
        }
        
        return result;
    }
    
    /**
     * Execute checks concurrently with improved connection management
     */
    private void executeChecks(List<CheckTask> tasks) throws InterruptedException {
        // Create separate thread pools for Oracle and GaussDB to better control concurrency
        int dbThreadCount = Math.max(1, threadCount / 2); // Split threads between databases
        ExecutorService oracleExecutor = Executors.newFixedThreadPool(dbThreadCount, r -> {
            Thread t = new Thread(r, "Oracle-Worker");
            t.setDaemon(true);
            return t;
        });
        ExecutorService gaussExecutor = Executors.newFixedThreadPool(dbThreadCount, r -> {
            Thread t = new Thread(r, "GaussDB-Worker");
            t.setDaemon(true);
            return t;
        });
        
        CountDownLatch latch = new CountDownLatch(tasks.size() * 2); // Oracle + Gauss
        
        System.out.println("Starting concurrent execution with " + dbThreadCount + " threads per database");
        
        for (CheckTask task : tasks) {
            // Store SQL statements for reporting
            oracleSqlMap.put(task.name, task.oracleSql);
            gaussSqlMap.put(task.name, task.gaussSql);
            
            // Oracle check
            oracleExecutor.submit(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    ChecksumResult result = executeChecksum(getOracleConnection(), task.oracleSql);
                    long endTime = System.currentTimeMillis();
                    long executionTime = endTime - startTime;
                    
                    oracleResults.put(task.name, result);
                    oracleExecutionTimes.put(task.name, executionTime);
                    System.out.println("Oracle [" + task.name + "]: " + result + " (" + executionTime + "ms)");
                } catch (Exception e) {
                    errors.put(task.name + "_ORACLE", e);
                    System.err.println("Oracle [" + task.name + "] execution failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
            
            // GaussDB check
            gaussExecutor.submit(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    ChecksumResult result = executeChecksum(getGaussConnection(), task.gaussSql);
                    long endTime = System.currentTimeMillis();
                    long executionTime = endTime - startTime;
                    
                    gaussResults.put(task.name, result);
                    gaussExecutionTimes.put(task.name, executionTime);
                    System.out.println("GaussDB [" + task.name + "]: " + result + " (" + executionTime + "ms)");
                } catch (Exception e) {
                    errors.put(task.name + "_GAUSS", e);
                    System.err.println("GaussDB [" + task.name + "] execution failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        
        // Shutdown executors gracefully
        oracleExecutor.shutdown();
        gaussExecutor.shutdown();
        
        try {
            if (!oracleExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                oracleExecutor.shutdownNow();
            }
            if (!gaussExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                gaussExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            oracleExecutor.shutdownNow();
            gaussExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("Concurrent execution completed");
    }
    
    /**
     * Execute checksum query and return both count and checksum values
     */
    private ChecksumResult executeChecksum(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long count = rs.getLong(1);      // cnt field
                long checksum = rs.getLong(2);   // cksum field
                return new ChecksumResult(count, checksum);
            }
            throw new SQLException("Query returned no results");
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    /**
     * Generate reports
     */
    private void generateReports() throws IOException {
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
        
        // Generate detail report
        generateDetailReport(timestamp);
        
        // Generate summary report
        generateSummaryReport(timestamp);
    }
    
    /**
     * Generate detail report
     */
    private void generateDetailReport(String timestamp) throws IOException {
        String filename = REPORT_DIR + "/detail_report_" + timestamp + ".txt";
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("=== Oracle and GaussDB Data Consistency Validation Detail Report ===");
            writer.println("Generated at: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
            writer.println();
            
            Set<String> allKeys = new HashSet<>();
            allKeys.addAll(oracleResults.keySet());
            allKeys.addAll(gaussResults.keySet());
            allKeys.addAll(sqlGenErrors.keySet()); // Include SQL generation errors
            
            for (String key : allKeys) {
                writer.println("Check item: " + key);
                writer.println("=" + "=".repeat(50 + key.length()));
                
                // Check if this item failed during SQL generation
                String sqlGenError = sqlGenErrors.get(key);
                
                if (sqlGenError != null) {
                    writer.println("  Status: [ERROR] SQL generation failed");
                    writer.println("  Error: " + sqlGenError);
                } else {
                    ChecksumResult oracleResult = oracleResults.get(key);
                    ChecksumResult gaussResult = gaussResults.get(key);
                    Long oracleTime = oracleExecutionTimes.get(key);
                    Long gaussTime = gaussExecutionTimes.get(key);
                    
                    // Display Oracle information
                    writer.println("Oracle Database:");
                    String oracleSql = oracleSqlMap.get(key);
                    if (oracleSql != null) {
                        writer.println("  SQL: " + formatSqlForDisplay(oracleSql));
                    }
                    writer.println("  Result: " + (oracleResult != null ? oracleResult.toString() : "Execution failed"));
                    if (oracleTime != null) {
                        writer.println("  Execution time: " + oracleTime + " ms");
                    }
                    
                    Exception oracleError = errors.get(key + "_ORACLE");
                    if (oracleError != null) {
                        writer.println("  Error: " + oracleError.getMessage());
                    }
                    writer.println();
                    
                    // Display GaussDB information
                    writer.println("GaussDB Database:");
                    String gaussSql = gaussSqlMap.get(key);
                    if (gaussSql != null) {
                        writer.println("  SQL: " + formatSqlForDisplay(gaussSql));
                    }
                    writer.println("  Result: " + (gaussResult != null ? gaussResult.toString() : "Execution failed"));
                    if (gaussTime != null) {
                        writer.println("  Execution time: " + gaussTime + " ms");
                    }
                    
                    Exception gaussError = errors.get(key + "_GAUSS");
                    if (gaussError != null) {
                        writer.println("  Error: " + gaussError.getMessage());
                    }
                    writer.println();
                    
                    // Display comparison result
                    if (oracleResult != null && gaussResult != null) {
                        if (oracleResult.equals(gaussResult)) {
                            writer.println("  Status: [PASS] Consistent");
                        } else {
                            writer.println("  Status: [FAIL] Inconsistent");
                            // Show detailed comparison when inconsistent
                            if (oracleResult.count != gaussResult.count) {
                                writer.println("  Count mismatch: Oracle=" + oracleResult.count + ", GaussDB=" + gaussResult.count);
                            }
                            if (oracleResult.checksum != gaussResult.checksum) {
                                writer.println("  Checksum mismatch: Oracle=" + oracleResult.checksum + ", GaussDB=" + gaussResult.checksum);
                            }
                        }
                        
                        // Display performance comparison if both executed successfully
                        // if (oracleTime != null && gaussTime != null) {
                        //     long timeDiff = Math.abs(oracleTime - gaussTime);
                        //     String fasterDb = oracleTime < gaussTime ? "Oracle" : "GaussDB";
                        //     writer.println("  Performance: " + fasterDb + " was faster by " + timeDiff + " ms");
                        // }
                    } else {
                        writer.println("  Status: [ERROR] Execution failed");
                    }
                }
                writer.println();
            }
        }
        System.out.println("Detail report generated: " + filename);
    }
    
    /**
     * Format SQL for display in reports
     */
    private String formatSqlForDisplay(String sql) {
        if (sql == null) return "N/A";
        
        // Replace multiple whitespaces and newlines with single space for compact display
        String compactSql = sql.replaceAll("\\s+", " ").trim();
        
        // If SQL is too long, truncate it and add ellipsis
        //if (compactSql.length() > 200) {
        //    return compactSql.substring(0, 197) + "...";
        //}
        
        return compactSql;
    }
    
    /**
     * Generate summary report
     */
    private void generateSummaryReport(String timestamp) throws IOException {
        String filename = REPORT_DIR + "/summary_report_" + timestamp + ".txt";
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("=== Oracle and GaussDB Data Consistency Validation Summary Report ===");
            writer.println("Generated at: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
            writer.println();
            
            Set<String> allKeys = new HashSet<>();
            allKeys.addAll(oracleResults.keySet());
            allKeys.addAll(gaussResults.keySet());
            allKeys.addAll(sqlGenErrors.keySet()); // Include SQL generation errors
            
            int totalConfiguredCount = allKeys.size();
            int sqlGenErrorCount = sqlGenErrors.size();
            int consistentCount = 0;
            int inconsistentCount = 0;
            int executionErrorCount = 0;
            
            List<String> inconsistentItems = new ArrayList<>();
            List<String> executionErrorItems = new ArrayList<>();
            List<String> sqlGenErrorItems = new ArrayList<>(sqlGenErrors.keySet());
            
            for (String key : allKeys) {
                if (sqlGenErrors.containsKey(key)) {
                    // Skip items with SQL generation errors for execution statistics
                    continue;
                }
                
                ChecksumResult oracleResult = oracleResults.get(key);
                ChecksumResult gaussResult = gaussResults.get(key);
                
                if (oracleResult != null && gaussResult != null) {
                    if (oracleResult.equals(gaussResult)) {
                        consistentCount++;
                    } else {
                        inconsistentCount++;
                        inconsistentItems.add(key);
                    }
                } else {
                    executionErrorCount++;
                    executionErrorItems.add(key);
                }
            }
            
            writer.println("Total configured items: " + totalConfiguredCount);
            writer.println("SQL generation failed: " + sqlGenErrorCount + " items");
            writer.println("Data consistent: " + consistentCount + " items");
            writer.println("Data inconsistent: " + inconsistentCount + " items");
            writer.println("Execution failed: " + executionErrorCount + " items");
            writer.println();
            
            if (!sqlGenErrorItems.isEmpty()) {
                writer.println("SQL generation failed items:");
                for (String item : sqlGenErrorItems) {
                    writer.println("  - " + item);
                }
                writer.println();
            }
            
            if (!inconsistentItems.isEmpty()) {
                writer.println("Inconsistent items:");
                for (String item : inconsistentItems) {
                    writer.println("  - " + item);
                }
                writer.println();
            }
            
            if (!executionErrorItems.isEmpty()) {
                writer.println("Execution failed items:");
                for (String item : executionErrorItems) {
                    writer.println("  - " + item);
                }
                writer.println();
            }
            
            int validItemsCount = totalConfiguredCount - sqlGenErrorCount;
            double successRate = validItemsCount > 0 ? (double) consistentCount / validItemsCount * 100 : 0;
            writer.println("Data consistency rate: " + String.format("%.2f%%", successRate) + 
                         " (based on " + validItemsCount + " items with valid SQL)");
        }
        System.out.println("Summary report generated: " + filename);
    }
    
    /**
     * Get Oracle database connection
     */
    private Connection getOracleConnection() throws SQLException {
        try {
            // Load driver dynamically
            loadDriver(oracleDriverJar, "oracle.jdbc.driver.OracleDriver");
            return DriverManager.getConnection(oracleUrl, oracleUser, oraclePassword);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to Oracle database: " + e.getMessage(), e);
        }
    }
    
    /**
     * Get GaussDB database connection
     */
    private Connection getGaussConnection() throws SQLException {
        try {
            // Load driver dynamically
            loadDriver(gaussDriverJar, "org.postgresql.Driver");
            return DriverManager.getConnection(gaussUrl, gaussUser, gaussPassword);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to GaussDB database: " + e.getMessage(), e);
        }
    }
    
    /**
     * Load database driver dynamically
     */
    private void loadDriver(String jarPath, String driverClass) throws Exception {
        if (jarPath != null && !jarPath.trim().isEmpty()) {
            // Check if driver jar exists, prioritize checking lib directory
            File jarFile = new File(jarPath);
            if (!jarFile.exists()) {
                // If direct path does not exist, try to find in lib directory
                File libJarFile = new File("lib/" + jarPath);
                if (!libJarFile.exists()) {
                    throw new FileNotFoundException("Database driver file not found: " + jarPath + " (also searched in lib directory)");
                }
            }
        }
        
        // Load driver class
        Class.forName(driverClass);
    }
    
    /**
     * Checksum result class to store both count and checksum values
     */
    private static class ChecksumResult {
        final long count;
        final long checksum;
        
        ChecksumResult(long count, long checksum) {
            this.count = count;
            this.checksum = checksum;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ChecksumResult that = (ChecksumResult) obj;
            return count == that.count && checksum == that.checksum;
        }
        
        @Override
        public String toString() {
            return "count=" + count + ", checksum=" + checksum;
        }
    }
    
    /**
     * Custom SQL class
     */
    private static class CustomSql {
        final String name;
        final String sql;
        
        CustomSql(String name, String sql) {
            this.name = name;
            this.sql = sql;
        }
    }
    
    /**
     * Check task class
     */
    private static class CheckTask {
        final String name;
        final String oracleSql;
        final String gaussSql;
        
        CheckTask(String name, String oracleSql, String gaussSql) {
            this.name = name;
            this.oracleSql = oracleSql;
            this.gaussSql = gaussSql;
        }
    }
}
