# Oracle与GaussDB数据一致性校验工具

## 概述

本工具是一款用于校验Oracle与GaussDB之间数据一致性的工具。通过MD5算法，对两个数据库中的相同表进行哈希计算和并比较结果，从而判断数据是否一致。

## 原理

1. 连接ORACLE，使用ORACLE的dbms_sql解析每个字段的字段名和字段类型
2. 将每个字段按照不同的数据类型进行格式化，拼接成长文本（ORACLE中使用json returning blob，可超4000长度）
3. 计算长文本的MD5，并切割成4个有符号整型值相加，将整列进行sum求和
4. 在两个数据库中分别执行相同逻辑的SQL，比较结果数值
5. 如果两个数值相等，则数据一致；否则数据不一致

## 文件说明

- `DataConsistencyChecker.java` - 主程序文件
- `config.yml` - YAML格式配置文件
- `config.yml.example` - 详细的YAML配置示例
- `build.sh` / `build.bat` - 编译脚本
- `run.sh` / `run.bat` - 执行脚本
- `README.md` - 说明文档

## 环境要求

- JDK 1.8 或更高版本
- cmd（Windows）或 Bash（Linux）
- Oracle数据库连接权限（需要DBA_TABLES视图访问权限，dbms_crypto执行权限）
- GaussDB数据库连接权限
- 相应的JDBC驱动jar包
- SnakeYAML库（放置在lib目录下）
- ORACLE服务端 19c以上 (12c实测不支持)
- GaussDB服务端存储中文字符集仅支持UTF8 (ORACLE源端无存储字符集要求，因为数据转成JSON时会自动转成UTF8)

## 安装步骤

1. **准备依赖库**
   - 创建lib目录
   - 下载并放置SnakeYAML库到lib目录
   - 下载并放置Oracle JDBC驱动到lib目录
   - 下载并放置GaussDB JDBC驱动到lib目录

2. **配置数据库连接**
   - 复制 `config.yml.example` 为 `config.yml`
   - 编辑 `config.yml` 文件
   - 填写Oracle和GaussDB的连接信息
   - 配置需要检查的表名（支持schema.table格式）或自定义SQL

3. **编译程序**
   ```bash
   # Windows
   build.bat
   
   # Linux
   ./build.sh
   ```

4. **运行程序**
   ```bash
   # Windows - 使用默认config.yml
   run.bat
   
   # Windows - 使用指定配置文件
   run.bat my_config.yml
   
   # Linux - 使用默认config.yml
   ./run.sh
   
   # Linux - 使用指定配置文件
   ./run.sh my_config.yml
   ```

## 命令行参数

工具支持通过命令行参数指定配置文件：

```bash
# 直接使用Java运行
java -cp ".;lib/*" DataConsistencyChecker [config_file]

# 使用脚本运行
.\run.bat [config_file]     # Windows
./run.sh [config_file]      # Linux
```

如果不指定配置文件参数，将默认使用 `config.yml`。

## 配置文件说明

配置文件使用YAML格式，具有更好的可读性和灵活性：

### 数据库连接配置

```yaml
databases:
  oracle:
    url: "jdbc:oracle:thin:@localhost:1521:orcl"
    user: "username"
    password: "password"
    driver_jar: "ojdbc8-12.2.0.1.jar"
  
  gauss:
    url: "jdbc:postgresql://localhost:5432/dbname"
    user: "username"
    password: "password"
    driver_jar: "gsjdbc4-1.1.jar"
```

### 检查范围配置

```yaml
performance:
  thread_count: 4

check_scope:
  # Schema映射配置：Oracle schema -> GaussDB schema
  schema_mapping:
    system: public        # Oracle的system schema映射到GaussDB的public schema
    hr: hr_schema         # Oracle的hr schema映射到GaussDB的hr_schema
  
  # 字段名映射（可选）：解决GaussDB中保留字或不同命名的问题
  # 配置格式：Oracle字段名 -> GaussDB字段名
  column_mapping:
    limit: limitval             # Oracle字段LIMIT在GaussDB中改名为limitval
    order: order_col            # Oracle字段ORDER在GaussDB中改名为order_col
    
  # Schema列表：自动从Oracle中查询指定schema下的所有表
  schemas:
    - system              # 自动查询system schema下的所有表并加入检查范围
    - hr                  # 自动查询hr schema下的所有表并加入检查范围
  
  # 表列表（支持换行，支持schema.table格式）
  tables:
    - hr.employees
    - hr.departments
    - sales.customers
  
  # 自定义SQL（支持多行，不受分号影响）
  custom_sqls:
    - name: "近期订单数据"
      sql: |
        SELECT * FROM finance.orders 
        WHERE order_date >= '2023-01-01' 
          AND status IN ('PENDING', 'PROCESSING')
    
    - name: "包含分号的查询"
      sql: |
        SELECT 
          CASE 
            WHEN price > 100 THEN 'High; Premium'
            ELSE 'Low; Basic'
          END as category
        FROM products
```

## 使用示例

### 1. 检查指定schema下的所有表

```yaml
check_scope:
  schema_mapping:
    system: public
    hr: hr_schema
  
  schemas:
    - system              # 自动发现system schema下的所有表
    - hr                  # 自动发现hr schema下的所有表
  
  tables: []              # 可为空，由schemas自动填充
  custom_sqls: []
```

### 2. 检查特定表

```yaml
check_scope:
  schema_mapping:
    hr: hr_schema
    system: public
  
  schemas: []             # 不使用自动发现
  
  tables:
    - hr.employees
    - hr.departments
    - sales.customers
  
  custom_sqls: []
```

### 3. 检查自定义查询

```yaml
check_scope:
  schemas: []
  tables: []
  custom_sqls:
    - name: "大表分区数据"
      sql: |
        SELECT * FROM large_table 
        WHERE partition_key = '2023'
    - name: "汇总统计"
      sql: "SELECT count(*) FROM summary_table"
```

### 4. 混合检查（推荐）

```yaml
check_scope:
  schema_mapping:
    system: public
    hr: hr_schema
  
  schemas:
    - system              # 自动发现system schema下的所有表
  
  tables:
    - hr.specific_table   # 另外指定特定表
  
  custom_sqls:
    - name: "近期数据"
      sql: |
        SELECT * FROM sales.large_table 
        WHERE create_time > sysdate - 30
```

## 执行流程

1. **Schema扩展**: 如果配置了schema列表，从Oracle中查询并自动添加表到检查列表
2. **表排序**: 根据Oracle统计信息按表大小排序（从大到小）
3. **SQL生成**: 为每个表和自定义SQL生成格式化的checksum查询
4. **并发执行**: 同时在Oracle和GaussDB中执行相应的查询
5. **结果比较**: 比较两个数据库的checksum结果
6. **报告生成**: 生成详细报告和汇总报告

## 错误处理

工具具有强大的错误处理能力：

- **不中断执行**: 即使某些表或SQL执行失败，程序会继续处理其他项目
- **详细错误记录**: 所有错误都会记录在日志和报告中
- **错误分类**: 区分SQL生成错误和执行错误
- **graceful degradation**: 程序在遇到错误时优雅降级，而不是崩溃

### 错误类型

1. **SQL生成错误**: 表不存在、字段无效、语法错误等
2. **执行错误**: 数据库连接问题、权限不足、查询超时等
3. **配置错误**: 配置文件格式错误、连接参数无效等

## 报告说明

工具会在 `reports` 目录下生成两种报告：

### 详细报告 (detail_report_yyyyMMdd_HHmmss.txt)
- 每个检查项目的具体结果
- Oracle和GaussDB的checksum值
- 一致性状态
- 错误信息（如有）

### 汇总报告 (summary_report_yyyyMMdd_HHmmss.txt)
- 总配置项目数
- SQL生成失败项目数
- 数据一致、不一致、执行失败的统计
- 数据一致性率（基于有效SQL的一致性百分比）
- 各类问题项目列表

## 注意事项

1. **权限要求**: 确保Oracle用户有DBA_TABLES及查询表的权限、dbms_crypto的执行权限
2. **网络连接**: 确保可以同时连接到Oracle和GaussDB
3. **资源使用**: 并发线程数建议根据数据库性能调整
4. **数据类型**: 工具会跳过某些不支持的数据类型（如BLOB、CLOB等）
5. **时间格式**: 日期时间字段会被格式化为统一格式进行比较
6. **YAML格式优势**:
   - 支持多行SQL，便于阅读和维护
   - 不受SQL中分号影响
   - 支持注释，便于文档化
   - 层次结构清晰，易于理解
   - 表名支持换行列表，便于管理
   - 支持schema级别的自动表发现功能

## 故障排除

### 常见错误

1. **编译失败**
   - 检查JDK版本是否为1.8+
   - 确保源文件编码为UTF-8

2. **连接失败**
   - 检查数据库连接字符串是否正确
   - 确保JDBC驱动jar包和SnakeYAML库存在且正确
   - 验证用户名密码是否正确

3. **权限错误**
   - 确保Oracle用户有dbms_crypto的执行权限
   - 确保Oracle用户有DBA_TABLES视图的SELECT权限
   - 检查对所有需要校验表的SELECT权限

4. **内存不足**
   - 减少并发线程数
   - 分批处理大表

### 日志查看

程序运行时会在控制台输出详细的执行信息，包括：
- 初始化状态
- 各项检查的执行进度
- 错误信息
- 最终结果统计

## 技术支持

如遇到问题，请检查：
1. 配置文件是否正确
2. 数据库连接是否正常
3. 权限是否充足
4. JDBC驱动是否匹配

更多技术细节请参考源代码注释。
