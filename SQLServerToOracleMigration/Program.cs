using ExtensionMethods;
using Oracle.ManagedDataAccess.Client;
using SQLServerToOracleMigrationUtility;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;

internal class Program
{
    private static void Main(string[] args)
    {
        int currentErrorCount = 0;
        int maxErrosRepetition = 10;
        int varcharLimit = 2000;
        DateTime startProcessing = DateTime.Now;

        string sql_host = string.Empty;
        string sql_catalog = string.Empty;
        string sql_userId = string.Empty;
        string sql_password = string.Empty;
        string ora_host = string.Empty;
        string ora_port = string.Empty;
        string ora_sid = string.Empty;
        string ora_userId = string.Empty;
        string ora_password = string.Empty;

        string sql_server_connection_string = string.Empty;
        string oracle_connection_string = string.Empty;
        string quote = "```";
        string bcp_params = "\"{0}\" QUERYOUT BCP\\{1}.dat -o Log\\{1}.log -S{2} -d {3} -U{4} -P{5} -C65001 -t \"" + quote + "," + quote + "\" -r \"" + quote + "\\n" + quote + "\" -w";
        Encoding utf8pure = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        bool tem_erro = false;
        StringBuilder bcp_scripts = new();
        StringBuilder sqlldr_scripts = new();
        StringBuilder update_seq_scripts = new();
        StringBuilder after_all_data_load = new();
        StringBuilder log_processamento = new();
        StringBuilder create_constraints = new();

        bool recriar = false;
        int maxRowsToInsert = 100000;
        string top = string.Empty;
        string andTable = string.Empty;
        if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP"))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP")); }
        if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "Log"))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "Log")); }

        string[] caracNaoPermitidos = new string[] { "$", "#", "_" };
        bool insertMoreRows = false;

        /* 
        en-US
        Params:
            sqlhost = SQL Server name or IP address, e.g., sqlhost=sql or sqlhost=myserver
            sqlcatalog = SQL Server database, e.g., sqlcatalog=mydatabase or sqlcatalog mydatabase
            sqlusr = SQL Server user, e.g., sqlusr=user or sqlusr user
            sqlpwd = SQL Server password, e.g., sqlpwd=password or sqlpwd password
            orahost = Oracle server name or IP address, e.g., orahost=192.168.0.2 or orahost 192.168.0.2
            oraport = Oracle port, e.g., oraport=1521 or oraport 1521
            orasid = Oracle Service ID, e.g., orasid=oraserviceId or orasid oraserviceId
            orausr = Oracle user, e.g., orausr=scott or orausr scott
            orapwd = Oracle password, e.g., orapwd=tigger or orapwd tigger
            recriar = true if you want to recreate the tables / false if you don't want to, e.g., recriar=true or recriar true
            append = true, whether you insert without truncating / false, whether you want to skip the table, e.g., append=true or append true
            -m = maxRowsToInsert, e.g., -m 100 (maximum of 100 records) 

        pt-BR
        Params:
            sqlhost = nome ou ip do servidor do SQL Server, ex: sqlhost=192.168.0.1 ou sqlhost meuservidor
            sqlcatalog = banco de dados do SQL Server, ex: sqlcatalog=meubanco ou sqlcatalog meubanco
            sqlusr = usuário do SQL Server, ex: sqlusr=usuario ou sqlusr usuario
            sqlpwd = senha do SQL Server, sqlpwd=senha ou sqlpwd senha
            orahost = nome ou ip do servidor Oracle, ex: orahost=192.168.0.2 ou orahost 192.168.0.2
            oraport = porta Oracle, ex: oraport=1521 ou oraport 1521
            orasid = Id do Serviço Oracle, ex: orasid=oraserviceId ou orasid oraserviceId
            orausr = usuário Oracle, ex: orausr=scott ou orausr scott
            orapwd = Senha Oracle, ex: orapwd=tigger ou orapwd tigger
            recriar = true se deseja recirar as tabelas / falso caso não deseje, ex: recriar=true ou recriar true
            append = true se inserir sem truncar / falso caso queira pular a tabela, ex: append=true ou append true
            -m = maxRowsToInsert ex: -m 100 (no máximo 100 regitros)
        */
        StringBuilder allParams = new();
        int paramMandatory = 0;
        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg[..1] == "-")
            {
                switch (arg[..2].ToLower())
                {
                    case "-m":
                        var param = arg.Remove(0, 2).Trim();
                        if (param.Length == 0) { if (i < args.Length) { i++; param = args[i]; } }
                        if (param.Length == 0) { break; }
                        _ = int.TryParse(param, out maxRowsToInsert);
                        allParams.Append($" -m={param},");
                        break;
                }
            }
            else
            {
                string argpure = arg.ToString()[..3].ToLower();
                if (argpure == "top") { top = arg; allParams.Append($" \"top..\"=\"{top}\","); }
                if (argpure == "and") { andTable = arg; allParams.Append($" \"and...\"=\"{andTable}\","); }
                string argtype2 = arg.ToString()[..6].ToLower();
                if (argtype2 == "sqlhos") { var param = arg.Remove(0, 7); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } sql_host = param; paramMandatory++; }
                if (argtype2 == "sqlcat") { var param = arg.Remove(0, 10); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } sql_catalog = param; paramMandatory++; }
                if (argtype2 == "sqlusr") { var param = arg.Remove(0, 6); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } sql_userId = param; paramMandatory++; }
                if (argtype2 == "sqlpwd") { var param = arg.Remove(0, 6); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } sql_password = param; paramMandatory++; }
                if (argtype2 == "orahos") { var param = arg.Remove(0, 7); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } ora_host = param; paramMandatory++; }
                if (argtype2 == "orapor") { var param = arg.Remove(0, 7); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } ora_port = param; paramMandatory++; }
                if (argtype2 == "orasid") { var param = arg.Remove(0, 6); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } ora_sid = param; paramMandatory++; }
                if (argtype2 == "orausr") { var param = arg.Remove(0, 6); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } ora_userId = param; paramMandatory++; }
                if (argtype2 == "orapwd") { var param = arg.Remove(0, 6); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } ora_password = param; paramMandatory++; }
                if (argtype2 == "recria") { var param = arg.Remove(0, 7); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } recriar = param == "true"; allParams.Append($" recriar={param},"); }
                if (argtype2 == "append") { var param = arg.Remove(0, 6); if (param.Contains('=')) { param = param.Remove(0, 1).Trim(); } else { i++; param = args[i]; } insertMoreRows = param == "true"; allParams.Append($" append={param},"); }
            }
        }
        if (paramMandatory < 9)
        {
            Console.WriteLine("Please provide the 9 mandatory parameters:\n\tsqlhost = SQL Server name or IP address, e.g., sqlhost=192.168.0.1 or sqlhost=myserver\n\tsqlcatalog = SQL Server database, e.g., sqlcatalog=mydatabase or sqlcatalog mydatabase\n\tsqlusr = SQL Server user, e.g., sqlusr=user or sqlusr user\n\tsqlpwd = SQL Server password, e.g., sqlpwd=password or sqlpwd password\n\torahost = Oracle server name or IP address, e.g., orahost=192.168.0.2 or orahost 192.168.0.2\n\toraport = Oracle port, e.g., oraport=1521 or oraport 1521\n\torasid = Oracle Service ID, e.g., orasid=oraserviceId or orasid oraserviceId\n\torausr = Oracle user, e.g., orausr=scott or orausr scott\n\torapwd = Oracle password, e.g., orapwd=tigger or orapwd tigger\n\nAnd the optional ones are:\n\trecriar = true if you want to recreate the tables / false if you don't want to, e.g., recriar=true or recriar true (Default is false)\n\t-m = maxRowsToInsert, e.g., -m 100 (maximum of 100 records).");
            Environment.Exit(0);
        }
        if (allParams.Length > 0) { allParams.Length--; }
        Console.WriteLine($"Running migration proccess using follow parameters {allParams}");
        Console.WriteLine("");

        sql_server_connection_string = $"Data Source={sql_host};Initial Catalog={sql_catalog};User ID={sql_userId};Password={sql_password};MultipleActiveResultSets=True";
        oracle_connection_string = $"Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST={ora_host})(PORT={ora_port}))(CONNECT_DATA=(SERVICE_NAME={ora_sid})));User Id={ora_userId};Password={ora_password};Persist Security Info=True;enlist=false;pooling=false;";

        using SqlConnection sql_server_connection = new(sql_server_connection_string);
        sql_server_connection.Open();

        try
        {
            using SqlCommand sql_server_command = new($"SELECT {top} TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' {andTable} ORDER BY TABLE_NAME", sql_server_connection);
            using SqlDataReader sql_server_reader = sql_server_command.ExecuteReader();

            using OracleConnection oracle_connection = new(oracle_connection_string);
            oracle_connection.Open();
            while (sql_server_reader.Read())
            {
                string table_schema = sql_server_reader.GetString(0);
                string table_name = sql_server_reader.GetString(1);
                int total = ContaRegistrosDeTabelaSQL(sql_server_connection, table_name);

                bool badTableName = false;
                bool table_exists = false;

                try
                { badTableName = caracNaoPermitidos.Contains(table_name[..1]); }
                catch (IndexOutOfRangeException)
                { badTableName = false; Environment.Exit(0); }

                if (!badTableName)
                {

                    ShowMessage($"Processing table {table_name}...");

                    using (OracleCommand oracle_command = new($"SELECT COUNT(*) FROM user_tables WHERE UPPER(table_name) = '{table_name.ToUpper()}'", oracle_connection))
                    {
                        table_exists = Convert.ToInt32(oracle_command.ExecuteScalar()) > 0;

                        if (table_exists && recriar)
                        {
                            using (OracleCommand oracle_disable_constraints_command = new($"SELECT constraint_name FROM all_constraints WHERE UPPER(table_name)='{table_name.ToUpper()}' AND constraint_type = 'R'", oracle_connection))
                            {
                                using OracleDataReader oracle_disable_constraints_reader = oracle_disable_constraints_command.ExecuteReader();
                                while (oracle_disable_constraints_reader.Read())
                                {
                                    string constraint_name = oracle_disable_constraints_reader.GetString(0);
                                    try
                                    {
                                        string disableConstraints = $"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name}";
                                        using OracleCommand disable_constraint_command = new(disableConstraints, oracle_connection);
                                        disable_constraint_command.ExecuteNonQuery();
                                    }
                                    catch
                                    { ShowMessage($"  Error disabling constraint {constraint_name}"); Environment.Exit(0); }
                                }
                            }

                            ShowMessage($"    Dropping the table, already existing, {table_name} => DROP TABLE {table_name}...");
                            try
                            {
                                string dropTable = $"DROP TABLE {table_name}";
                                using OracleCommand drop_table_command = new(dropTable, oracle_connection);
                                drop_table_command.ExecuteNonQuery();
                            }
                            catch
                            { ShowMessage($"    Error dropping table"); Environment.Exit(0); }
                            oracle_connection.FlushCache();
                        }
                        else
                        {
                            if (table_exists)
                            { ShowMessage("    Skipping, already existing table."); }
                        }
                    }

                    List<Column> cols = new();
                    string sqlTableSchema = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT, " +
                        "COLS.IS_NULLABLE, CASE WHEN is_identity = 1 THEN 1 ELSE 0 END AS IS_IDENTITY, ISNULL(PK.PRIMARYKEY, 0) AS PRIMARYKEY, ORDINAL_POSITION " +
                        "FROM INFORMATION_SCHEMA.COLUMNS COLS JOIN SYS.columns C ON C.object_id = object_id(COLS.TABLE_NAME) AND C.name = COLUMN_NAME " +
                        "LEFT JOIN (SELECT table_name AS TABELA, column_name AS COLUNA, 1 AS PRIMARYKEY FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                        "WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1) PK ON PK.TABELA=COLS.TABLE_NAME AND PK.COLUNA=COLS.COLUMN_NAME " +
                        $"WHERE UPPER(TABLE_NAME)='{table_name.ToUpper()}' ORDER BY ORDINAL_POSITION ";

                    using SqlCommand sql_server_table_schema_command = new(sqlTableSchema, sql_server_connection);
                    using SqlDataReader table_schema_reader = sql_server_table_schema_command.ExecuteReader();
                    StringBuilder create_table_query = new();
                    StringBuilder sequence_statment = new();
                    List<OracleParameter> parameters = new();

                    create_table_query.Append($"CREATE TABLE {table_name.ToUpper()} (");
                    bool hasPrimaryKey = false;
                    bool hasIdentity = false;
                    while (table_schema_reader.Read())
                    {
                        string column_name = table_schema_reader.GetString(0);
                        string data_type = table_schema_reader.GetString(1).ToLower();
                        int? data_size = table_schema_reader.IsDBNull(2) ? null : table_schema_reader.GetInt32(2);
                        int? numeric_precision = table_schema_reader.IsDBNull(3) ? null : table_schema_reader.GetByte(3);
                        int? numeric_scale = table_schema_reader.IsDBNull(4) ? null : table_schema_reader.GetInt32(4);
                        object? value_default = table_schema_reader.IsDBNull(5) ? null : table_schema_reader.GetValue(5);
                        string is_nullable = table_schema_reader.GetString(6);
                        bool is_identity = table_schema_reader.GetInt32(7) == 1;
                        bool is_primarykey = table_schema_reader.GetInt32(8) == 1;
                        int position = table_schema_reader.GetInt32(9);

                        if (is_primarykey) { hasPrimaryKey = true; }
                        if (is_identity) { hasIdentity = true; }

                        cols.Add(new Column()
                        {
                            ColumnName = column_name,
                            DataType = data_type,
                            DataSize = data_size,
                            NumericPrecision = numeric_precision,
                            NumericScale = numeric_scale,
                            DefaultValue = value_default,
                            IsNullable = is_nullable,
                            IsIdentity = is_identity,
                            IsPrimarykey = is_primarykey,
                            Position = position,
                        });
                    }
                    if (!table_schema_reader.IsClosed) { table_schema_reader.Close(); }
                    if (!hasPrimaryKey && !hasIdentity)
                    {
                        foreach (Column c in cols)
                        {
                            if (c.ColumnName[..2].ToLower() == "id" || c.ColumnName[..2].ToLower() == "cd")
                            {
                                c.IsPrimarykey = true;
                                if (c.DataType == "int" || c.DataType == "bigint" || c.DataType == "integer") { c.IsIdentity = true; }
                                break;
                            }
                        }
                        if (!cols.Where(c => c.IsPrimarykey).Any())
                        { if (cols[0].DataSize > 0 && cols[0].DataSize <= 10) { cols[0].IsPrimarykey = true; } }
                    }

                    //Column? primaryKey = cols.Where(c => c.IsPrimarykey || c.IsIdentity).FirstOrDefault() ?? throw new Exception($"A tabela {table_name} não possui uma coluna Primary Key, resolva este problema e tente novamente.");
                    Column? primaryKey = cols.Where(c => c.IsPrimarykey || c.IsIdentity).FirstOrDefault();
                    if (primaryKey != null)
                    { if (cols.Where(c => c.IsPrimarykey).FirstOrDefault() == null) { cols[cols.FindIndex(c => c.Position == primaryKey.Position)].IsPrimarykey = true; } }
                    int qtdPK = cols.Where(c => c.IsPrimarykey).Count();

                    foreach (Column c in cols)
                    {
                        if (c.DataType == "datetime" && c.DefaultValue is string)
                        {
                            string? stValue = c.DefaultValue?.ToString();
                            if (stValue != null)
                            {
                                string defaultValue = stValue.Replace("(", "").Replace(")", "");
                                if (defaultValue.Length > 10)
                                { c.DefaultValue = $"TO_DATE({defaultValue},'YYYY-MM-DD HH24:MI:SS')"; }
                                else
                                { c.DefaultValue = $"TO_DATE({defaultValue},'YYYY-MM-DD')"; }
                            }
                        }

                        StringBuilder complement = new();
                        //if (c.IsIdentity) { complement.Append(" GENERATED BY DEFAULT ON NULL AS IDENTITY"); }
                        if (c.IsIdentity)
                        {
                            using OracleCommand check_sequence_statment = new($"SELECT COUNT(1) FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME)='SEQ_{table_name.ToUpper()}'", oracle_connection);
                            if (Convert.ToInt32(check_sequence_statment.ExecuteScalar()) == 0)
                            {
                                try
                                {
                                    sequence_statment.AppendLine($"CREATE SEQUENCE SEQ_{table_name}");
                                    using OracleCommand new_sequence_statment = new(sequence_statment.ToString(), oracle_connection);
                                    new_sequence_statment.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    ShowMessage($"    An error occurred: {ex.Message}");
                                    if (ex.StackTrace != null)
                                    {
                                        string[] stackTrace = ex.StackTrace.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                                        for (int i = 0; i < stackTrace.Length; i++) { ShowMessage($"           {(i == 0 ? "StackTrace: " : "            ")}{stackTrace[i]}"); }
                                    }
                                }
                            }
                            complement.Append($" DEFAULT SEQ_{table_name}.NEXTVAL");
                        }
                        //if (c.IsPrimarykey && qtdPK == 1) { complement.Append(" PRIMARY KEY"); }
                        if (c.DefaultValue != null) { complement.Append($" DEFAULT {c.DefaultValue}"); }
                        if (c.IsNullable == "NO") { complement.Append(" NOT NULL"); }

                        switch (c.DataType)
                        {
                            case "tinyint":
                            case "smallint":
                            case "int":
                            case "integer":
                            case "decimal":
                            case "numeric":
                            case "smallmoney":
                            case "money":
                            case "bigint":
                                create_table_query.Append($"\n\t{c.ColumnName} number({c.NumericPrecision},{c.NumericScale}){complement},");
                                break;
                            case "real":
                                create_table_query.Append($"\n\t{c.ColumnName} binary_float{complement},");
                                break;
                            case "float":
                                create_table_query.Append($"\n\t{c.ColumnName} binary_double({c.NumericPrecision},{c.NumericScale}){complement},");
                                break;
                            case "binary":
                                create_table_query.Append($"\n\t{c.ColumnName} raw({c.DataSize}){complement},");
                                break;
                            case "uniqueidentifier":
                                create_table_query.Append($"\n\t{c.ColumnName} raw(16){complement},");
                                break;
                            case "image":
                            case "varbinary":
                                create_table_query.Append($"\n\t{c.ColumnName} long raw{complement},");
                                break;
                            case "bit":
                                create_table_query.Append($"\n\t{c.ColumnName} number(1){complement},");
                                break;
                            case "varchar":
                            case "nvarchar":
                                if (c.DataSize > 0 && c.DataSize <= varcharLimit)
                                { create_table_query.Append($"\n\t{c.ColumnName} nvarchar2({c.DataSize}){complement},"); }
                                else
                                { create_table_query.Append($"\n\t{c.ColumnName} nclob{complement},"); }
                                break;
                            case "text":
                                create_table_query.Append($"\n\t{c.ColumnName} nclob{complement},");
                                break;
                            case "char":
                                create_table_query.Append($"\n\t{c.ColumnName} char({c.DataSize}){complement},");
                                break;
                            case "date":
                            case "datetime":
                            case "smalldatetime":
                                create_table_query.Append($"\n\t{c.ColumnName} date{complement},");
                                break;
                            case "xml":
                                create_table_query.Append($"\n\t{c.ColumnName} xmltype{complement},");
                                break;
                            default:
                                create_table_query.Append($"\n\t{c.ColumnName} {c.DataSize}{complement},");
                                break;
                        }
                    }

                    if (qtdPK >= 1)
                    {
                        create_table_query.Append($"\n\tCONSTRAINT PK_{table_name} PRIMARY KEY (");
                        foreach (Column c in cols.Where(c => c.IsPrimarykey))
                        { create_table_query.Append($"{c.ColumnName},"); }
                        create_table_query.Length--;
                        create_table_query.Append("),");
                    }
                    create_table_query.Length--;
                    create_table_query.AppendLine("\n)");

                    if (!table_exists || recriar)
                    {
                        using OracleCommand oracle_create_table_command = new(create_table_query.ToString(), oracle_connection);
                        oracle_create_table_command.ExecuteNonQuery();
                    }

                    if (sequence_statment.Length > 0)
                    {
                        sequence_statment.AppendLine("");
                        sequence_statment.AppendLine(create_table_query.ToString());
                        create_table_query = new(sequence_statment.ToString());
                        sequence_statment = new();
                    }
                    SaveFileUTF8(create_table_query, $"{Path.Combine(Environment.CurrentDirectory, "Scripts")}", $"{table_name}.sql");
                    create_table_query = new();

                    int totalDestination = AccountTableRecordsOracle(oracle_connection, table_name);

                    if (CheckIfTypeColumnExists("image", cols) || CheckIfTypeColumnExists("xml", cols) || CheckIfTypeColumnExists("nclob", cols))
                    { ExecuteBCPComExtracaoDeArquivos(table_schema, table_name, cols, sql_server_connection, total); }
                    else { ExecuteBCP(table_schema, table_name, cols, total); }

                    if (total > 0 && totalDestination < total)
                    {
                        // Inserção de dados
                        //if (total <= maxRowsToInsert || maxRowsToInsert == 0 || HasDataTypeColumn("image", cols))
                        bool executaSqlLoader = !CheckIfTypeColumnExists("image", cols);
                        if (!executaSqlLoader)
                        {
                            SqlCommand sql_server_table_command = new($"SELECT * FROM {table_name}", sql_server_connection);

                            if (!table_exists || insertMoreRows)
                            {
                                int rows = 0;
                                int completed = 0;

                                ShowMessage($"    Table {table_name} being loaded by row-by-row insert...");
                                ShowMessage("    ", false);
                                using SqlDataReader table_reader = sql_server_table_command.ExecuteReader();
                                while (table_reader.Read())
                                {
                                    rows++;
                                    int pc = (int)(rows / (double)total * 100) / 10;
                                    if (pc > completed) { for (int i = completed; i < pc; i++) { ShowMessage("#", false); } completed = pc; }
                                    ExecuteInclusionOfRecords(table_name, table_reader, oracle_connection, primaryKey, cols);
                                }
                                if (!table_reader.IsClosed) { table_reader.Close(); }
                                for (int i = completed; i < 10; i++) { ShowMessage("#", false); }
                                oracle_connection.FlushCache();
                            }
                            ShowMessage("");
                        }
                        else
                        { ShowMessage($"    Table {table_name} being loaded via SQL*Loader..."); }

                        if (CheckIfTypeColumnExists("image", cols) || CheckIfTypeColumnExists("xml", cols) || CheckIfTypeColumnExists("nclob", cols))
                        { ExecuteSQLLoader(cols, table_name, executaSqlLoader, oracle_connection, (CheckIfTypeColumnExists("image", cols) || CheckIfTypeColumnExists("xml", cols) || CheckIfTypeColumnExists("nclob", cols)), (CheckIfTypeColumnExists("image", cols) ? "image" : "") + (CheckIfTypeColumnExists("xml", cols) ? "xml" : "") + (CheckIfTypeColumnExists("nclob", cols) ? ".txt" : "")); }
                        else
                        { ExecuteSQLLoader(cols, table_name, executaSqlLoader, oracle_connection); }

                        if (cols.Where(c => c.IsIdentity).Any())
                        {
                            string query = $"SELECT COUNT(1) FROM USER_SEQUENCES WHERE UPPER(SEQUENCE_NAME)='SEQ_{table_name.ToUpper()}'";
                            OracleCommand query_check_exists = new(query, oracle_connection);
                            if (Convert.ToInt32(query_check_exists.ExecuteScalar()) == 0)
                            {
                                update_seq_scripts.AppendLine(query);
                                Column pk = cols.Where(c => c.IsIdentity).First();
                                query = $"SELECT NVL(MAX({pk.ColumnName}),0) FROM {table_name.ToUpper()}";
                                update_seq_scripts.AppendLine(query);
                                OracleCommand get_current_val = new(query, oracle_connection);
                                int currentVal = Convert.ToInt32(get_current_val.ExecuteScalar());
                                if (currentVal > 0)
                                {
                                    query = $"ALTER SEQUENCE SEQ_{table_name} INCREMENT BY {currentVal}";
                                    update_seq_scripts.AppendLine(query);
                                    OracleCommand set_current_val = new(query, oracle_connection);
                                    set_current_val.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    else
                    {
                        ShowMessage($"    Table {(total == 0 ? "empty" : "already populated")}.");
                    }
                    ShowMessage($"");
                }
            }
            if (!sql_server_reader.IsClosed) { sql_server_reader.Close(); }
            if (!tem_erro) { LinkingAsReferences(sql_server_connection, oracle_connection); }
            if (oracle_connection.State == ConnectionState.Open) { oracle_connection.Close(); }
        }
        catch (Exception ex)
        {
            ShowMessage($"    An error occurred: {ex.Message}");
            if (ex.StackTrace != null)
            {
                string[] stackTrace = ex.StackTrace.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                for (int i = 0; i < stackTrace.Length; i++) { ShowMessage($"           {(i == 0 ? "StackTrace: " : "            ")}{stackTrace[i]}"); }
            }
        }
        finally
        {
            if (sql_server_connection.State == ConnectionState.Open) { sql_server_connection.Close(); }
            if (bcp_scripts.Length > 0) { SaveFileUTF8(bcp_scripts, $"{Path.Combine(Environment.CurrentDirectory, "Scripts")}", $"_1_bcp.bat"); }
            if (sqlldr_scripts.Length > 0) { SaveFileUTF8(sqlldr_scripts, $"{Path.Combine(Environment.CurrentDirectory, "Scripts")}", $"_2_sqlloader.bat"); }
            if (update_seq_scripts.Length > 0) { SaveFileUTF8(update_seq_scripts, $"{Path.Combine(Environment.CurrentDirectory, "Scripts")}", $"_3_run_after_sqlloader.sql"); }
            if (create_constraints.Length > 0) { SaveFileUTF8(create_constraints, $"{Path.Combine(Environment.CurrentDirectory, "Scripts")}", $"_4_create_constraints.sql"); }
            if (after_all_data_load.Length > 0) { SaveFileUTF8(after_all_data_load, $"{Path.Combine(Environment.CurrentDirectory, "Log")}", $"after_data_load.log"); }

            DateTime finishProcessing = DateTime.Now;
            ShowMessage("");
            ShowMessage($"Processing started at {startProcessing:dd/MM/yyyy HH:mm:ss} and ended at {finishProcessing:dd/MM/yyyy HH:mm:ss}, totaling {((int)finishProcessing.Subtract(startProcessing).TotalMinutes == 0 ? (int)finishProcessing.Subtract(startProcessing).TotalSeconds : (int)finishProcessing.Subtract(startProcessing).TotalMinutes)} {((int)finishProcessing.Subtract(startProcessing).TotalMinutes == 0 ? " seconds" : " minutes")} of execution ({(int)finishProcessing.Subtract(startProcessing).TotalHours}:{finishProcessing.Subtract(startProcessing).Minutes}:{finishProcessing.Subtract(startProcessing).Seconds}).");
            if (log_processamento.Length > 0) { SaveFileUTF8(log_processamento, $"{Path.Combine(Environment.CurrentDirectory, "Log")}", $"log_processamento{DateTime.Now:yyyyMMddHHmmss}.log"); }
        }

        OracleDbType GetDataTypeOracle(Type type)
        {
            if (type == typeof(byte[])) { return OracleDbType.LongRaw; }
            else if (type == typeof(bool)) { return OracleDbType.Int16; }
            else if (type == typeof(string)) { return OracleDbType.NVarchar2; }
            else if (type == typeof(char)) { return OracleDbType.NChar; }
            else if (type == typeof(DateTime)) { return OracleDbType.Date; }
            else if (type == typeof(long)) { return OracleDbType.Int64; }
            else if (type == typeof(decimal)) { return OracleDbType.Decimal; }
            else if (type == typeof(double)) { return OracleDbType.Double; }
            else if (type == typeof(float)) { return OracleDbType.Single; }
            else if (type == typeof(int)) { return OracleDbType.Int32; }
            else if (type == typeof(short)) { return OracleDbType.Int16; }
            else if (type == typeof(byte)) { return OracleDbType.Byte; }
            else { return OracleDbType.BinaryDouble; }
        }

        void ExecuteInclusionOfRecords(string table_name, SqlDataReader table_reader, OracleConnection oracle_connection, Column primaryKey, List<Column> cols)
        {
            StringBuilder insert_query = new($"INSERT INTO {table_name.ToUpper()} (");
            List<OracleParameter> oraParams = new();
            try
            {
                var primaryKeyValue = table_reader.GetValue(primaryKey.Position - 1);
                string statmentFindRecord = $"SELECT COUNT(*) FROM {table_name.ToUpper()} WHERE {primaryKey.ColumnName} = :PK";
                OracleParameter pKey = new()
                {
                    Value = primaryKeyValue,
                    OracleDbType = GetDataTypeOracle(primaryKeyValue.GetType()),
                    ParameterName = "PK"
                };
                using (OracleCommand oracle_command = new(statmentFindRecord, oracle_connection))
                {
                    oracle_command.Parameters.Add(pKey);
                    int qtty = Convert.ToInt32(oracle_command.ExecuteScalar());
                    if (qtty > 0) { return; }
                }

                foreach (Column c in cols)
                { insert_query.Append(c.ColumnName + ","); }

                insert_query.Length--;
                insert_query.Append(") VALUES (");

                for (int i = 0; i < table_reader.FieldCount; i++)
                {
                    object? value = table_reader.IsDBNull(i) ? null : table_reader.GetValue(i);
                    string parameter_name = $"param{i}";
                    OracleDbType parameter_type = GetDataTypeOracle(table_reader.GetFieldType(i));

                    if (cols[i].IsNullable == "NO" && cols[i].DataType == "varchar" && (value == null || value?.ToString() == ""))
                    { value = " "; }
                    insert_query.Append($":{parameter_name},");
                    oraParams.Add(new OracleParameter(parameter_name, parameter_type) { Value = value });
                }

                insert_query.Length--;
                insert_query.Append(')');

                using OracleCommand oracle_insert_command = new(insert_query.ToString(), oracle_connection);
                oracle_insert_command.Parameters.AddRange(oraParams.ToArray());
                oracle_insert_command.CommandTimeout = 120;
                oracle_insert_command.CommandType = CommandType.Text;
                int efetiveRows = oracle_insert_command.ExecuteNonQuery();
                if (efetiveRows == 0) { throw new Exception($"Registro não foi incluido na Tabela {table_name}."); }
                oracle_connection.FlushCache();
            }
            catch (Exception ex)
            {
                ShowMessage($"    Error Operation {insert_query}...");
                foreach (OracleParameter p in oraParams)
                { ShowMessage($"    Table:: {table_name} - Parameter:: {p.Value} - {p.DbType} - {p.OracleDbType}"); }
                ShowMessage($"    An error occurred: {ex.Message}");
                if (ex.StackTrace != null)
                {
                    string[] stackTrace = ex.StackTrace.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                    for (int i = 0; i < stackTrace.Length; i++) { ShowMessage($"           {(i == 0 ? "StackTrace: " : "            ")}{stackTrace[i]}"); }
                }
                tem_erro = true;
            }

        }

        int AccountTableRecordsOracle(OracleConnection ora_connection, string table_name)
        {
            string statmentFindRecord = $"SELECT COUNT(*) FROM {table_name.ToUpper()}";
            using OracleCommand sql_command = new(statmentFindRecord, ora_connection);
            return Convert.ToInt32(sql_command.ExecuteScalar());
        }

        int ContaRegistrosDeTabelaSQL(SqlConnection sql_connection, string table_name)
        {
            string statmentFindRecord = $"SELECT COUNT(*) FROM {table_name.ToUpper()}";
            using SqlCommand sql_command = new(statmentFindRecord, sql_connection);
            return Convert.ToInt32(sql_command.ExecuteScalar());
        }

        void ExecuteBCP(string table_schema, string table_name, List<Column> cols, int total)
        {
            string bcpCommand = "BCP";

            StringBuilder statmentToExport = new("SELECT ");
            foreach (Column col in cols)
            {
                if (col.DataType == "varchar" || col.DataType == "nvarchar" || col.DataType == "text")
                { statmentToExport.Append($" SUBSTRING(REPLACE(REPLACE({col.ColumnName}, CHAR(13),'\\n'), CHAR(10), ''),1,{varcharLimit}) AS {col.ColumnName},"); }
                else
                { statmentToExport.Append($" {col.ColumnName},"); }
            }
            statmentToExport.Length--;
            statmentToExport.Append($" FROM {table_schema.ToLower()}.{table_name.ToUpper()}");
            string bcpArguments = string.Format(bcp_params, statmentToExport.ToString(), table_name, sql_host, sql_catalog, sql_userId, sql_password);
            if (File.Exists(Path.Combine(Environment.CurrentDirectory, $"BCP\\{table_name}.dat"))) { File.Delete(Path.Combine(Environment.CurrentDirectory, $"BCP\\{table_name}.dat")); }
            ShowMessage($"    Running BCP to create import BCP\\{table_name}.dat file...");
            bcp_scripts.AppendLine(string.Concat(bcpCommand, " ", bcpArguments));
            if (total > 0)
            {
                ExecuteExternalProgram(bcpCommand, bcpArguments);
                RepairDataFile(Path.Combine(Environment.CurrentDirectory, $"BCP\\{table_name}.dat"));
            }
        }

        void ExecuteBCPComExtracaoDeArquivos(string table_schema, string table_name, List<Column> cols, SqlConnection sql_server_connection, int total)
        {
            DateTime startTime = DateTime.Now;
            ShowMessage($"    Extracting files {(CheckIfTypeColumnExists("image", cols) ? "Images" : "") + (CheckIfTypeColumnExists("xml", cols) ? "XMLs" : "") + (CheckIfTypeColumnExists("nclob", cols) ? " Text" : "")}...");

            int rows = 0;
            int completed = 0;

            ShowMessage("    ", false);

            int batchSize = 50000;

            int totalBatches = (int)Math.Ceiling((double)total) / batchSize;

            int startIndex = 0;
            int batchRecords = total;
            ExtractBatchFiles(cols, table_schema, table_name, startIndex, batchRecords, ref rows, ref completed, total);

            //if (totalBatches == 0)
            //{
            //    int startIndex = 0;
            //    int batchRecords = total;
            //    ExtraiLoteArquivos(cols, table_schema, table_name, startIndex, batchRecords, ref rows, ref completed, total);
            //}
            //else
            //{
            //    Parallel.For(0, totalBatches, index =>
            //    {
            //        int startRow = index * batchSize;
            //        int endRow = Math.Min(startRow + batchSize, total);
            //        DateTime beginThread = DateTime.Now;
            //        ExtraiLoteArquivos(cols, table_schema, table_name, startRow, endRow, ref rows, ref completed, total);
            //        DateTime finishThread = DateTime.Now;
            //        string msgThread = $"Thread {index} iniciou às {beginThread:dd/MM/yyyy HH:mm:ss} e finalizou às {finishThread:dd/MM/yyyy HH:mm:ss}, totalizando {((int)finishThread.Subtract(beginThread).TotalMinutes == 0 ? (int)finishThread.Subtract(beginThread).TotalSeconds : (int)finishThread.Subtract(beginThread).TotalMinutes)} {((int)finishThread.Subtract(beginThread).TotalMinutes == 0 ? " segundos" : " minutos")} de execução ({(int)finishThread.Subtract(beginThread).TotalHours}:{finishThread.Subtract(beginThread).Minutes}:{finishThread.Subtract(beginThread).Seconds}).";
            //        log_processamento.AppendLine(msgThread);
            //    });
            //}

            if (total > 0) for (int i = completed; i < 10; i++) { ShowMessage("#", false); }
            ShowMessage("");
            string bcpCommand = "BCP";
            StringBuilder sql = new("SELECT ");

            StringBuilder pkFieldNames = new();
            foreach (Column col in cols.Where(c => c.IsPrimarykey)) { pkFieldNames.Append("CAST(" + col.ColumnName + " AS VARCHAR)+'_'+"); }
            if (pkFieldNames.Length > 0) { pkFieldNames.Length -= 5; }
            else { pkFieldNames.Append("CAST((ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS VARCHAR)+'_'+"); }

            string path = Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name);

            foreach (Column col in cols)
            {
                if ((col.DataType == "varchar" || col.DataType == "nvarchar" || col.DataType == "char" || col.DataType == "nchar" || col.DataType == "text") && (col.DataSize > 0 && col.DataSize <= varcharLimit))
                { sql.Append($" REPLACE(REPLACE({col.ColumnName}, CHAR(13),'\\n'), CHAR(10), '') AS {col.ColumnName},"); }
                else if (col.DataType == "image" || col.DataType == "varchar" || col.DataType == "nvarchar" || col.DataType == "char" || col.DataType == "nchar" || col.DataType == "xml" || col.DataType == "text")
                { sql.Append($" '{Path.Combine(path, table_name)}_'+{pkFieldNames.ToString()}+'{(col.DataType == "image" ? ".frm" : "") + (col.DataType == "xml" ? ".xml" : "") + (CheckColumnOfType("nclob", col) ? ".txt" : "")}',"); }
                else
                { sql.Append($" {col.ColumnName},"); }
            }
            sql.Length--;
            sql.Append($" FROM {table_schema.ToLower()}.{table_name.ToUpper()}");

            string bcpArguments = string.Format(bcp_params, sql.ToString(), table_name, sql_host, sql_catalog, sql_userId, sql_password);
            if (File.Exists(Path.Combine(Environment.CurrentDirectory, $"BCP\\{table_name}.dat")))
            { File.Delete(Path.Combine(Environment.CurrentDirectory, $"BCP\\{table_name}.dat")); }
            ShowMessage($"    Running BCP to create import BCP\\{table_name}.dat file...");
            bcp_scripts.AppendLine(string.Concat(bcpCommand, " ", bcpArguments));
            if (total > 0)
            {
                ExecuteExternalProgram(bcpCommand, bcpArguments);
                RepairDataFile(Path.Combine(Environment.CurrentDirectory, $"BCP\\{table_name}.dat"));
            }
            DateTime finishTime = DateTime.Now;
            ShowMessage($"    Extraction started at {startTime:dd/MM/yyyy HH:mm:ss} - Finished at {finishTime:dd/MM/yyyy HH:mm:ss}, totaling {((int)finishTime.Subtract(startTime).TotalMinutes == 0 ? (int)finishTime.Subtract(startTime).TotalSeconds : (int)finishTime.Subtract(startTime).TotalMinutes)} {((int)finishTime.Subtract(startTime).TotalMinutes == 0 ? " seconds" : " minutes")} of execution ({(int)finishTime.Subtract(startTime).TotalHours}:{finishTime.Subtract(startTime).Minutes}:{finishTime.Subtract(startTime).Seconds}).");
        }

        void ExtractBatchFiles(List<Column> cols, string table_schema, string table_name, int startRow, int endRow, ref int rows, ref int completed, int total)
        {
            StringBuilder query = new("");
            StringBuilder subquery = new("SELECT ");
            string rownum = String.Empty;
            foreach (Column col in cols)
            {
                subquery.Append($"{col.ColumnName},");
                if (col.IsPrimarykey) { rownum = $"ROW_NUMBER() OVER (ORDER BY {col.ColumnName}) AS ROWNUN"; }
                else { rownum = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ROWNUN"; }
            }
            query.Append(subquery);
            query.Length--;
            subquery.Append(rownum);
            subquery.Append($" FROM {table_schema.ToLower()}.{table_name.ToUpper()}");
            query.Append($" FROM ({subquery.ToString()}) T WHERE ROWNUN > {startRow} AND ROWNUN <= {endRow}");

            //SqlCommand sql_server_table_command = new($"SELECT * FROM {table_schema}.{table_name} OFFSET {startIndex} ROWS FETCH NEXT {batchRecords} ROWS ONLY", sql_server_connection);
            SqlCommand sql_server_table_command = new(query.ToString(), sql_server_connection);
            SqlDataReader table_reader = sql_server_table_command.ExecuteReader();

            while (table_reader.Read())
            {
                Interlocked.Add(ref rows, 1);
                int pc = (int)(rows / (double)total * 100) / 10;
                if (pc > completed) { for (int i = completed; i < pc; i++) { ShowMessage("#", false); } _ = Interlocked.Exchange(ref completed, pc); }
                if (CheckIfTypeColumnExists("image", cols)) { ExtraImagesInFiles(table_name, cols, table_reader); }
                if (CheckIfTypeColumnExists("xml", cols)) { ExtractTextsInFiles(table_name, cols, table_reader); }
                if (CheckIfTypeColumnExists("nclob", cols)) { ExtractTextsInFiles(table_name, cols, table_reader); }
            }
            if (!table_reader.IsClosed) { table_reader.Close(); }

        }

        void ExecuteSQLLoader(List<Column> cols, string table_name, bool executar, OracleConnection? oracle_connection, bool HasExternalFileToLoad = false, string dataType = "")
        {
            var path = Path.Combine(Environment.CurrentDirectory, "SQLoader");

            StringBuilder cltFile = new();
            cltFile.AppendLine("LOAD DATA");
            cltFile.AppendLine("\tCHARACTERSET UTF8");
            cltFile.AppendLine($"\tINSERT INTO TABLE {table_name}");
            cltFile.AppendLine("\tTRUNCATE");
            cltFile.AppendLine("\tFIELDS TERMINATED BY \",\"");
            cltFile.AppendLine($"\tOPTIONALLY ENCLOSED BY '{quote}'");
            cltFile.AppendLine("\tTRAILING NULLCOLS");
            cltFile.AppendLine("\t(");

            foreach (Column col in cols)
            {
                cltFile.AppendLine("");
                if (CheckColumnOfType("image", col) || CheckColumnOfType("xml", col) || CheckColumnOfType("nclob", col)) { cltFile.AppendLine($"\t\t{col.ColumnName}FileName FILLER CHAR(100),"); }
                cltFile.Append($"\t\t{col.ColumnName}");
                if (col.DataType == "date" || col.DataType == "datetime") { cltFile.Append($" \"TO_DATE(SUBSTR(:{col.ColumnName}, 1, 19), 'YYYY-MM-DD HH24:MI:SS')\""); }
                if (col.DataType == "smalldatetime") { cltFile.Append($" \"TO_DATE(SUBSTR(:{col.ColumnName}, 1, 19), 'YYYY-MM-DD HH24:MI:SS')\""); }
                if (CheckColumnOfType("image", col) || CheckColumnOfType("xml", col) || CheckColumnOfType("nclob", col)) { cltFile.Append($" LOBFILE({col.ColumnName}FileName) TERMINATED BY EOF"); }
                cltFile.Append(',');
            }

            cltFile.Length--;
            cltFile.AppendLine("");
            cltFile.AppendLine("\t)");

            ShowMessage($"    Creating the file Oracle SQL Loader {path}\\{table_name}.ctl...");
            SaveFileUTF8(cltFile, path, $"{table_name}.ctl");

            StringBuilder parFile = new();
            parFile.AppendLine($"userid={ora_userId}/{ora_password}@//{ora_host}:{ora_port}/{ora_sid}");
            parFile.AppendLine($"control='{Path.Combine(Environment.CurrentDirectory, "SQLoader", string.Concat(table_name, ".ctl"))}'");
            parFile.AppendLine($"log='{Path.Combine(Environment.CurrentDirectory, "Log", string.Concat(table_name, "_sqlldr.log"))}'");
            parFile.AppendLine($"bad='{Path.Combine(Environment.CurrentDirectory, "Log", string.Concat(table_name, "_sqlldr.bad"))}'");
            parFile.AppendLine($"data='{Path.Combine(Environment.CurrentDirectory, "BCP", string.Concat(table_name, ".dat"))}'");
            parFile.AppendLine("errors=2147483647");
            parFile.AppendLine("direct=true");
            parFile.AppendLine("silent=all");

            ShowMessage($"    Creating the file Oracle SQL Loader {path}\\{table_name}.par...");
            SaveFileUTF8(parFile, path, $"{table_name}.par");

            ShowMessage($"    Executing Oracle SQL Loader parfile='{path}\\{table_name}.par'...");
            sqlldr_scripts.AppendLine(string.Concat("sqlldr", " ", $"parfile='{path}\\{table_name}.par'"));
            if (executar)
            {
                while (!File.Exists($"{path}\\{table_name}.par")) { Thread.Sleep(1000); }
                ExecuteExternalProgram("sqlldr.exe", $"parfile='{path}\\{table_name}.par'");
                bool updateOk = false;
                while (!updateOk)
                {
                    try
                    {
                        if (after_all_data_load.Length > 0) { after_all_data_load.AppendLine(""); }
                        StringBuilder updateStatment = new($"UPDATE\t{table_name}\n\tSET ");
                        foreach ((Column col, int index) in cols.Where(c => c.DataType == "text" || c.DataType == "varchar" || c.DataType == "nvarchar" || (c.DataSize > 0 && c.DataSize <= varcharLimit)).WithIndex())
                        { updateStatment.Append($"{(index > 0 ? "\n\t\t" : "\t")}{col.ColumnName} = REPLACE({col.ColumnName}, '\\n', CHR(13)||CHR(10)),"); }
                        updateStatment.Length--;
                        if (currentErrorCount == 0) { after_all_data_load.AppendLine(updateStatment + ";"); }
                        using OracleCommand command = new(updateStatment.ToString(), oracle_connection);
                        command.ExecuteNonQuery();
                        updateOk = true;

                    }
                    catch (Exception ex)
                    {
                        ShowMessage($"    An error occurred: {ex.Message}");
                        if (ex.StackTrace != null)
                        {
                            string[] stackTrace = ex.StackTrace.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                            for (int i = 0; i < stackTrace.Length; i++) { ShowMessage($"           {(i == 0 ? "StackTrace: " : "            ")}{stackTrace[i]}"); }
                        }
                        if (currentErrorCount >= maxErrosRepetition)
                        {
                            currentErrorCount = 0;
                            after_all_data_load.AppendLine($"    Unable to perform an Update on table {table_name}. Message: {ex.Message}.");
                            break;
                        }
                        currentErrorCount++;
                    }
                }
            }
        }

        void SaveFileUTF8(StringBuilder content, string path, string fileName)
        {
            if (!Directory.Exists(path)) { Directory.CreateDirectory(path); }
            if (File.Exists(Path.Combine(path, fileName))) { File.Delete(Path.Combine(path, fileName)); }
            using StreamWriter writer = new(Path.Combine(path, fileName), false, utf8pure);
            writer.Write(content.ToString());
        }

        void ExecuteExternalProgram(string command, string arguments)
        {
            Process process = new();
            try
            {
                process.StartInfo.FileName = command;
                process.StartInfo.Arguments = arguments;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                process.StartInfo.UseShellExecute = false;

                process.Start();

                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();

                process.WaitForExit();

                if (output.Length > 0) { ShowMessage($"    Output: {output.Replace('\n', ' ')}"); }
                if (error.Length > 0) { ShowMessage($"    Errors: {error}"); tem_erro = true; }
            }
            catch (Exception ex)
            {
                ShowMessage($"    An error occurred: {ex.Message}");
                if (ex.StackTrace != null)
                {
                    string[] stackTrace = ex.StackTrace.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                    for (int i = 0; i < stackTrace.Length; i++) { ShowMessage($"           {(i == 0 ? "StackTrace: " : "            ")}{stackTrace[i]}"); }
                }
            }
            finally
            { process.Close(); }
        }

        void LinkingAsReferences(SqlConnection sql_server_connection, OracleConnection oracle_connection)
        {
            ShowMessage("Enabling all Foreign Key Constraints...");
            string query = "SELECT fk_tab.name as tabela_estrangeira,\r\npk_tab.name as tabela_primaria,\r\nSUBSTRING(column_names, 1, len(column_names)-1) as [fk_columns],\r\nfk.name as fk_constraint_name\r\nFROM sys.foreign_keys fk\r\nINNER JOIN sys.tables fk_tab\r\nON fk_tab.object_id = fk.parent_object_id\r\nINNER JOIN sys.tables pk_tab\r\nON pk_tab.object_id = fk.referenced_object_id\r\nCROSS APPLY (select col.[name] + ', '\r\nFROM sys.foreign_key_columns fk_c\r\nINNER JOIN sys.columns col\r\nON fk_c.parent_object_id = col.object_id\r\nAND fk_c.parent_column_id = col.column_id\r\nWHERE fk_c.parent_object_id = fk_tab.object_id\r\nAND fk_c.constraint_object_id = fk.object_id\r\nORDER BY col.column_id\r\nFOR XML PATH ('') ) D (column_names)\r\nORDER BY schema_name(fk_tab.schema_id) + '.' + fk_tab.name,\r\nschema_name(pk_tab.schema_id) + '.' + pk_tab.name;";
            SqlCommand sql_server_table_command = new(query, sql_server_connection);
            using SqlDataReader table_reader = sql_server_table_command.ExecuteReader();
            bool reading = table_reader.Read();
            while (reading)
            {
                string? ConstraintName = table_reader.IsDBNull(3) ? null : table_reader.GetValue(3).ToString();
                query = $"SELECT COUNT(1) FROM USER_CONSTRAINTS WHERE CONSTRAINT_NAME = '{ConstraintName}'";
                OracleCommand query_check_exists = new(query, oracle_connection);
                if (Convert.ToInt32(query_check_exists.ExecuteScalar()) == 0)
                {
                    string? TabelaEstrangeira = table_reader.IsDBNull(0) ? null : table_reader.GetValue(0).ToString();
                    string? TabelaPrimaria = table_reader.IsDBNull(1) ? null : table_reader.GetValue(1).ToString();
                    StringBuilder sbFK = new();
                    sbFK.Append($"ALTER TABLE {TabelaEstrangeira} ADD CONSTRAINT {ConstraintName} FOREIGN KEY ");
                    StringBuilder Fields = new();
                    while (reading && ConstraintName == (table_reader.IsDBNull(3) ? null : table_reader.GetValue(3).ToString()))
                    {
                        Fields.Append($"{table_reader.GetValue(2)},");
                        reading = table_reader.Read();
                    }
                    Fields.Length--;
                    sbFK.Append($"({Fields.ToString()}) REFERENCES {TabelaPrimaria} ({Fields.ToString()})");
                    OracleCommand ora_server_constraint_command = new(sbFK.ToString(), oracle_connection);
                    try
                    {
                        ora_server_constraint_command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        ShowMessage($"    Error creating Constraint {ConstraintName}...");
                        ShowMessage($"    An error occurred: {ex.Message}");
                        if (ex.StackTrace != null)
                        {
                            string[] stackTrace = ex.StackTrace.Split(new string[] { System.Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                            for (int i = 0; i < stackTrace.Length; i++) { ShowMessage($"           {(i == 0 ? "StackTrace: " : "            ")}{stackTrace[i]}"); }
                        }
                    }
                    create_constraints.AppendLine(sbFK.ToString() + ";");
                }
            }
            if (!table_reader.IsClosed) { table_reader.Close(); }
            oracle_connection.FlushCache();
        }

        bool CheckIfTypeColumnExists(string dataType, List<Column> cols)
            => cols.Where(c => c.DataType == dataType || (dataType == "nclob" && ((c.DataType == "varchar" || c.DataType == "nvarchar" || c.DataType == "char" || c.DataType == "nchar") && (c.DataSize <= 0 || c.DataSize > varcharLimit)))).Any();

        bool CheckColumnOfType(string dataType, Column col)
            => col.DataType == dataType || (dataType == "nclob" && ((col.DataType == "varchar" || col.DataType == "nvarchar" || col.DataType == "char" || col.DataType == "nchar") && (col.DataSize <= 0 || col.DataSize > varcharLimit)));

        string ExtraImagesInFiles(string table_name, List<Column> cols, SqlDataReader reader)
        {
            string colName = (cols.Where(c => c.DataType == "image").FirstOrDefault() ?? throw new Exception("Column Image não encontrada")).ColumnName;
            byte[] imageData = (byte[])reader[colName];
            StringBuilder fileName = new();
            foreach (Column col in cols.Where(c => c.IsPrimarykey))
            { fileName.Append($"{reader[col.ColumnName]}_"); }
            fileName.Length--;
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP"))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP")); }
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP", "Files"))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP", "Files")); }
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name)); }
            string filePath = Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name, string.Concat(table_name, "_", fileName.ToString(), ".frm"));
            using var writer = new BinaryWriter(File.OpenWrite(filePath));
            writer.Write(imageData);
            return filePath;
        }

        string ExtractTextsInFiles(string table_name, List<Column> cols, SqlDataReader reader)
        {
            string colName = (cols.Where(c => c.DataType == "xml" || ((c.DataType == "varchar" || c.DataType == "nvarchar" || c.DataType == "char" || c.DataType == "nchar") && (c.DataSize <= 0 || c.DataSize > varcharLimit))).FirstOrDefault() ?? throw new Exception("Column Text/XML não encontrada")).ColumnName;
            string textData = (string)reader[colName];
            StringBuilder fileName = new();
            foreach (Column col in cols.Where(c => c.IsPrimarykey))
            { fileName.Append($"{reader[col.ColumnName]}_"); }
            fileName.Length--;
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP"))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP")); }
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP", "Files"))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP", "Files")); }
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name))) { Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name)); }
            string filePath = Path.Combine(Environment.CurrentDirectory, "BCP", "Files", table_name, string.Concat(table_name, "_", fileName.ToString(), (CheckIfTypeColumnExists("xml", cols) ? ".xml" : "") + (CheckIfTypeColumnExists("nclob", cols) ? ".txt" : "")));
            using (StreamWriter writer = new(filePath))
            { writer.Write(textData); }
            return filePath;
        }

        void RepairDataFile(string filePath)
        {
            if (!File.Exists(filePath)) { return; }
            List<string> lines = File.ReadAllLines(filePath).ToList();
            if (!lines.Any()) { return; }
            string tempFilePath = $"{Path.Combine(Environment.CurrentDirectory, "forDelete.dat")}";
            string firstLine = lines[0];
            firstLine = string.Concat($"{quote}", firstLine.AsSpan(0));
            lines[0] = firstLine;
            var itemToDelete = lines[^1];
            if (lines.Remove(itemToDelete))
            {
                var newLines = lines.ToArray();
                File.WriteAllLines(tempFilePath, newLines, utf8pure);
                File.Delete(filePath);
                File.Move(tempFilePath, filePath);
            }
        }

        void ShowMessage(string mensagem, bool cr = true)
        {
            if (cr) { Console.WriteLine(mensagem); log_processamento.AppendLine(mensagem); }
            else { Console.Write(mensagem); log_processamento.Append(mensagem); }
        }

    }

}

namespace SQLServerToOracleMigrationUtility
{
    public class Column
    {
        public int Position { get; set; }
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public int? DataSize { get; set; } = null;
        public int? NumericPrecision { get; set; } = null;
        public int? NumericScale { get; set; } = null;
        public object? DefaultValue { get; set; } = null;
        public string IsNullable { get; set; } = string.Empty;
        public bool IsIdentity { get; set; } = false;
        public bool IsPrimarykey { get; set; } = false;
    }

}
