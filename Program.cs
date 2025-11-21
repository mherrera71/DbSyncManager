using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

using DbUp;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.ScriptProviders;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
namespace dbupdate;
public class Program
{
    // Ajusta aquí (valores por defecto; pueden ser sobreescritos por appsettings.json)
    static readonly string CentralLogConn = "Server=.;Database=PCMDbUpdate;Trusted_Connection=True;Trust Server Certificate=True;";
    static readonly string ServerBaseConn = "Server=.;Trusted_Connection=True;Trust Server Certificate=True;"; // sin DB
    static readonly string AppVersion = "DbSyncManager-v1.0.0";
    static readonly string ScriptsRoot = @"C:\\Code\\DbSyncManager\\Scripts"; // tu carpeta
    static readonly string CentralLogDbNameDefault = "PCMDbUpdate";

    static readonly Regex DbTag = new Regex(@"--\s*@db:\s*(?<db>\w+)", RegexOptions.IgnoreCase);
    static readonly Regex UseDb = new Regex(@"^\s*USE\s+\[?(?<db>[A-Za-z0-9_]+)\]?\s*;?\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);

    static int Main(string[] args)
    {
        // 1) Cargar configuración desde appsettings.json (si existe)
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .Build();

        var centralLogConn = config["CentralLogConn"] ?? CentralLogConn;
        var serverBaseConn = config["ServerBaseConn"] ?? ServerBaseConn;
        var scriptsRoot = config["ScriptsRoot"] ?? ScriptsRoot;
        // Determinar nombre de la base central (desde la cadena o desde config)
        var csb = new SqlConnectionStringBuilder(centralLogConn);
        var centralDbName = !string.IsNullOrWhiteSpace(csb.InitialCatalog)
            ? csb.InitialCatalog
            : (config["CentralLogDbName"] ?? CentralLogDbNameDefault);

        // 2) Preparar bitácora por corrida (archivo)
        var batchId = Guid.NewGuid();
        var runLogEnabled = bool.TryParse(config["RunLog:Enabled"], out var e) ? e : true;
        var runLogDir = config["RunLog:Directory"] ?? Path.Combine(AppContext.BaseDirectory, "Logs");
        var runMinLevel = ParseLevel(config["RunLog:MinLevel"]) ?? LogLevel.Information;
        Directory.CreateDirectory(runLogDir);
        var runLogPath = Path.Combine(runLogDir, $"dbupdate_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{batchId}.log");

        // 3) Configurar logging (console + archivo por corrida)
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Information);
            builder.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
            if (runLogEnabled)
            {
                builder.AddProvider(new FileLoggerProvider(runLogPath, runMinLevel));
            }
        });
        var logger = loggerFactory.CreateLogger<Program>();

        try
        {
            logger.LogInformation("Iniciando dbupdate {AppVersion}", AppVersion);
            logger.LogInformation("Log de corrida: {RunLog}", runLogEnabled ? runLogPath : "(deshabilitado)");
            logger.LogInformation("Configuración: ScriptsRoot={ScriptsRoot}", scriptsRoot);

            // 4) Cargar scripts desde el file system (incluyendo subcarpetas)
            logger.LogInformation("Cargando scripts desde el sistema de archivos (recursivo)...");
            var fileSystemProvider = new FileSystemScriptProvider(scriptsRoot, new FileSystemScriptOptions
            {
                IncludeSubDirectories = true,
                Filter = path => path.EndsWith(".sql", StringComparison.OrdinalIgnoreCase),
                Encoding = Encoding.UTF8
            });
            var rawScripts = fileSystemProvider
                .GetScripts(null)
                .OrderBy(s => ExtractDateKey(s.Name))
                .ThenBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            // Enriquecer: obtener DB desde el script y decorar el nombre como "db::Nombre"
            var scriptsWithDb = rawScripts
                .Select(s =>
                {
                    var db = ExtractDbFromScript(s) ?? "master"; // fallback a master
                    return new { db, script = s };
                })
                .ToList();

            // Reempaquetar scripts con nombre decorado para que el journal sea por DB+script
            var decoratedScripts = scriptsWithDb
                .Select(x => new SqlScript($"{x.db}::{x.script.Name}", x.script.Contents))
                .ToList();

            logger.LogInformation("Total de scripts encontrados: {Count}", decoratedScripts.Count);

            // 5) Agrupar por base destino (abrir conexión correcta), usando la DB detectada o master por defecto
            var decoratedByDb = decoratedScripts
                .Select(ds => new { db = TryParseDbFromDecoratedName(ds.Name)!, script = ds })
                .GroupBy(x => x.db, x => x.script, StringComparer.OrdinalIgnoreCase)
                .ToList();

            logger.LogInformation("Bases de datos destino detectadas: {DbCount}", decoratedByDb.Count);
            foreach (var grp in decoratedByDb)
                logger.LogInformation("- DB {Db}: {Scripts} scripts", grp.Key, grp.Count());
            // 6) Journal centralizado (no depende del DB fijo; decide por nombre decorado o por tag)
            var journal = new CentralJournal(centralLogConn, AppVersion, batchId, logger, serverBaseConn, centralDbName);

            journal.EnsureCentralTableExists();

            // 7) Ejecutar por base
            var overallOk = true;

            foreach (var grp in decoratedByDb)
            {
                var dbName = grp.Key;
                var scriptsForDb = grp.ToList();
                logger.LogInformation("Iniciando ejecución para DB {DbName}", dbName);

                // Conexión directa a la base destino (si no existe, para 'master' siempre existirá)
                var targetConn = $"{serverBaseConn};Database={dbName}";


                // Script provider “en memoria” con los scripts decorados de esta base
                var provider = new StaticScriptProvider(scriptsForDb);

                // Engine DbUp
                var engineBuilder =
                    DeployChanges.To
                        .SqlDatabase(targetConn)
                        .WithScripts(provider)
                        .JournalTo(journal)
                        .LogTo(new LoggerUpgradeLog(logger));

                // Envolver el ejecutor de scripts para medir duración real por script
                engineBuilder.Configure(c =>
                {
                    c.ScriptExecutor = new TimingScriptExecutor(c.ScriptExecutor, journal);
                });

                // Si el destino es master (p. ej., scripts que crean BDs), ejecutar SIN transacción
                if (string.Equals(dbName, "master", StringComparison.OrdinalIgnoreCase))
                {
                    logger.LogInformation("DB {DbName}: ejecutando sin transacción (scripts de administración/CREATE DATABASE)", dbName);
                    engineBuilder = engineBuilder.WithoutTransaction();
                }
                else
                {
                    engineBuilder = engineBuilder.WithTransaction(); // transacción por script
                }

                var upgrader = engineBuilder.Build();

                // Log previo: listar exactamente los scripts que se ejecutarán en esta corrida para esta DB
                var pending = upgrader.GetScriptsToExecute();
                if (pending.Count == 0)
                {
                    logger.LogInformation("No hay scripts pendientes para DB {DbName}", dbName);
                }
                else
                {
                    foreach (var sc in pending)
                        logger.LogInformation("Se ejecutará el script: {Script} en DB {DbName}", sc.Name, dbName);
                }

                var result = upgrader.PerformUpgrade();
                if (!result.Successful)
                {
                    overallOk = false;
                    var errScript = result.ErrorScript;
                    if (errScript != null)
                    {
                        logger.LogError(result.Error, "Fallo en script {Script} para DB {DbName}", errScript.Name, dbName);
                        journal.LogFailure(errScript, result.Error); // registrar en central (extrae DB desde script)
                    }
                    else
                    {
                        logger.LogError(result.Error, "Fallo en ejecución para DB {DbName} (sin script identificado)", dbName);
                    }
                }
                else
                {
                    logger.LogInformation("Ejecución finalizada OK para DB {DbName}", dbName);
                }
            }

            logger.LogInformation("Proceso finalizado. Resultado: {Status}", overallOk ? "OK" : "CON ERRORES");
            return overallOk ? 0 : -1; // -1 = error en scripts
        }
        catch (Exception ex)
        {
            // Error de aplicación (no de scripts)
            Console.Error.WriteLine(ex);
            return -2;
        }
    }

    // ====== Soporte ======

    static string? ExtractDbFromScript(SqlScript s) => ExtractDbFromContents(s.Contents ?? string.Empty);
    static string? ExtractDbFromContents(string contents)
    {
        var m = DbTag.Match(contents);
        if (m.Success) return m.Groups["db"].Value;

        var u = UseDb.Match(contents);
        if (u.Success) return u.Groups["db"].Value;

        // Heurística: si crea bases o hace administración de instancias, devolver 'master'
        if (contents.IndexOf("CREATE DATABASE", StringComparison.OrdinalIgnoreCase) >= 0)
            return "master";

        return null;
    }
    static string? TryParseDbFromDecoratedName(string name)
    {
        var idx = name.IndexOf("::", StringComparison.Ordinal);
        return idx > 0 ? name[..idx] : null;
    }

    static LogLevel? ParseLevel(string? s) => s?.ToLowerInvariant() switch
    {
        "trace" => LogLevel.Trace,
        "debug" => LogLevel.Debug,
        "information" or "info" => LogLevel.Information,
        "warning" or "warn" => LogLevel.Warning,
        "error" => LogLevel.Error,
        "critical" or "fatal" => LogLevel.Critical,
        _ => null
    };

    // Proveedor simple de scripts
    class StaticScriptProvider : IScriptProvider
    {
        private readonly IReadOnlyList<SqlScript> _scripts;
        public StaticScriptProvider(IEnumerable<SqlScript> scripts) => _scripts = scripts.ToList();
        public IEnumerable<SqlScript> GetScripts(DbUp.Engine.Transactions.IConnectionManager _) => _scripts;
    }

    // Journal central que implementa IJournal (DbUp v6)
    class CentralJournal : IJournal
    {
        private readonly string _centralConn;
        private readonly string _appVersion;
        private readonly Guid _batchId;
        private readonly ILogger<Program> _logger;
        private readonly string _serverBaseConn;
        private readonly string _centralDbName;

        public CentralJournal(string centralConn, string appVersion, Guid batchId
            , ILogger<Program> logger
            , string serverBaseConn, string centralDbName)
        {
            _centralConn = centralConn;
            _appVersion = appVersion;
            _batchId = batchId;
            _logger = logger;
            _serverBaseConn = serverBaseConn;
            _centralDbName = centralDbName;
        }

        // Devuelve el conjunto de nombres de script ya ejecutados exitosamente.
        // Como decoramos los nombres como "db::script", no filtramos por DB aquí.
        public string[] GetExecutedScripts()
        {
            using var cn = new SqlConnection(_centralConn);
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = $@"
        SELECT ScriptName
        FROM [{_centralDbName}].dbo.SchemaChangeLog
        WHERE Success = 1
        GROUP BY ScriptName";

            var list = new List<string>();
            using var rd = cmd.ExecuteReader();
            while (rd.Read()) list.Add(rd.GetString(0));
            return list.ToArray();
        }


        public void StoreExecutedScript(SqlScript script, Func<IDbCommand> dbCommandFactory)
        {
            // Intentar no duplicar si ya existe con duración
            if (!TryUpdateDuration(script, null))
            {
                LogCentral(script, success: true, durationMs: null, error: null);
            }
        }

        public void EnsureTableExistsAndIsLatestVersion(Func<IDbCommand> dbCommandFactory)
        {
        }

        public void LogFailure(SqlScript script, Exception ex)
        {
            LogCentral(script, success: false, durationMs: null, error: ex.ToString());
        }

        public void LogSuccessWithDuration(SqlScript script, long durationMs)
        {
            // Primero intentar actualizar si existe el registro previo con DurationMs NULL
            if (!TryUpdateDuration(script, (int)Math.Min(int.MaxValue, durationMs)))
            {
                // Si no existe, insertar nuevo con duración
                LogCentral(script, success: true, durationMs: durationMs, error: null);
            }
        }

        bool TryUpdateDuration(SqlScript script, int? durationMs)
        {
            try
            {
                var db = TryParseDbFromDecoratedName(script.Name)
                         ?? ExtractDbFromContents(script.Contents ?? string.Empty)
                         ?? "(unknown)";

                using var cn = new SqlConnection(_centralConn);
                cn.Open();
                using var cmd = cn.CreateCommand();
                if (durationMs.HasValue)
                {
                    cmd.CommandText = $@"
UPDATE [{_centralDbName}].dbo.SchemaChangeLog
SET DurationMs = @ms
WHERE DatabaseName = @db AND ScriptName = @name AND Success = 1 AND DurationMs IS NULL";
                    cmd.Parameters.AddWithValue("@ms", durationMs.Value);
                }
                else
                {
                    // solo comprobar si ya hay un registro exitoso con duración
                    cmd.CommandText = $@"
SELECT COUNT(1)
FROM [{_centralDbName}].dbo.SchemaChangeLog
WHERE DatabaseName = @db AND ScriptName = @name AND Success = 1 AND DurationMs IS NOT NULL";
                }
                cmd.Parameters.AddWithValue("@db", db);
                cmd.Parameters.AddWithValue("@name", script.Name);

                var scalar = cmd.ExecuteScalar();
                var count = scalar is int i ? i : Convert.ToInt32(scalar);
                return count > 0;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "No se pudo actualizar duración en bitácora central para script {Script}", script.Name);
                return false;
            }
        }


        public void EnsureCentralTableExists()
        {
            var targetConn = $"{_serverBaseConn};Database=master";

            using var cn = new SqlConnection(targetConn);
            cn.Open();

            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = $@"
                    IF DB_ID('{_centralDbName}') IS NULL
                    BEGIN
                        CREATE DATABASE [{_centralDbName}];
                    END";
                cmd.ExecuteNonQuery();
            }
            _logger.LogInformation("Asegurada que la base de datos {CentralDb} exista", _centralDbName);

            using (var cmd2 = cn.CreateCommand())
            {
                cmd2.CommandText = $@"
                    IF OBJECT_ID('{_centralDbName}.dbo.SchemaChangeLog') IS NULL
                    BEGIN
                        CREATE TABLE [{_centralDbName}].dbo.SchemaChangeLog(
                            [Id] INT IDENTITY(1, 1) PRIMARY KEY,
                            [DatabaseName] NVARCHAR(200) NOT NULL,
                            [ScriptName] NVARCHAR(1024) NOT NULL,
                            [ScriptHash] NVARCHAR(64) NULL,
                            [AppliedOn] DATETIME2 NOT NULL,
                            [DurationMs] INT NULL,
                            [Success] BIT NOT NULL,
                            [ErrorMessage] NVARCHAR(MAX) NULL,
                            [AppVersion] NVARCHAR(100) NULL,
                            [BatchId] UNIQUEIDENTIFIER NULL
                        );
                        CREATE UNIQUE INDEX IX_SchemaChangeLog_UQ 
                            ON [{_centralDbName}].dbo.SchemaChangeLog([DatabaseName], [ScriptName], [Success]);
                    END";
                cmd2.ExecuteNonQuery();
            }
            _logger.LogInformation("Tabla central de bitácora asegurada en {CentralDb}.dbo.SchemaChangeLog", _centralDbName);
        }


        void LogCentral(SqlScript script, bool success, long? durationMs, string? error)
        {
            var db = TryParseDbFromDecoratedName(script.Name)
                     ?? ExtractDbFromContents(script.Contents ?? string.Empty)
                     ?? "(unknown)";

            using var cn = new SqlConnection(_centralConn);
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = $@"
                INSERT INTO [{_centralDbName}].dbo.SchemaChangeLog
                ( DatabaseName, ScriptName, ScriptHash, AppliedOn, DurationMs, Success, ErrorMessage, AppVersion, BatchId)
                VALUES ( @db, @name, @hash, SYSUTCDATETIME(), @ms, @ok, @err, @ver, @batch)";
            cmd.Parameters.AddWithValue("@db", db);
            cmd.Parameters.AddWithValue("@name", script.Name);
            cmd.Parameters.AddWithValue("@hash", ComputeSha256(script.Contents ?? ""));
            cmd.Parameters.AddWithValue("@ms", durationMs.HasValue ? (object)Math.Min(int.MaxValue, durationMs.Value) : DBNull.Value);
            cmd.Parameters.AddWithValue("@ok", success);
            cmd.Parameters.AddWithValue("@err", (object?)error ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ver", (object?)_appVersion ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@batch", _batchId);
            try { cmd.ExecuteNonQuery(); } catch { /* idempotencia por índice único */ }
        }


        private static string ComputeSha256(string s)
        {
            using var sha = SHA256.Create();
            var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(s));
            var sb = new StringBuilder(bytes.Length * 2);
            foreach (var b in bytes) sb.Append(b.ToString("x2"));
            return sb.ToString();
        }
    }

    // ScriptExecutor que mide duración por script y registra en el journal central
    class TimingScriptExecutor : IScriptExecutor
    {
        private readonly IScriptExecutor _inner;
        private readonly CentralJournal _journal;

        public TimingScriptExecutor(IScriptExecutor inner, CentralJournal journal)
        {
            _inner = inner;
            _journal = journal;
        }

        public int? ExecutionTimeoutSeconds
        {
            get => _inner.ExecutionTimeoutSeconds;
            set => _inner.ExecutionTimeoutSeconds = value;
        }

        public void Execute(SqlScript script)
        {
            var sw = Stopwatch.StartNew();
            _inner.Execute(script);
            sw.Stop();
            _journal.LogSuccessWithDuration(script, sw.ElapsedMilliseconds);
        }

        public void Execute(SqlScript script, IDictionary<string, string> variables)
        {
            var sw = Stopwatch.StartNew();
            _inner.Execute(script, variables);
            sw.Stop();
            _journal.LogSuccessWithDuration(script, sw.ElapsedMilliseconds);
        }

        public void VerifySchema()
        {
            _inner.VerifySchema();
        }
    }

    // Adaptador para enrutar logs de DbUp al ILogger (IUpgradeLog moderno)
    class LoggerUpgradeLog : IUpgradeLog
    {
        private readonly ILogger _logger;
        public LoggerUpgradeLog(ILogger logger) => _logger = logger;
        public void LogTrace(string message, params object[] args) => _logger.LogTrace(message, args);
        public void LogDebug(string message, params object[] args) => _logger.LogDebug(message, args);
        public void LogInformation(string message, params object[] args) => _logger.LogInformation(message, args);
        public void LogWarning(string message, params object[] args) => _logger.LogWarning(message, args);
        public void LogError(string message, params object[] args) => _logger.LogError(message, args);
        public void LogError(Exception ex, string message, params object[] args) => _logger.LogError(ex, message, args);
    }

    // Ordena por fecha en el nombre/ruta (yyyy.MM.dd); si no hay fecha, lo manda al final
    static DateTime ExtractDateKey(string path)
    {
        var m = Regex.Match(path, @"(?<!\d)(\d{4})[.\-_\\/](\d{2})[.\-_\\/](\d{2})(?!\d)");
        if (m.Success &&
            int.TryParse(m.Groups[1].Value, out var y) &&
            int.TryParse(m.Groups[2].Value, out var mo) &&
            int.TryParse(m.Groups[3].Value, out var d))
        {
            if (y >= 1900 && mo is >= 1 and <= 12 && d is >= 1 and <= 31)
            {
                try { return new DateTime(y, mo, d); } catch { }
            }
        }
        return DateTime.MaxValue; // sin fecha => al final
    }
}

// ====== Logging a archivo simple ======
internal sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly string _path;
    private readonly LogLevel _minLevel;
    private readonly object _lock = new();
    private StreamWriter? _writer;

    public FileLoggerProvider(string path, LogLevel minLevel)
    {
        _path = path;
        _minLevel = minLevel;
        _writer = new StreamWriter(new FileStream(_path, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true
        };
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(this, categoryName);

    public void Dispose()
    {
        _writer?.Dispose();
        _writer = null;
    }

    private sealed class FileLogger : ILogger
    {
        private readonly FileLoggerProvider _provider;
        private readonly string _category;
        public FileLogger(FileLoggerProvider provider, string category) { _provider = provider; _category = category; }
        public IDisposable BeginScope<TState>(TState state) => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= _provider._minLevel;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;
            var ts = DateTime.Now.ToString("HH:mm:ss");
            var level = logLevel.ToString().ToLowerInvariant();
            var msg = formatter(state, exception);
            lock (_provider._lock)
            {
                _provider._writer?.WriteLine($"{ts} {level}: {_category}[{eventId.Id}] {msg}");
                if (exception != null)
                {
                    _provider._writer?.WriteLine(exception);
                }
            }
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}
