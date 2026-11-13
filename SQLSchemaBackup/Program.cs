using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace SQLSchemaBackup
{
    internal static class Program
    {
        private static readonly object ConsoleLock = new();
        private static int _errorCount;
        private static bool _quiet;
        private static readonly CancellationTokenSource Cancel = new();

        private static int Main(string[] args)
        {
            switch (BackupSettings.Parse(args, out var settings, out var message))
            {
                case ParseOutcome.Exit:
                    Console.WriteLine(message);
                    return 0;
                case ParseOutcome.Error:
                    Console.Error.WriteLine(message);
                    return 2;
            }

            _quiet = settings.Quiet;
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; Cancel.Cancel(); };

            try
            {
                if (settings.Login.Length > 0 && settings.Password.Length == 0)
                {
                    if (Console.IsInputRedirected)
                    {
                        Console.Error.WriteLine($"No password for --login. Set {BackupSettings.PasswordEnvironmentVariable} or use --password.");
                        return 2;
                    }
                    settings.Password = ReadPassword($"Password for {settings.Login}: ");
                }

                BackupServers(settings).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Error("Main: " + Describe(ex));
            }

            return _errorCount == 0 ? 0 : 1;
        }

        private static string ReadPassword(string prompt)
        {
            Console.Error.Write(prompt);
            var sb = new StringBuilder();
            while (true)
            {
                var key = Console.ReadKey(intercept: true);
                if (key.Key == ConsoleKey.Enter) break;
                if (key.Key == ConsoleKey.Backspace) { if (sb.Length > 0) sb.Length--; }
                else if (!char.IsControl(key.KeyChar)) sb.Append(key.KeyChar);
            }
            Console.Error.WriteLine();
            return sb.ToString();
        }

        private static async Task BackupServers(BackupSettings settings)
        {
            using var gate = new SemaphoreSlim(settings.Parallel);

            var tasks = settings.Servers.Select(serverName => Task.Run(async () =>
            {
                await gate.WaitAsync();
                try
                {
                    // SMO is synchronous, so give each server its own thread.
                    await Task.Factory.StartNew(() => BackupServer(settings, serverName), TaskCreationOptions.LongRunning);
                }
                finally
                {
                    gate.Release();
                }
            }));

            await Task.WhenAll(tasks);
        }

        private static void BackupServer(BackupSettings settings, string serverName)
        {
            try
            {
                var builder = new SqlConnectionStringBuilder
                {
                    DataSource = serverName,
                    TrustServerCertificate = true,
                    ApplicationName = "SQLSchemaBackup"
                };

                if (settings.Login.Length > 0)
                {
                    builder.UserID = settings.Login;
                    builder.Password = settings.Password;
                }
                else
                {
                    builder.IntegratedSecurity = true;
                }

                using var sqlConnection = new SqlConnection(builder.ConnectionString);
                sqlConnection.Open();

                var smoServer = new Server(new ServerConnection(sqlConnection));

                // Without this SMO fetches each object's properties (and the text of every view,
                // procedure and function) with its own round trip. Load them in bulk instead.
                smoServer.SetDefaultInitFields(typeof(View), true);
                smoServer.SetDefaultInitFields(typeof(StoredProcedure), true);
                smoServer.SetDefaultInitFields(typeof(UserDefinedFunction), true);
                smoServer.SetDefaultInitFields(typeof(Trigger), true);

                var databaseNames = smoServer.Databases.Cast<Database>()
                    .Where(d => d.IsAccessible && d.Status == DatabaseStatus.Normal)
                    .Select(d => d.Name).ToList();

                foreach (string databaseName in databaseNames)
                {
                    if (Cancel.IsCancellationRequested) return;
                    if (!CanInclude(settings, databaseName) || CanExclude(settings, databaseName)) continue;

                    try
                    {
                        BackupDatabase(settings, serverName, smoServer.Databases[databaseName]);
                    }
                    catch (Exception ex)
                    {
                        Error($"{serverName}.{databaseName}: {Describe(ex)}");
                    }
                }
            }
            catch (Exception ex)
            {
                Error($"{serverName}: {Describe(ex)}");
            }
        }

        private static void BackupDatabase(BackupSettings settings, string serverName, Database db)
        {
            string folder = Path.Combine(settings.OutputFolder, SafeName(serverName), SafeName(db.Name));
            Directory.CreateDirectory(folder);

            // Remove scripts from earlier runs so sections that no longer apply don't linger.
            foreach (var old in Directory.EnumerateFiles(folder, "*.sql").Where(f => Regex.IsMatch(Path.GetFileName(f), @"^\d{3} ")))
                File.Delete(old);

            int sequence = 0;
            string label = serverName + "." + db.Name;

            void Section(string title, Action<TextWriter> write)
            {
                if (Cancel.IsCancellationRequested) return;
                Log($"{label}.{title}");
                var path = Path.Combine(folder, $"{++sequence:000} {title}.sql");
                using var tw = new StreamWriter(path, false, new UTF8Encoding(false));
                tw.WriteLine($"USE [{db.Name.Replace("]", "]]")}]");
                tw.WriteLine("GO");
                tw.WriteLine();
                write(tw);
            }

            // Writes one object, so a single broken object doesn't lose the rest of the file.
            void Each(TextWriter tw, string name, Func<IEnumerable<string>> script)
            {
                try
                {
                    WriteBatches(tw, script());
                }
                catch (Exception ex)
                {
                    string reason = Describe(ex).Replace("*/", "* /");
                    Error($"{label}: unable to script {name}: {reason}");
                    tw.WriteLine($"/* Unable to script {name.Replace("*/", "* /")}: {reason} */");
                    tw.WriteLine("GO");
                    tw.WriteLine();
                }
            }

            var plain = new ScriptingOptions { IncludeIfNotExists = true, Permissions = true };

            Log($"{label}.Database");
            using (var tw = new StreamWriter(Path.Combine(folder, $"{++sequence:000} Database.sql"), false, new UTF8Encoding(false)))
            {
                Each(tw, "database", () => db.Script().Cast<string>());
            }

            Section("Schemas", tw =>
            {
                foreach (Schema o in db.Schemas)
                {
                    if (o.IsSystemObject) continue;
                    Each(tw, o.Name, () => o.Script(plain).Cast<string>());
                }
            });

            Section("Database Rules", tw =>
            {
                foreach (Microsoft.SqlServer.Management.Smo.Rule o in db.Rules) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
            });

            Section("Database Triggers", tw =>
            {
                foreach (DatabaseDdlTrigger o in db.Triggers) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
            });

            Section("Database Roles", tw =>
            {
                foreach (DatabaseRole o in db.Roles)
                {
                    if (o.IsFixedRole || o.Name == "public") continue;
                    Each(tw, o.Name, () => o.Script(plain).Cast<string>());
                }
            });

            Section("Users", tw =>
            {
                foreach (User o in db.Users)
                {
                    if (o.IsSystemObject) continue;
                    Each(tw, o.Name, () => o.Script(plain).Cast<string>());
                }
            });

            Section("Data Types", tw =>
            {
                foreach (UserDefinedDataType o in db.UserDefinedDataTypes) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
            });

            Section("Types", tw =>
            {
                foreach (UserDefinedType o in db.UserDefinedTypes) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
                foreach (UserDefinedTableType o in db.UserDefinedTableTypes) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
                foreach (XmlSchemaCollection o in db.XmlSchemaCollections) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
            });

            Section("Sequences", tw =>
            {
                foreach (Sequence o in db.Sequences) Each(tw, o.Name, () => o.Script(plain).Cast<string>());
            });

            // Tables: keys, indexes, triggers and the like travel with the table; foreign keys are
            // scripted separately afterwards so circular references can't break a restore.
            var tableOptions = new ScriptingOptions
            {
                IncludeIfNotExists = true,
                Permissions = true,
                ExtendedProperties = true,
                ScriptDataCompression = true,
                DriPrimaryKey = true,
                DriUniqueKeys = true,
                DriChecks = true,
                DriDefaults = true,
                DriClustered = true,
                DriNonClustered = true,
                DriForeignKeys = false,
                Indexes = true,
                ClusteredIndexes = true,
                NonClusteredIndexes = true,
                XmlIndexes = true,
                FullTextIndexes = true,
                Triggers = true,
                OptimizerData = true
            };

            // Load every table's columns, indexes, keys, triggers, etc. in a few queries rather than thousands.
            try
            {
                var prefetch = new ScriptingOptions(tableOptions) { DriForeignKeys = true };
                db.PrefetchObjects(typeof(Table), prefetch);
            }
            catch (Exception ex)
            {
                Log($"{label}: prefetch failed, falling back to on-demand loading (slower): {ex.Message}");
            }

            var tables = Ordered<Table>(db, "OrderTablesByForeignKeyDependencies", "Table", db.Tables)
                .Where(t => !t.IsSystemObject).ToList();

            Section("Tables", tw =>
            {
                foreach (Table o in tables)
                    Each(tw, $"[{o.Schema}].[{o.Name}]", () => o.Script(tableOptions).Cast<string>());
            });

            Section("Foreign Keys", tw =>
            {
                var fkOptions = new ScriptingOptions { IncludeIfNotExists = true, DriForeignKeys = true };
                foreach (Table o in tables)
                    foreach (ForeignKey fk in o.ForeignKeys)
                        Each(tw, $"[{o.Schema}].[{o.Name}].[{fk.Name}]", () => fk.Script(fkOptions).Cast<string>());
            });

            // Views, functions and procedures are scripted as "create a stub if missing, then ALTER with the
            // real text", so the script can be re-run against an existing database, ordered by dependency.
            Section("Views", tw =>
            {
                foreach (View o in Ordered<View>(db, "OrderViewsByDependencies", "View", db.Views).Where(v => !v.IsSystemObject))
                {
                    var view = o;
                    Each(tw, $"[{view.Schema}].[{view.Name}]", () => TextObject(view, view.IsEncrypted,
                        $"CREATE VIEW [{view.Schema}].[{view.Name}] AS SELECT 1 AS o", plain, view.ScriptHeader(true), view.TextBody));
                }
            });

            Section("Functions", tw =>
            {
                foreach (UserDefinedFunction o in Ordered<UserDefinedFunction>(db, "OrderFunctionsByDependencies", "Function", db.UserDefinedFunctions).Where(f => !f.IsSystemObject))
                {
                    var fn = o;
                    string stub = fn.FunctionType switch
                    {
                        UserDefinedFunctionType.Inline => $"CREATE FUNCTION [{fn.Schema}].[{fn.Name}]() RETURNS TABLE AS RETURN (SELECT 1 AS C)",
                        UserDefinedFunctionType.Table => $"CREATE FUNCTION [{fn.Schema}].[{fn.Name}]() RETURNS @Array TABLE (Value INT) AS BEGIN RETURN END",
                        _ => $"CREATE FUNCTION [{fn.Schema}].[{fn.Name}]() RETURNS INT AS BEGIN RETURN 0 END"
                    };
                    Each(tw, $"[{fn.Schema}].[{fn.Name}]", () => TextObject(fn, fn.IsEncrypted,
                        stub, plain, fn.ScriptHeader(true), fn.TextBody));
                }
            });

            Section("Procedures", tw =>
            {
                foreach (StoredProcedure o in Ordered<StoredProcedure>(db, "OrderProceduresByDependencies", "Procedure", db.StoredProcedures).Where(p => !p.IsSystemObject))
                {
                    var proc = o;
                    Each(tw, $"[{proc.Schema}].[{proc.Name}]", () => TextObject(proc, proc.IsEncrypted,
                        $"CREATE PROCEDURE [{proc.Schema}].[{proc.Name}] AS", plain, proc.ScriptHeader(true), proc.TextBody));
                }
            });
        }

        /// <summary>
        /// Builds the re-runnable script for a view, function or procedure.
        /// Encrypted objects have no text to read, so they fall back to SMO's own script.
        /// </summary>
        private static IEnumerable<string> TextObject(ScriptSchemaObjectBase obj, bool encrypted,
            string stub, ScriptingOptions options, string alterHeader, string body)
        {
            if (encrypted || string.IsNullOrEmpty(body))
                return ((IScriptable)obj).Script(options).Cast<string>();

            string escaped = stub.Replace("'", "''");
            return new[]
            {
                $"IF NOT EXISTS(SELECT TOP 1 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[{obj.Schema}].[{obj.Name}]'))\r\n" +
                $"BEGIN\r\n    EXEC dbo.sp_executesql @statement = N'{escaped}'\r\nEND",
                alterHeader + body
            };
        }

        /// <summary>
        /// Lists the objects of a collection in dependency order using the ordering scripts. If the ordering
        /// query fails (for instance a dependency cycle) the SMO collection order is used instead.
        /// </summary>
        private static IEnumerable<T> Ordered<T>(Database db, string scriptName, string nameColumn, System.Collections.IEnumerable collection)
            where T : ScriptSchemaObjectBase
        {
            var all = collection.Cast<T>().ToList();
            try
            {
                var byName = all.ToDictionary(o => (o.Schema, o.Name), o => o);
                var ds = db.ExecuteWithResults(LoadScript(scriptName));
                var ordered = new List<T>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    if (byName.Remove((row["Schema"].ToString(), row[nameColumn].ToString()), out var found))
                        ordered.Add(found);
                }
                ordered.AddRange(byName.Values); // Anything the ordering didn't list.
                return ordered;
            }
            catch (Exception ex)
            {
                Log($"{db.Name}: dependency ordering for {nameColumn}s failed, using name order: {Describe(ex)}");
                return all;
            }
        }

        private static string LoadScript(string name)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resource = assembly.GetManifestResourceNames().First(n => n.EndsWith(name + ".sql", StringComparison.OrdinalIgnoreCase));
            using var reader = new StreamReader(assembly.GetManifestResourceStream(resource)!);
            return reader.ReadToEnd();
        }

        private static void WriteBatches(TextWriter tw, IEnumerable<string> batches)
        {
            foreach (string batch in batches)
            {
                if (string.IsNullOrWhiteSpace(batch)) continue;
                tw.WriteLine(batch);
                tw.WriteLine("GO");
                tw.WriteLine();
            }
        }

        private static string Describe(Exception ex)
        {
            while (ex.InnerException != null) ex = ex.InnerException;
            return ex.Message;
        }

        private static void Log(string message)
        {
            if (!_quiet) lock (ConsoleLock) Console.WriteLine(message);
        }

        private static void Error(string message)
        {
            Interlocked.Increment(ref _errorCount);
            lock (ConsoleLock) Console.Error.WriteLine("ERROR: " + message);
        }


        private static bool CanInclude(BackupSettings settings, string databaseName)
            => settings.IncludeDatabases.Count == 0 || settings.IncludeDatabases.Any(f => Matches(f, databaseName));

        private static bool CanExclude(BackupSettings settings, string databaseName)
            => settings.ExcludeDatabases.Any(f => Matches(f, databaseName));

        /// <summary>Case-insensitive match where * matches any run of characters (leading, trailing or in the middle).</summary>
        private static bool Matches(string filter, string name)
            => Regex.IsMatch(name, "^" + Regex.Escape(filter).Replace("\\*", ".*") + "$", RegexOptions.IgnoreCase);

        private static string SafeName(string name)
        {
            foreach (char c in Path.GetInvalidFileNameChars()) name = name.Replace(c, '_');
            return name;
        }
    }
}
