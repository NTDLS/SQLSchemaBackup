using System;
using System.Collections.Generic;
using System.Reflection;

namespace SQLSchemaBackup
{
    internal enum ParseOutcome { Run, Exit, Error }

    /// <summary>
    /// The settings for a run, and the command line parser that fills them in.
    /// Options follow the usual conventions: --long-name value, --long-name=value, -s value.
    /// </summary>
    internal sealed class BackupSettings
    {
        public const string PasswordEnvironmentVariable = "SQLSCHEMABACKUP_PASSWORD";

        public List<string> Servers = new();
        public string OutputFolder = string.Empty;
        public List<string> IncludeDatabases = new();
        public List<string> ExcludeDatabases = new();
        public int Parallel = 4;
        public string Login = string.Empty;
        public string Password = string.Empty;
        public bool Quiet;

        // long name -> (short name, takes a value)
        private static readonly Dictionary<string, (char Short, bool Value)> Options = new(StringComparer.OrdinalIgnoreCase)
        {
            ["server"] = ('s', true),
            ["output"] = ('o', true),
            ["include"] = ('i', true),
            ["exclude"] = ('x', true),
            ["parallel"] = ('p', true),
            ["login"] = ('l', true),
            ["password"] = ('\0', true),
            ["quiet"] = ('q', false),
            ["version"] = ('v', false),
            ["help"] = ('h', false),
        };

        // The pre-2.0 /Switch:value spellings, still accepted so existing scheduled tasks keep working.
        private static readonly Dictionary<string, string> Legacy = new(StringComparer.OrdinalIgnoreCase)
        {
            ["/Servers:"] = "server",
            ["/Folder:"] = "output",
            ["/Include:"] = "include",
            ["/Exclude:"] = "exclude",
            ["/Threads:"] = "parallel",
        };

        public static ParseOutcome Parse(string[] args, out BackupSettings settings, out string message)
        {
            settings = new BackupSettings();
            message = null;

            if (args.Length == 0)
            {
                message = Usage();
                return ParseOutcome.Exit;
            }

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                string name = null, value = null;

                if (arg is "/?" or "-?")
                {
                    name = "help";
                }
                else if (arg.StartsWith("--", StringComparison.Ordinal))
                {
                    int eq = arg.IndexOf('=');
                    name = eq < 0 ? arg.Substring(2) : arg.Substring(2, eq - 2);
                    if (eq >= 0) value = arg.Substring(eq + 1);
                    if (!Options.ContainsKey(name))
                        return Fail(out message, $"Unknown option: {arg}");
                }
                else if (arg.Length == 2 && arg[0] == '-')
                {
                    foreach (var (key, info) in Options)
                        if (info.Short == char.ToLowerInvariant(arg[1]) && info.Short != '\0') { name = key; break; }
                    if (name == null)
                        return Fail(out message, $"Unknown option: {arg}");
                }
                else if (arg.StartsWith("/User:", StringComparison.OrdinalIgnoreCase) || arg.StartsWith("/Password:", StringComparison.OrdinalIgnoreCase))
                {
                    return Fail(out message, "/User and /Password (Windows impersonation) were removed. Run the tool as that account, or use --login for SQL authentication");
                }
                else if (arg.StartsWith('/'))
                {
                    foreach (var (prefix, key) in Legacy)
                        if (arg.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) { name = key; value = arg.Substring(prefix.Length); break; }
                    if (name == null)
                        return Fail(out message, $"Unknown option: {arg}");
                }
                else
                {
                    return Fail(out message, $"Unexpected argument: {arg}");
                }

                name = name.ToLowerInvariant();
                if (Options[name].Value && value == null)
                {
                    if (i + 1 >= args.Length)
                        return Fail(out message, $"Option --{name} needs a value.");
                    value = args[++i];
                }
                else if (!Options[name].Value && value != null)
                {
                    return Fail(out message, $"Option --{name} does not take a value.");
                }

                value = value?.Trim();

                switch (name)
                {
                    case "help":
                        message = Usage();
                        return ParseOutcome.Exit;
                    case "version":
                        message = Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";
                        return ParseOutcome.Exit;
                    case "server": settings.Servers.AddRange(Split(value)); break;
                    case "output": settings.OutputFolder = value; break;
                    case "include": settings.IncludeDatabases.AddRange(Split(value)); break;
                    case "exclude": settings.ExcludeDatabases.AddRange(Split(value)); break;
                    case "quiet": settings.Quiet = true; break;
                    case "password": settings.Password = value; break;
                    case "parallel":
                        if (!int.TryParse(value, out settings.Parallel) || settings.Parallel < 1)
                            return Fail(out message, $"--parallel must be a whole number of 1 or more, not [{value}].");
                        break;
                    case "login": settings.Login = value; break;
                }
            }

            if (settings.Servers.Count == 0)
                return Fail(out message, "No servers were specified. Use --server.");
            if (settings.OutputFolder.Length == 0)
                return Fail(out message, "No output folder was specified. Use --output.");

            if (settings.Password.Length == 0)
                settings.Password = Environment.GetEnvironmentVariable(PasswordEnvironmentVariable) ?? string.Empty;

            return ParseOutcome.Run;
        }

        private static ParseOutcome Fail(out string message, string text)
        {
            message = text.TrimEnd('.') + ". Use --help for usage.";
            return ParseOutcome.Error;
        }

        private static IEnumerable<string> Split(string list)
            => list.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        public static string Usage() => """
            SQLSchemaBackup - scripts the schema of SQL Server databases to .sql files.

            Usage:
              SQLSchemaBackup --server <names> --output <folder> [options]

            Required:
              -s, --server <names>      Servers to back up. Comma separated and/or repeated.
              -o, --output <folder>     Folder to write the scripts to.

            Options:
              -i, --include <patterns>  Only back up databases matching these names (* wildcards).
              -x, --exclude <patterns>  Skip databases matching these names (* wildcards).
              -p, --parallel <n>        Servers to back up at the same time (default 4).
              -l, --login <name>        Connect with this SQL Server login instead of Windows
                                        authentication (the current Windows account).
              --password <text>         Password for --login. Prefer the SQLSCHEMABACKUP_PASSWORD
                                        environment variable, or omit it to be prompted.
              -q, --quiet               Only report errors.
              -v, --version             Show the version.
              -h, --help                Show this help.

            Examples:
              SQLSchemaBackup -s SQL01 -o D:\Schemas
              SQLSchemaBackup -s SQL01,SQL02\REPORTING -o D:\Schemas -x master,model,msdb,tempdb
              SQLSchemaBackup -s SQL01 -o D:\Schemas -i Sales* -p 8 -q

            Exit codes: 0 success, 1 one or more errors while scripting, 2 invalid usage.
            """;
    }
}
