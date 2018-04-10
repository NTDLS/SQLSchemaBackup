using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Threading;

namespace SQLSchemaBackup
{
    class Program
    {
        class BackupSettings
        {
            public string UserName = string.Empty;
            public string DomainName = string.Empty;
            public string Password = string.Empty;
            public List<string> Servers = new List<string>();
            public string BackupLocation = string.Empty;
            public List<string> IncludeDatabases = new List<string>();
            public List<string> ExcludeDatabases = new List<string>();
        }

        class ThreadParam
        {
            public BackupSettings Settings;
            public string ServerName;
        }

        static object LockObject = new object();
        static int ActiveThreads = 0;
        static int MaxThreads = 10;

        static void Main(string[] args)
        {
            BackupSettings settings = new BackupSettings();

            if (args.GetLength(0) > 0)
            {
                foreach (string arg in args)
                {
                    if (arg.ToLower() == "-?" || arg.ToLower() == "/?")
                    {
                        PrintUsage();
                        return;
                    }
                    else if (arg.ToLower().StartsWith("/Folder:".ToLower()))
                    {
                        settings.BackupLocation = arg.Substring(8).Trim();
                    }
                    else if (arg.ToLower().StartsWith("/User:".ToLower()))
                    {
                        settings.UserName = arg.Substring(6).Trim();
                        if (settings.UserName.IndexOf("\\") > 0)
                        {
                            settings.DomainName = settings.UserName.Substring(0, settings.UserName.IndexOf("\\"));
                            settings.UserName = settings.UserName.Substring(settings.UserName.IndexOf("\\") + 1);
                        }
                    }
                    else if (arg.ToLower().StartsWith("/Password:".ToLower()))
                    {
                        settings.Password = arg.Substring(10).Trim();
                    }
                    else if (arg.ToLower().StartsWith("/Threads:".ToLower()))
                    {
                        MaxThreads = int.Parse(arg.Substring(9).ToString());
                    }
                    else if (arg.ToLower().StartsWith("/Servers:".ToLower()))
                    {
                        settings.Servers.AddRange(arg.Substring(9).Trim().Split(','));
                    }
                    else if (arg.ToLower().StartsWith("/Include:".ToLower()))
                    {
                        settings.IncludeDatabases.AddRange(arg.Substring(9).Trim().Split(','));
                    }
                    else if (arg.ToLower().StartsWith("/Exclude:".ToLower()))
                    {
                        settings.ExcludeDatabases.AddRange(arg.Substring(9).Trim().Split(','));
                    }
                    else
                    {
                        Console.WriteLine("Invlaid argument: [" + arg + "]. Type /? for help.");
                        return;
                    }
                }
            }
            else
            {
                PrintUsage();
                return;
            }

            if (settings.Servers.Count == 0)
            {
                Console.WriteLine("No server names were specified.");
                return;
            }
            if (settings.BackupLocation.Length == 0)
            {
                Console.WriteLine("No backup folder was specified.");
                return;
            }

            if (settings.UserName == string.Empty)
            {
                ScheduleThreads(settings);
            }
            else
            {
                try
                {
                    using (new Impersonation(settings.UserName, settings.DomainName, settings.Password))
                    {
                        ScheduleThreads(settings);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("BackupServer()->Main: " + ex.Message);
                }
            }
        }

        static void ScheduleThreads(BackupSettings settings)
        {
            ThreadPool.SetMaxThreads(MaxThreads, MaxThreads);

            foreach (string serverName in settings.Servers)
            {
                try
                {
                    ThreadParam threadParam = new ThreadParam();
                    threadParam.Settings = settings;
                    threadParam.ServerName = serverName;
                    ThreadPool.QueueUserWorkItem(BackupServer, threadParam);
                    lock (LockObject)
                    {
                        ActiveThreads++;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("BackupServer()->ScheduleThreads: " + ex.Message);
                }
            }

            //Wait on all worker threads to complete.
            while (true)
            {
                lock (LockObject)
                {
                    if (ActiveThreads == 0)
                    {
                        return;
                    }
                }
                Thread.Sleep(100);
            }
        }

        static void BackupServer(object _ThreadParam)
        {
            try
            {
                ThreadParam threadParam = (ThreadParam)_ThreadParam;

                Microsoft.SqlServer.Management.Smo.Server smoServer = new Microsoft.SqlServer.Management.Smo.Server(threadParam.ServerName);

                foreach (Microsoft.SqlServer.Management.Smo.Database smoDatabase in smoServer.Databases)
                {
                    if (!CanInclude(threadParam.Settings, smoDatabase.Name))
                    {
                        continue;
                    }
                    if (CanExclude(threadParam.Settings, smoDatabase.Name))
                    {
                        continue;
                    }

                    string backupLocation = SafeFileName(threadParam.Settings.BackupLocation + "\\" + threadParam.ServerName.Replace('\\', '_') + "\\" + smoDatabase.Name.Replace('\\', '_'));
                    int sequence = 0;

                    try
                    {
                        Directory.CreateDirectory(backupLocation);

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Databases");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Database" + ".sql")))
                        {
                            WriteScripts(tw, smoDatabase.Script(), "");
                            tw.Close();
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Schemas");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Schemas" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.Schema smoSchema in smoDatabase.Schemas)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smoSchema.Script(), smoDatabase.Name);
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Database Rules");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Database Rules" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.Rule smoRule in smoDatabase.Rules)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smoRule.Script(), smoDatabase.Name);
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Database Triggers");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Database Triggers" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.DatabaseDdlTrigger smotrigger in smoDatabase.Triggers)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smotrigger.Script(), smoDatabase.Name);
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Database Roles");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Database Roles" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.DatabaseRole smoRole in smoDatabase.Roles)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smoRole.Script(), smoDatabase.Name);
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Users");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Users" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.User smoUser in smoDatabase.Users)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smoUser.Script(), smoDatabase.Name);
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Types");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Data Types" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.UserDefinedDataType smoType in smoDatabase.UserDefinedDataTypes)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smoType.Script(), smoDatabase.Name);
                            }
                        }


                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Types");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Types" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.UserDefinedType smoType in smoDatabase.UserDefinedTypes)
                            {
                                Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                scriptingOptions.IncludeIfNotExists = true;
                                scriptingOptions.Permissions = true;
                                WriteScripts(tw, smoType.Script(), smoDatabase.Name);
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Tables");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Tables" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.Table smoTable in smoDatabase.Tables)
                            {
                                if (!smoTable.IsSystemObject)
                                {
                                    Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                    scriptingOptions.DriAll = true;
                                    scriptingOptions.DriAllConstraints = true;
                                    scriptingOptions.DriAllKeys = true;
                                    scriptingOptions.Statistics = true;
                                    scriptingOptions.Triggers = true;
                                    scriptingOptions.ScriptDataCompression = true;
                                    scriptingOptions.Permissions = true;
                                    scriptingOptions.Indexes = true;
                                    scriptingOptions.IncludeIfNotExists = true;
                                    WriteScripts(tw, smoTable.Script(scriptingOptions), smoDatabase.Name);
                                }
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Views");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Views" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.View smoView in smoDatabase.Views)
                            {
                                if (!smoView.IsSystemObject)
                                {
                                    Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                    scriptingOptions.DriAll = true;
                                    scriptingOptions.DriAllConstraints = true;
                                    scriptingOptions.DriAllKeys = true;
                                    scriptingOptions.Triggers = true;
                                    scriptingOptions.ScriptDataCompression = true;
                                    scriptingOptions.Permissions = true;
                                    scriptingOptions.Indexes = true;
                                    scriptingOptions.IncludeIfNotExists = true;
                                    WriteScripts(tw, smoView.Script(), smoDatabase.Name);
                                }
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Procedures");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Procedures" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.StoredProcedure smoProcedure in smoDatabase.StoredProcedures)
                            {
                                if (!smoProcedure.IsSystemObject)
                                {
                                    Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                    scriptingOptions.IncludeIfNotExists = true;
                                    scriptingOptions.Permissions = true;
                                    WriteScripts(tw, smoProcedure.Script(), smoDatabase.Name);
                                }
                            }
                        }

                        Console.WriteLine(threadParam.ServerName + "." + smoDatabase.Name + "." + "Functions");
                        using (TextWriter tw = new StreamWriter(SafeFileName(backupLocation + "\\" + (++sequence).ToString("000") + " " + "Functions" + ".sql")))
                        {
                            tw.WriteLine("USE [" + smoDatabase.Name + "]");
                            tw.WriteLine("GO");

                            foreach (Microsoft.SqlServer.Management.Smo.UserDefinedFunction smoFunction in smoDatabase.UserDefinedFunctions)
                            {
                                if (!smoFunction.IsSystemObject)
                                {
                                    Microsoft.SqlServer.Management.Smo.ScriptingOptions scriptingOptions = new Microsoft.SqlServer.Management.Smo.ScriptingOptions();
                                    scriptingOptions.IncludeIfNotExists = true;
                                    scriptingOptions.Permissions = true;
                                    WriteScripts(tw, smoFunction.Script(), smoDatabase.Name);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("BackupServer()->Exception: " + ex.Message + "(" + threadParam.ServerName + "." + smoDatabase.Name + "." + ")");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("BackupServer()->Exception: " + ex.Message);
            }

            lock (LockObject)
            {
                ActiveThreads--;
            }
        }

        static bool CanInclude(BackupSettings settings, string databaseName)
        {
            if(settings.IncludeDatabases.Count == 0)
            {
                return true; //No include filters, only apply exclude filters.
            }

            foreach (string filter in settings.IncludeDatabases)
            {
                if (filter.Substring(filter.Length - 1) == "*") //Trailing wildcard.
                {
                    if (databaseName.IndexOf(filter.Substring(0, filter.Length - 1)) == 0)
                    {
                        return true;
                    }
                }
                else if (filter.Substring(0, 1) == "*") //Leading wildcard.
                {
                    int index = databaseName.IndexOf(filter.Substring(1, filter.Length - 1));
                    if ((databaseName.Length - index) == filter.Length - 1)
                    {
                        return true;
                    }
                }
                else if (databaseName == filter) //Exact match.
                {
                    return true;
                }
            }

            return false;
        }

        static bool CanExclude(BackupSettings settings, string databaseName)
        {
            if (settings.ExcludeDatabases.Count == 0)
            {
                return false; //No exclude filters, only apply include filters.
            }

            foreach (string filter in settings.ExcludeDatabases)
            {
                if (filter.Substring(filter.Length - 1) == "*") //Trailing wildcard.
                {
                    if (databaseName.IndexOf(filter.Substring(0, filter.Length - 1)) == 0)
                    {
                        return true;
                    }
                }
                else if (filter.Substring(0, 1) == "*") //Leading wildcard.
                {
                    int index = databaseName.IndexOf(filter.Substring(1, filter.Length - 1));
                    if ((databaseName.Length - index) == filter.Length - 1)
                    {
                        return true;
                    }
                }
                else if (databaseName == filter) //Exact match.
                {
                    return true;
                }
            }

            return false;
        }

        static string SafeFileName(string fileName)
        {
            string uncDrive = fileName.Substring(0, 2);
            fileName = fileName.Substring(2);
            fileName = fileName.Replace('/', '\\');
            fileName = fileName.Replace(':', '_');
            fileName = fileName.Replace('?', '_');
            fileName = fileName.Replace('\"', '_');
            fileName = fileName.Replace('<', '_');
            fileName = fileName.Replace('>', '_');
            fileName = fileName.Replace('|', '_');
            fileName = fileName.Replace("\\\\", "\\");
            fileName = uncDrive + fileName;
            return fileName;
        }

        static void WriteSegmentTag(TextWriter tw, string segmentText)
        {
            Console.WriteLine("\t" + segmentText);
            System.Text.StringBuilder hashes = new System.Text.StringBuilder();
            string halfHash;

            for(int i = 0; i < 100; i++)
            {
                hashes.Append("#");
            }

            halfHash = hashes.ToString().Substring(0, (50 - (segmentText.Length / 2)) - 1);

            tw.WriteLine("/*");
            tw.WriteLine(hashes);
            tw.WriteLine(halfHash + " " + segmentText + " " + halfHash);
            tw.WriteLine(hashes);
            tw.WriteLine("*/");
        }

        static void WriteScripts(TextWriter tw, StringCollection scripts, string databaseName)
        {
            foreach (string script in scripts)
            {
                tw.WriteLine(script);
            }
        }

        static void PrintUsage()
        {
            Console.WriteLine("/?         - Displays this help message.");
            Console.WriteLine("/Threads:  - The maximum number of parallel backups to perform.");
            Console.WriteLine("/Folder:   - Specifies where the scripts should be saved.");
            Console.WriteLine("/User:     - Windows username to use for connecting to SQL Server.");
            Console.WriteLine("/Password: - Windows password to use for connecting to SQL Server.");
            Console.WriteLine("/Servers:  - Comma seperated list of server to backup.");
            Console.WriteLine("/Include:  - Comma seperated list of databases to include in the backup.");
            Console.WriteLine("/Exclude:  - Comma seperated list of databases to eexclude from the backup.");
        }
    }
}