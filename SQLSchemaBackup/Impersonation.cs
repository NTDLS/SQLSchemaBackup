using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.Principal;
using Microsoft.Win32.SafeHandles;

namespace SQLSchemaBackup
{
    /// <summary>
    /// Logs on as a Windows user so that work can be run under that identity.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public sealed class Impersonation : IDisposable
    {
        private const int LOGON32_LOGON_INTERACTIVE = 2;
        private const int LOGON32_PROVIDER_DEFAULT = 0;

        [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern bool LogonUser(string userName, string domain, string password,
            int logonType, int logonProvider, out SafeAccessTokenHandle token);

        private readonly SafeAccessTokenHandle _token;

        public Impersonation(string userName, string domainName, string password)
        {
            if (!LogonUser(userName, string.IsNullOrEmpty(domainName) ? "." : domainName, password,
                LOGON32_LOGON_INTERACTIVE, LOGON32_PROVIDER_DEFAULT, out _token))
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }

        /// <summary>
        /// Runs the action as the logged-on user. Call this on each thread that needs the identity.
        /// </summary>
        public void Run(Action action) => WindowsIdentity.RunImpersonated(_token, action);

        public void Dispose() => _token.Dispose();
    }
}
