// Bounded Windows ACL test. No credentials, network, or vendor execution.
using System;
using System.IO;
using System.Security;
using System.Security.Principal;
using System.Threading;

public static class IsolationProbe
{
    static bool Denied(Action operation)
    {
        try { operation(); return false; }
        catch (UnauthorizedAccessException) { return true; }
        catch (SecurityException) { return true; }
    }

    public static int Main(string[] args)
    {
        if (args.Length != 1 || (args[0] != "service" && args[0] != "agent"))
            return 2;
        const string root = @"G:\Hivemind-Isolated";
        string role = args[0];
        string own = Path.Combine(root, role);
        string other = Path.Combine(root, role == "service" ? "agent" : "service");
        string sid = WindowsIdentity.GetCurrent().User.Value;
        int pid = System.Diagnostics.Process.GetCurrentProcess().Id;
        Thread.Sleep(15000); // Allow bounded WMI startup before observing the PID.
        File.WriteAllText(Path.Combine(own, "own-write.txt"), "EXPECTED_OWN_WRITE");
        bool cross = Denied(() => File.WriteAllText(Path.Combine(other, "forbidden-write.txt"), "MUST_NOT_WRITE"));
        bool secret = role != "agent" || Denied(() => File.ReadAllText(
            Path.Combine(root, "service", "credentials", "sentinel.txt")));
        bool tools = Denied(() => File.WriteAllText(Path.Combine(root, "tools", "forbidden-write.txt"), "MUST_NOT_WRITE"));
        string json = String.Format(
            "{{\"role\":\"{0}\",\"sid\":\"{1}\",\"pid\":{2},\"own_write\":true," +
            "\"cross_write_denied\":{3},\"service_secret_read_denied\":{4}," +
            "\"trusted_tools_write_denied\":{5}}}",
            role, sid, pid, cross.ToString().ToLowerInvariant(),
            secret.ToString().ToLowerInvariant(), tools.ToString().ToLowerInvariant());
        File.WriteAllText(Path.Combine(own, "probe-native-result.json"), json);
        return cross && secret && tools ? 0 : 3;
    }
}
