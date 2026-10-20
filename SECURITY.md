# Security

## Reporting a vulnerability

Report it privately, through GitHub's
[private vulnerability reporting](https://github.com/Rosebud-Biosciences/ferry/security/advisories/new)
(the repository's Security tab, "Report a vulnerability"). Please do not open a
public issue, pull request or discussion about it.

Say what an attacker can do, what they need first (access to the instrument
PC, the store, the network, the config), the version (`ferry --version`) and
platform, and how to reproduce it. We aim to acknowledge a report within a
week, and to agree on a disclosure date with you once there is a fix.

## Supported versions

ferry is in alpha: fixes go into the next release only. Run the latest one.

## What ferry trusts

These are by design, and reports that rely on them alone are not
vulnerabilities:

- **The config is code.** Registrars and notifiers run the commands it names,
  with ferry's privileges; whoever can edit the config can run anything as
  the account ferry runs as. Keep it, and the state directory, writable only
  by that account and administrators.
- **Secrets sit where you put them.** Store credentials and webhook URLs (a
  Slack webhook URL is itself a credential) belong in an `--env-file` readable
  only by the service's account, or a credentials profile -- not in the config
  or the machine's environment. ferry does not log them; `ferry doctor` shows
  only a webhook's host.
- **`/metrics` and `/healthz` have no authentication.** They are off unless
  `metrics.listen` is set; bind them to localhost or a management network.
- **The watched folders are the instrument's.** ferry uploads what lands
  there, and with `finalize.action: move` or `delete` it removes source files
  once their copies are verified. It never deletes from a store.
