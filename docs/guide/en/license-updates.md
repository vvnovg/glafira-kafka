# License and updates

This chapter covers what Pro adds, how to activate a license key, what happens when the license expires, how to update the app, the [[ui:sidebar.about]] screen and the diagnostics archive.

## Free and Pro {#free-and-pro}

Free covers everyday work with Kafka: all security protocols including mTLS, the cluster overview and topics, browsing messages, the Producer with batch sending, consumer groups and offset reset. All interface languages are included as well. See [Interface language](getting-started.md#language).

Pro adds:

- consumer sessions: reading as a group member, committing offsets, pausing partitions. See [Consumer sessions](consumer-sessions.md#what-is);
- prod mode: creating prod connections, switching connections to prod and clearing [[ui:common.readOnly]] on a prod connection. See [Environments and prod](connections.md#environments).

In Free, these features are visible but unavailable. The consumer session commands are disabled and marked with a lock and `Pro`. The prod option is shown as [[ui:connections.form.prodPro]]. If an action needs Pro, the error says [[ui:errors.license.proRequired]] and offers the [[ui:common.openLicense]] button.

The [[ui:tabs.license]] screen lists consumer sessions and prod mode under [[ui:license.whatProGives]].

## Activate a key {#activate}

A license key is a line of text issued to a person or to an organization. The app checks the key on your computer and does not contact any server.

1. Click [[ui:tabs.license]] at the bottom of the left panel.
2. Paste the whole key into [[ui:license.keyLabel]].
3. Click [[ui:license.activate]].

Pro features become available at once. The screen shows [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] or [[ui:license.kindOrganization]] ) and [[ui:license.expires]]: a date, or [[ui:license.perpetual]]. The key is saved in the app data folder, so it stays after a restart or an update. A new key replaces the previous one.

If the key is not accepted, the reason is shown under the field:

- [[ui:errors.license.malformed]] — part of the key is missing or changed. Copy the key again as one line, without extra characters.
- [[ui:errors.license.badSignature]] — the key was not issued for Glafira, or it was edited.
- The key has expired — the message gives its end date. Ask for a new key.
- The key needs a newer version of the app — the message gives the key version. Update the app first, see [Updates](license-updates.md#updates).

To remove the key, click [[ui:license.removeKey]]. The app asks [[ui:license.removeQuestion]] Click [[ui:license.removeYes]] to confirm. Your data stays.

An organization can activate Pro for all users through a policy. The key field and buttons are then hidden, and the screen says [[ui:license.managed]] A key that you entered yourself is not used while the policy key is set. The [[ui:license.policyTitle]] section shows where the policy comes from and lists its errors. If the organization key is invalid, the reason is shown on the [[ui:tabs.license]] screen. Contact your administrators in this case. Administrators find the policy settings in `docs/admin.md` in the Glafira source code.

## Expiry {#expiry}

A key can have an end date, shown in [[ui:license.expires]]. The license is valid until the end of that day.

When fewer than 14 days are left, a notice above the tabs says how many days remain, and the [[ui:tabs.license]] screen shows the end date. [[ui:license.notice.more]] in the notice opens the [[ui:tabs.license]] screen. The `×` button hides the notice until the app restarts.

After the end date, the plan switches to Free. The notice and the [[ui:tabs.license]] screen say that the license has expired and that Pro features are disabled. What changes:

- New consumer sessions cannot be started. In a session that is already running, commits and pausing partitions are refused with an error that gives the end date.
- Prod connections keep working with the same confirmations. New prod connections cannot be created, and [[ui:common.readOnly]] cannot be cleared on a prod connection.

Connections, passwords, templates and send history are kept. To continue with Pro, activate a new key as described in [Activate a key](license-updates.md#activate).

## Updates {#updates}

When it starts, the app checks once whether a new version is available. If there is one, a notice above the tabs gives its version. [[ui:license.notice.more]] opens the [[ui:sidebar.about]] screen. If the check fails, for example without internet access, no message is shown.

To check manually:

1. Open [[ui:sidebar.about]] at the bottom of the left panel.
2. In [[ui:about.updates.title]], click [[ui:about.updates.check]].
3. If a new version is available, the screen shows its number and release notes. Click [[ui:about.updates.install]].

The app downloads the update, checks its signature, installs it and restarts. A download interrupted by a network problem is retried automatically, up to three attempts in total. If all three attempts fail, the app reports [[ui:errors.updates.downloadInterrupted]]. If the signature check fails, the app reports [[ui:errors.updates.badSignature]].

Installing may ask for an administrator password, because the app is installed for all users of the computer: by the `.pkg` installer on macOS, the `.msi` installer on Windows, or a `.deb` or `.rpm` package on Linux. Users without administrator rights cannot update such an installation.

If messages are being read, a consumer session is running or a batch is being sent, the app asks first and says how many sessions are active. Click [[ui:about.updates.stopAndUpdate]] to stop them and continue, or [[ui:common.cancel]]. Connections, settings, the license key and other data are kept after an update.

Updates are downloaded from the releases page on GitHub. If the check fails with [[ui:errors.updates.checkFailed]], check your network or proxy. [[ui:errors.updates.notConfigured]] means that this build was made without the updater key, for example a development or test build. In that case, download new versions from the releases page: https://github.com/vvnovg/glafira-kafka-releases/releases

An organization can turn off update checks through a policy, or point them to an internal mirror. When checks are turned off, the [[ui:about.updates.title]] section says [[ui:errors.updates.managed]] after you click [[ui:about.updates.check]]. Like [[ui:errors.updates.notConfigured]], this message appears only after the click; the check at startup shows nothing in these cases.

## About {#about}

Open [[ui:sidebar.about]] at the bottom of the left panel. On macOS, it is also in the Help menu as [[ui:menu.about]].

The screen shows a short description of the app and:

- [[ui:about.version]] — the version number, and [[ui:about.build]] — the identifier of the source code it was built from. Include both in a support request.
- [[ui:about.system]] — the operating system and processor architecture.
- [[ui:about.tier]] — Free or Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] or [[ui:about.policyNone]]. Errors in the policy are listed below.
- [[ui:about.dataDir]] — the folder with connection profiles, templates, history, settings and the license key.
- [[ui:about.logDir]] — the folder with log files. See [Logs](troubleshooting.md#logs).

The [[ui:about.updates.title]] and [[ui:about.diagnostics.title]] sections are also on this screen.

## Diagnostics archive {#diagnostics}

The diagnostics archive collects what support needs to understand a problem. To create it:

1. Open [[ui:sidebar.about]].
2. In [[ui:about.diagnostics.title]], click [[ui:about.diagnostics.collect]]. On macOS, you can also choose [[ui:menu.diagnostics]] in the Help menu.
3. Choose where to save the file. The suggested name is `diagnostics-<date>.zip`.

When the archive is saved, the screen shows its path. The ZIP file contains:

- `info.json` — the app version and build, the operating system, the data and log folder paths, the policy status and the license status (plan, licensee, type, end date);
- `connections.json` — connection profiles: names, environments, bootstrap servers, protocols, SASL usernames, certificate file paths, extra properties and timeouts;
- `logs/` — the app log files.

The archive contains no passwords: they are kept in the system secret store, and extra properties with passwords and secrets are refused when a connection is saved. The license key and certificate files are not included either. Host names, usernames and file paths are stored as plain text, and the logs contain broker addresses from Kafka error messages. Look through the archive before you send it outside your company.

The app sends nothing by itself. Attach the archive to your support request yourself, see [Contact support](troubleshooting.md#support).
