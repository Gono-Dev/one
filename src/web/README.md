# Admin Web Rendering Reference

This directory captures the intended server-rendered HTML shape for the `/admin` user-management
surface described in `plan/10-admin.md` and the app-password permission model in
`plan/11-permission.md`.

The visual direction follows the logged-in `seek.li` management pages:

- fixed left sidebar, white surface, pale gray page background;
- compact content column with 8px cards and clear section titles;
- dense operational forms instead of a marketing-style dashboard;
- dark navy primary actions, quiet secondary buttons, red outlined danger actions;
- muted helper text, thin borders, no decorative gradients or nested cards.

The prototype is static on purpose. The Rust implementation should render equivalent markup directly
from `src/admin/html.rs` and may inline `admin.css` or serve it as a small static asset.

Current implementation note: `/admin/settings` edits only restart-applied runtime settings stored
in SQLite `settings`. Non-editable values such as storage paths, database path, bind address, TLS
files, and the generated `instance.id` stay outside the form.

## Files

- `admin-users.html`: target markup for the user list, create-user form, app-password scope forms,
  per-password WebDAV start points, and one-time app-password notice.
- `admin.css`: shared design tokens and component styles for the server-rendered admin pages.

## Rendering Rules

- Escape all dynamic text before writing it into HTML.
- Every mutating form must include the process-local CSRF token as `_csrf`.
- Keep destructive actions visually separated with `.button-danger`.
- Keep password output in `.secret-panel`, only after create/reset responses.
- Render each app password start point as `mount_path`, `storage_path`, and `permission`.
- Display `full` as "Full access"; keep the submitted value `full`.
- Keep each user's alias edit, enabled/disabled action, display-name edit, and delete action beside
  the username in the same summary line. Disable controls that the backend will reject, such as
  deleting `gono`.
- Edit aliases, display names, and app-password expiry through the centered popover form pattern
  modeled after seek.li mailbox remarks. App-password expiry controls stay hidden until the
  `Save expiry` action opens the editor, including the create-app-password form.
- For every app password, render the current expiry status and provide an expiry editor with both
  "At time" and "Never" options. Use plain text inputs with the placeholder `YYYY-MM-DDTHH:MM`
  instead of native datetime controls so browser locale text never appears in the UI.
