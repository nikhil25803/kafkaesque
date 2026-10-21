# Kafkaesque UI Design System

This document is the source of truth for Kafkaesque's read-only Go and HTMX
interface. The visual language follows the existing banner: charcoal-black
surfaces, strong white hierarchy, restrained semantic color, and dense but
readable operational data.

## Principles

- Keep Kafka information readable before making it decorative.
- Use color to reinforce meaning, never as the only indication of meaning.
- Prefer compact, stable layouts that do not jump during refreshes.
- Preserve keyboard, screen-reader, and reduced-motion access.
- Keep the future UI self-contained in the Kafkaesque binary.
- Do not introduce a light theme.

## Color

| Token | Hex | Usage |
| --- | --- | --- |
| Canvas | `#050505` | Application background |
| Sidebar | `#090909` | Navigation background |
| Surface | `#101010` | Cards, tables, and controls |
| Elevated Surface | `#171717` | Hovered or raised elements |
| Border | `#292929` | Default separators |
| Strong Border | `#404040` | Emphasized boundaries |
| Primary Text | `#F5F5F5` | Titles and primary content |
| Secondary Text | `#A3A3A3` | Supporting information |
| Muted Text | `#737373` | Placeholders and disabled content |
| Selected Surface | `#F5F5F5` | Active navigation item |
| Inverse Text | `#0A0A0A` | Text on selected white surfaces |
| Healthy | `#22C55E` | Connected, stable, and healthy |
| Warning | `#F59E0B` | Lag warnings and degraded states |
| Critical | `#EF4444` | Disconnected, failed, and unhealthy |
| Focus | `#FFFFFF` | Keyboard focus outline |

Primary and secondary text, semantic text, active navigation, and focus states
must meet WCAG AA contrast against their intended backgrounds. Muted Text is
for nonessential hints and disabled content, not body copy. Healthy, Warning,
and Critical must be paired with a visible label or icon and an accessible
name.

Use semantic colors only when the displayed value represents an active state:

| State | Color | Examples |
| --- | --- | --- |
| Neutral | Primary or Secondary Text | Zero counts, inventory, empty groups, and topic levels |
| Healthy | Healthy | Connected, stable, healthy, and positive healthy counts |
| Warning | Warning | Degraded, rebalancing, under-replicated, lagging, and warning-level lag |
| Error | Critical | Failed, unhealthy, dead, offline, and blocking UI errors |

A zero warning or error count is neutral, not yellow, red, or green. Color must
describe the current value rather than the category that the value measures.

## Typography

Use locally available system fonts; do not require a font download.

```css
--font-sans: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
  "Segoe UI", sans-serif;
--font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas,
  "Liberation Mono", monospace;
```

| Role | Size / line height | Weight | Usage |
| --- | --- | --- | --- |
| Page title | `24px / 32px` | `700` | One title per page |
| Section title | `18px / 28px` | `600` | Major content sections |
| Card title | `14px / 20px` | `600` | Cards and widgets |
| Body | `14px / 20px` | `400` | Tables, controls, and descriptions |
| Label | `12px / 16px` | `600` | Column headings, badges, and metadata |
| Metric | `28px / 34px` | `700` | Dashboard totals |

Use the monospace stack for offsets, partition IDs, broker IDs, cluster IDs,
and other values where character alignment aids comparison. Do not use all caps
for sentences; reserve it for short labels and table headings.

## Spacing and Shape

Use a four-pixel base scale: `4`, `8`, `12`, `16`, `24`, `32`, and `48px`.

- Page padding: `24px` desktop, `16px` tablet and mobile.
- Card padding: `16px`; use `24px` for primary summary cards.
- Component gap: `8px`; section gap: `24px`; page-section gap: `32px`.
- Controls and navigation items: minimum `40px` height.
- Control radius: `6px`; card and panel radius: `8px`.
- Default border: `1px solid #292929`.
- Focus ring: `2px solid #FFFFFF` with a `2px` offset.
- Shadow: `0 8px 24px rgb(0 0 0 / 35%)`; use only for drawers and overlays.

## Application Layout

### Desktop — 1024px and wider

- Use a fixed `240px` sidebar spanning the viewport height.
- Place a text-only **Kafkaesque** wordmark at the top of the sidebar.
- Put the page title at the top-left of the main content area.
- Let the main content column fill the remaining width with a sensible maximum
  width for dense tables rather than centering a narrow marketing layout.
- Keep page actions and connection state on the right side of the page header.

### Tablet — 768px through 1023px

- Collapse the sidebar to a `72px` icon rail.
- Preserve the active state and expose each label through an accessible name.
- Show the visible label in a tooltip on hover and keyboard focus.
- Keep the page title in the main content header.

### Mobile — below 768px

- Replace the rail with a menu button and off-canvas navigation drawer.
- Trap focus inside the open drawer, close it with Escape, and return focus to
  the menu button after closing.
- Close the drawer after navigation.
- Stack metric cards and allow wide tables to scroll horizontally without
  clipping their first column.

## Navigation

Initial navigation contains, in this order:

1. Dashboard
2. Consumer Groups
3. Topics
4. Brokers
5. Cluster Health
6. Users

Do not show Alerts or Settings until those features exist. The shared shell
anchors the authenticated account role and logout control at the bottom of the
sidebar.

| State | Surface | Text and icon | Additional requirement |
| --- | --- | --- | --- |
| Active | Selected Surface | Inverse Text | Add `aria-current="page"` |
| Inactive | Transparent | Secondary Text | Keep the complete row clickable |
| Hover | Elevated Surface | Primary Text | Do not move or resize the item |
| Focus | Current surface | Current text color | Show the two-pixel Focus outline |
| Disabled | Surface | Muted Text | Add `aria-disabled="true"`; do not navigate |

Disabled navigation styling is reserved for future use and must not appear in
the initial interface.

## Components

### Headings

- Use one page title at the start of main content.
- Place optional supporting text below it in Secondary Text.
- Keep section actions aligned with the section title, wrapping below it on
  small screens.

### Metric Cards

- Show one primary number, a short label, and optional status or context.
- Use Primary Text for neutral values and semantic color only when the value
  itself represents health.
- Keep equal-height cards within a row and avoid decorative gradients.

### Health and Connection Widgets

- Always display a textual state such as `CONNECTED`, `HEALTHY`, `DEGRADED`,
  or `FAILED`.
- Use Healthy for connected/healthy, Warning for degraded, and Critical for
  failed/disconnected.
- Show connection latency, protocol, mechanism, and mTLS state as labeled
  values rather than relying on icons.
- Mark live status text with an appropriate `aria-live` region.

### Status Badges

- Use compact labels with a status icon or dot and explicit text.
- Do not use filled semantic backgrounds for large areas; prefer semantic text
  and borders on the dark Surface.
- Use consistent mappings across pages: Healthy/Stable/Connected, Warning/
  Degraded/Rebalancing/Unknown, and Critical/Failed/Dead/Unhealthy.

### Data Tables

- Use Surface for the table, Border for row separators, and no zebra striping.
- Keep headers visible, concise, and visually distinct using Label typography.
- Left-align names and states; right-align counts, offsets, and lag values.
- Use tabular numbers or the monospace stack for comparable numeric columns.
- Preserve deterministic row ordering and never encode status by color alone.
- On narrow screens, use horizontal scrolling; do not silently hide columns.

### Search and Filters

- Use a visible label or accessible name for every control.
- Use Surface with Border at rest, Strong Border on hover, and the Focus ring
  for keyboard focus.
- Keep search, state filters, and pagination separate so each can be changed or
  cleared independently.
- Display active filters and provide a clear action when any are applied.

### Buttons

| Variant | Surface | Text | Usage |
| --- | --- | --- | --- |
| Primary | Selected Surface | Inverse Text | Main action in a section |
| Secondary | Surface | Primary Text | Supporting actions |
| Destructive | Critical | Primary Text | Destructive actions only |

- Use verbs for labels and retain a `40px` minimum target height.
- Include hover, active, focus, disabled, and loading states.
- Destructive actions are documented for consistency but are not used by the
  read-only initial UI.

### Empty, Loading, and Error States

- Empty: explain that no results match and offer a clear-filter action when
  relevant.
- Loading: retain the component's dimensions and show a subtle skeleton or
  inline progress indicator.
- Error: show a short sanitized explanation and a retry action where retrying
  is safe.
- Unavailable: distinguish unavailable Kafka data from a valid empty result.
- Never replace the entire application shell for a section-level failure.

### Auto Refresh and Toasts

- Consumer Lag detail refreshes in the browser only and defaults to every 10
  seconds, with Off, 5, 10, 30, and 60 second choices.
- Remember the selected interval in browser storage, pause while the page is
  hidden, and never overlap refresh requests.
- Refresh the complete group snapshot atomically so its metrics, members,
  topics, and partition lag describe the same request.
- Keep the last successful content when a dynamic request fails. Show refresh
  failures as bottom-right warning toasts and other HTMX failures as critical
  toasts.
- Toasts use sanitized text, an accessible live region, a dismiss button, and
  never stack duplicate failures. Initial-page and login errors remain inline.

### Pagination

- Show previous and next controls, the current page, and total pages.
- Disable unavailable directions and keep their layout space reserved.
- Preserve current search and filters when navigating pages.
- Announce updated result counts after an HTMX page swap.

### Mobile Drawer

- Use Sidebar as the drawer background and a subtle overlay over main content.
- Include the wordmark and the same navigation order as desktop.
- Prevent background scrolling while open.
- Meet dialog semantics and focus-management requirements.

## Go and HTMX Guidance

- Render complete pages and reusable fragments with Go `html/template`.
- Vendor HTMX locally; do not load runtime assets from a CDN.
- Embed templates, CSS, JavaScript, and UI assets with `go:embed` so the
  existing `kafkaesque serve` process can remain a single binary.
- Require no Node build step.
- Keep Kafka requests out of templates and presentation helpers.
- Define fragment boundaries around independently refreshed widgets, tables,
  filters, and pagination rather than replacing the application shell.
- Preserve or deliberately restore keyboard focus after swaps.
- Use `aria-live="polite"` for refreshed summaries and result counts; reserve
  assertive announcements for blocking failures.
- Use `hx-indicator` and stable component dimensions to prevent flashing and
  layout shifts while a request is in flight.
- Respect `prefers-reduced-motion`; no transition is required for correctness.
- Render a normal link or form action wherever practical so navigation remains
  understandable without HTMX enhancement.

## Assets

Treat `asset/image/KafkaesqueBanner.png` and
`asset/image/KafkaesqueLogo.png` as visual references. They are detailed raster
compositions with black backgrounds and must not be used as compact sidebar
icons. The login page uses the complete logo composition in its branding panel;
the sidebar keeps the text wordmark until a transparent mark is designed.

## Login

- Use a 50/50 desktop split with the sign-in form on the left and branding
  on the right.
- Match the logo's dotted and orbital background with subdued CSS decoration.
- Stack the form before the branding panel on smaller screens.
- Keep generic errors, visible labels, browser autocomplete, and a full-width
  submit button below the credentials.

## User Management

- Keep the user list as the primary `/ui/users` content, with Add user in the
  page header.
- Render account creation on `/ui/users/new` in a centered panel and stack form
  fields on narrow screens.
- Keep roles neutral; they are authorization labels rather than health states.
- Show a five-segment red/green password meter and keep the detailed ×/✓ rules
  collapsed by default. Update both after a 250ms input debounce.
- Preserve safe form fields after validation failures, but never render a
  submitted password.
- State clearly that new accounts can sign in immediately.

## Initial Boundaries

- The existing `kafkaesque serve` process serves the read-only UI at `/ui` and
  preserves JSON endpoints under `/api/v1`.
- The Dashboard, Consumer Groups, Consumer Lag detail, Topics, Brokers, and
  Cluster Health pages render live request-time Kafka data through typed Go
  view models.
- Consumer Groups, Topics, and Brokers use locally vendored HTMX for filtered
  and paginated fragment swaps while preserving ordinary GET forms and links.
- Topic Detail remains deferred until it is explicitly approved.
- PostgreSQL-backed account login, logout, user listing, and Admin/User
  provisioning are implemented. Role-based permissions, password management,
  MFA, charts, alerts, settings, and new logo assets remain deferred.
