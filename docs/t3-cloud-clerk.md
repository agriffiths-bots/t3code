# T3 Cloud Clerk Setup

T3 Cloud uses one Clerk application for web, desktop, and mobile authentication. The relay accepts
Clerk JWTs only when they are generated from the `t3-relay` template with the relay URL as the
audience.

## Application Keys

Web, desktop, and mobile use checked-in public development defaults from
`packages/shared/src/relayAuth.ts`, so a fresh clone can use T3 Cloud without creating local
environment files. These values are safe to embed in client builds: the Clerk publishable key and
relay URL are public identifiers, not secrets.

To point all clients at another Clerk/relay deployment, add a repository-root `.env` or
`.env.local` file:

```dotenv
T3CODE_CLERK_PUBLISHABLE_KEY=<publishable key>
T3_RELAY_URL=https://relay.example.com
```

The shared client loader projects these canonical values into the framework-specific
`VITE_CLERK_PUBLISHABLE_KEY`, `VITE_T3_RELAY_URL`, and
`EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY` aliases. Existing aliases remain accepted as overrides for
compatibility, but new client configuration should use the canonical names.

Configuration precedence is:

1. Process or CI environment variables.
2. Repository-root `.env.local`.
3. Repository-root `.env`.
4. Checked-in public development defaults.

Release builds read `T3CODE_CLERK_PUBLISHABLE_KEY` and `T3_RELAY_URL` from GitHub Actions repository
variables. EAS preview and production builds should define the same client-facing values in their
EAS environment.

For a hosted relay deployment, copy `infra/relay/.env.example` to `infra/relay/.env`. The relay
deployment reads `T3_RELAY_DOMAIN` and `T3_RELAY_ZONE_NAME` through Effect `Config`, with the
checked-in shared values as defaults. `bun --cwd infra/relay run deploy` invokes Alchemy from the
relay directory, so Alchemy loads `infra/relay/.env`. The relay still requires `CLERK_SECRET_KEY` as
an Alchemy secret. Never put `CLERK_SECRET_KEY` in a client application environment or commit it to
the repository.

## JWT Template

In **Clerk Dashboard > JWT templates**, create a template with:

| Setting | Value                                                  |
| ------- | ------------------------------------------------------ |
| Name    | `t3-relay`                                             |
| Claims  | `{ "aud": "https://t3code-relay.ineededadomain.com" }` |

The `aud` value must be the deployed relay public URL, with no trailing slash. It must match the
client-facing `T3_RELAY_URL` and the HTTPS URL derived from the deployment's `T3_RELAY_DOMAIN`. If
the relay domain changes, update both values and the JWT template.

## Desktop OAuth Redirect Allowlist

The desktop app opens OAuth in the system browser and returns to the app with a custom URL scheme.
In **Clerk Dashboard > Native applications**, enable native application support and add these
entries under the mobile SSO redirect allowlist:

```text
t3code-dev://auth/callback
t3code://auth/callback
```

The first entry is for local desktop development. The second is for packaged desktop builds.
The app also adds a request-scoped `t3_state` query parameter and validates it on callback.

The current mobile UI uses Clerk's native authentication view. If a future mobile browser OAuth
flow uses a custom redirect URI, add that exact URI to the same allowlist.

## Enable Waitlist Access

For a private beta where people should request access, use **Clerk Dashboard > Waitlist**:

1. Toggle on **Enable waitlist** and save.
2. Review requests on the same page and select **Invite** or **Deny**.

Signed-out web and desktop users see Clerk's waitlist enrollment as the T3 Cloud page content,
while approved signed-in users see cloud settings. The browser app also uses `/settings/cloud` as
its Clerk waitlist URL.

On mobile, signed-out users open **Settings > T3 Account** to reach `/settings/waitlist` within the
Settings form sheet. It submits enrollment through Clerk's `useWaitlist()` flow because the prebuilt
`<Waitlist />` component is web-only in the Expo SDK. Approved users can use **Sign in** from that
screen.

## Alternative: Known-User Allowlist

For a closed beta where all permitted users are known in advance, use an allowlist instead of a
request-and-approval waitlist:

To restrict the beta to permitted email addresses or domains:

1. In **Clerk Dashboard > Restrictions > Allowlist**, add each permitted email address or email
   domain.
2. Enable the allowlist and save.
3. Alternatively, enable **Restricted mode** when all new users must be explicitly invited or
   manually created without a waitlist request flow.

Do not enable an empty allowlist: it blocks all new sign-ups.

Clerk allowlists control who can sign up. They do not revoke an existing user's active cloud
access. To remove an already-created user's access, ban that user in Clerk so their active
sessions are ended and future sign-ins are rejected.
