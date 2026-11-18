# Native Sign in with Apple

The iOS client `ace.jun.realEstate` uses the system Apple authorization sheet,
requesting only name and email. Private Relay email addresses are accepted.
The provider subject identifies the account; email never silently merges users.

## Server configuration

Enable Sign in with Apple for this App ID and create an app-scoped Sign in with
Apple key in the Apple Developer account. Keep the P8 outside Git, with local
file permissions `600` and a private parent directory. Set:

- `APPLE_BUNDLE_ID=ace.jun.realEstate`
- `APPLE_SIGNIN_TEAM_ID`: Apple Developer team ID
- `APPLE_SIGNIN_KEY_ID`: app-scoped Sign in with Apple key ID
- `APPLE_SIGNIN_PRIVATE_KEY_HOST_FILE`: protected external P8 path
- `APPLE_SIGNIN_TOKEN_KEY`: dedicated Fernet key from the protected secret store

Compose mounts the P8 read-only at `/run/secrets/apple-signin.p8`. Apply
`db/016_apple_signin.sql` before deploying the updated API. Keep the Fernet key
stable across deployments; existing refresh tokens depend on it for decryption.
Never commit these secrets or log credentials or provider response bodies.

## Authentication and revocation

`POST /api/mobile/v1/auth/apple/start` creates a short-lived server nonce and
one-use state bound to the client's PKCE challenge. The native client returns
the identity token, authorization code, state and original verifier to
`POST /api/mobile/v1/auth/apple/token`.

The API verifies Apple's RSA signature using Apple's fixed JWKS endpoint,
issuer, exact bundle audience, expiry, issuance time and nonce. It exchanges
the single-use code with Apple using an app-scoped ES256 client secret and
verifies that both identity tokens have the same subject. Only then does it
issue the normal account session. Apple refresh tokens are encrypted at rest.

The app-specific key registered on 2026-11-18 is
`BuildingLandAppleSignIn20261118` (key ID `QM82CGU9B5`, team `6G8922HHG3`),
associated only with the primary App ID `ace.jun.realEstate`. The approved
private copies are `~/.config/building-land/apple-signin/AuthKey_QM82CGU9B5.p8`
on the development Mac and API Mac mini, with permissions `600` inside a `700`
directory. This metadata does not contain the key or token-encryption secret.

Existing social-login browser callbacks are separate from this native flow.
Explicit account linking requires the original, recently authenticated bearer
session. Cancelling native authorization keeps the previous session intact.

The API checks saved Apple authorizations at most daily on authenticated mobile
requests. Confirmed Apple revocation invalidates local sessions; provider
outages leave the account and credentials intact for retry. Account withdrawal
and Apple identity disconnection revoke the Apple refresh token before the
local account change commits. Disconnecting the last login method is rejected.
Existing safeguards for purchased balances and pending transactions still apply.

## Verification

Run the authentication contracts against a disposable loopback PostgreSQL
cluster, never the production database:

```sh
python scripts/run_shared_archive_tests.py --auth-only \
  --postgres-bin /path/to/postgres/bin --work-dir /path/to/private/test-output \
  --pattern test_apple_signin.py
```

Also run the existing OAuth contracts and test the native sheet, Private Relay,
cancellation, subsequent login and withdrawal using a dedicated test account.
Regenerate provisioning profiles after enabling the Apple login capability.
