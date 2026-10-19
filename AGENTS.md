# Release

- Release flow: commit, `just bump <part>`, commit the bump, push, `just release github`, then wait for the Release workflow to pass (`gh run watch <id> --exit-status`).
- The Release workflow signs macOS binaries with the InfraOps LLC Developer ID (team Q8UP8C53Y8) from the `MACOS_SIGNING_P12_BASE64` and `MACOS_SIGNING_P12_PASSWORD` secrets and the `MACOS_SIGNING_IDENTITY` variable, so macOS privacy grants survive updates. The macOS jobs fail rather than ship unsigned when these are missing.

# Homebased skill

- `.agents/skills/homebased` is the agent skill that ships with homebased. Change it in the same commit as the behavior it documents, so each release tag's skill matches its binary.
