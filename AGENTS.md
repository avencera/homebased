# Deploy

- Install homebased only from GitHub releases, on every machine, including this Mac mini. Do not run `just release local` or `cargo xtask release local`, and do not copy a locally built binary into place.
- Release flow: commit, `just bump <part>`, commit the bump, push, `just release github`, then wait for the Release workflow to pass (`gh run watch <id> --exit-status`).
- Deploy after the release has its assets. Prefer `cmd fleet update`, which updates ai5090, `code`, and the calling machine and syncs the skill on each. Otherwise run `homebased update` on each machine, or the install script with `--tag vX.Y.Z` when no working binary exists, then run `~/code/dotfiles/bin/homebased-skill-sync` there. Then check `homebased --version` and `homebased --json daemon status`.

# Homebased skill

- `.agents/skills/homebased` is the skill that agents load on every machine, including this Mac mini, but they read it from `~/.local/share/homebased/skill-src`, a clone pinned to the tag of the installed binary. Skill edits in this checkout reach agents only after they ship in a release and the machine updates and syncs.
- Change the skill in the same commit as the behavior it documents, so each release tag's skill matches its binary.
- Do not edit `~/.local/share/homebased/skill-src` or point the skill links at this checkout. `homebased-skill-sync` discards local edits there, and the dotfiles link (`~/code/dotfiles/agents/skills/homebased`) is what keeps every machine on its installed release.
