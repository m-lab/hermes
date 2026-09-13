---
name: github-publish-followups
description: Publish follow-up GitHub changes safely when an earlier pull request was squash-merged, rebased, or otherwise merged with rewritten history. Use alongside a GitHub publishing skill when reusing the old feature branch creates conflicts, GitHub shows already-merged commits again, or new commits were added after the branch's prior PR merged.
---

# Publish GitHub Follow-ups

**Created by Loqman Salamatian / [GitHub](https://github.com/Burdantes)**

Extend the normal GitHub publishing workflow with a preflight for branches whose
commit ancestry no longer matches the target after a squash or rebase merge.

**Licence:** This skill is released under the Creative Commons Attribution 4.0
International licence. You may share and adapt it with attribution.

**Feedback & Support:** Report methodology issues through the skill author's
public GitHub profile. Distinguish a weakness in this workflow from an agent
failing to follow it.

## Preserve the original state

Before switching branches:

1. Inspect the working tree and keep unrelated or untracked user files out of
   the operation.
2. Fetch the current target branch and the old feature branch.
3. Record the prior pull request, its base, its head commit at merge, its merge
   method, and the old branch's current head.
4. Do not force-push, delete, or rewrite the old branch while diagnosing.

If the working tree cannot be switched safely, use a separate worktree or stop
for direction.

## Detect rewritten merged history

Do not trust an ahead/behind count alone. A squash merge creates a new commit on
the target, so the original feature commits may appear unmerged even though
their patch is already present.

Confirm all of the following:

- the earlier pull request is merged;
- the current branch contains commits after the prior PR's recorded head;
- GitHub's comparison includes the old PR's commits or repeats its files;
- merging the entire old branch produces conflicts that the follow-up patch
  alone does not.

Identify the exact follow-up range as:

`<prior-pr-head>..<old-branch-current-head>`

If that boundary is uncertain, stop. Guessing which commits are new can silently
drop or duplicate changes.

## Reconstruct from the current target

1. Create a fresh branch from the fetched target branch, following the active
   environment's branch-naming convention.
2. Cherry-pick only the follow-up commits after the prior PR head, in
   chronological order.
3. Resolve conflicts only if they arise from genuine changes made after the
   earlier merge. If the old PR's full patch is replaying, abort and correct the
   boundary instead.
4. Preserve original authorship and review the new committer identity before
   publishing.

Never solve this situation by blindly rebasing or merging the whole reused
branch onto the target; that replays the history the fresh branch is intended
to exclude.

## Prove equivalence and mergeability

Before pushing:

- compare the old follow-up range with the fresh branch's target-relative diff;
- verify changed filenames, additions/deletions, and commit order;
- run `git diff --check`;
- run the focused tests and linters for the follow-up files;
- use a three-way merge check to confirm the follow-up patch applies cleanly to
  the current target.

After opening the pull request, verify through GitHub that:

- only the intended follow-up commits and files are present;
- the base and head branches are correct;
- mergeability is clean;
- required checks complete successfully.

Keep the old branch available until the replacement pull request is verified.

## Report the repair

Explain that the conflict came from rewritten Git ancestry, not necessarily
from incompatible code. Report:

- the prior merged PR and its recorded head;
- the new branch and exact cherry-picked commits;
- equivalence and test evidence;
- pull-request mergeability and checks;
- any manual action still required, such as marking a draft ready.

Do not claim completion merely because the branch pushed successfully.
