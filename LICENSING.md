# Licensing

KDB is **dual-licensed** by Limidus Corp. You may use it under either of the
following, at your option:

1. The **GNU Affero General Public License, version 3** (see [`LICENSE`](LICENSE)) — free of charge.
2. A **commercial license** from Limidus Corp — for use that AGPL-3.0 does not permit.

Copyright © 2026 Limidus Corp. All rights not granted under one of these two
licenses are reserved.

---

## Which one do you need?

### AGPL-3.0 — free

Use this if you are comfortable with reciprocal source disclosure. In summary,
AGPL-3.0 requires that:

- Any derivative work you **distribute** is itself licensed under AGPL-3.0, with complete corresponding source.
- Any modified version you make available to users **over a network** — the AGPL's §13 clause, and the reason this license rather than GPL-3.0 — must offer those users the complete corresponding source of your modified version.
- Copyright and license notices are preserved.

Embedding KDB in a product you ship, or running a modified KDB behind a hosted
service, therefore places your product or service under AGPL-3.0 as well. That
is the intended trade.

### Commercial — paid

Use this if you want to embed KDB in a closed-source product, ship it inside a
proprietary application, or offer a hosted service built on it without
publishing your own source. The commercial license grants the same code under
terms that carry no copyleft or network-disclosure obligation, and can include
warranty and support terms that the AGPL explicitly disclaims.

Contact **davja@limidus.com** to arrange one.

---

## How this works

Dual licensing is possible because Limidus Corp holds the copyright to the
entire codebase. The AGPL constrains *licensees*, not the copyright holder: as
the author, Limidus Corp may license the same code to anyone else under any
terms it chooses, as often as it chooses. Releasing under AGPL-3.0 does not
surrender that right, and it is not revoked by the AGPL grant already made to
the public.

Two things preserve it, and both matter:

- **Every line must be owned or compatibly licensed.** Copyright must remain
  consolidated in Limidus Corp. Contributed code that Limidus Corp does not own
  cannot be relicensed commercially, and a single unowned contribution merged
  into the tree is enough to block a commercial grant for the whole file.
- **Third-party dependencies bind the commercial build too.** A commercial
  license from Limidus Corp covers KDB's own code and nothing else. Any GPL- or
  AGPL-licensed dependency linked into a distributed build would impose its own
  copyleft on that build regardless of KDB's terms. Permissive dependencies
  (Apache-2.0, MIT, BSD) are safe here; strong-copyleft ones are not.

## Contributions

By submitting a contribution you assign copyright in it to Limidus Corp, or
grant Limidus Corp a perpetual, irrevocable, worldwide right to license that
contribution under both AGPL-3.0 and commercial terms. Without this, the
contribution cannot be included in a commercially licensed build.

External contributions should be covered by a signed CLA before merge.
