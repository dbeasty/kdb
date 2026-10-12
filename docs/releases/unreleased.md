# Unreleased

## Behaviour changes — read before upgrading

**Unique constraints over arrays are multikey, as in MongoDB.** A field holding an array used to
be one opaque value under a unique constraint, so `["a","b"]` and `["b","c"]` were "different"
and both went in. Each element is now a claim of its own: no two documents may share an element,
a compound constraint with array parts claims every combination, a document may repeat an element
in its own array, an empty array or a null element claims nothing, and an array nested inside an
element is that element's value. A unique rule over a list (one external login per account) can
now be stated. `CREATE UNIQUE INDEX` over existing data whose arrays already overlap is refused;
check for overlaps before upgrading if a namespace has a unique constraint on an array field.
Go and Kotlin engines agree.
