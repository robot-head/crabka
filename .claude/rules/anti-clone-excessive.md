# anti-clone-excessive

> Don't clone when borrowing works.

Source: https://github.com/leonardomso/rust-skills/blob/master/rules/anti-clone-excessive.md

## Why It Matters

`.clone()` allocates memory and copies data. When code only needs to read data,
borrow it (`&T`, `&str`, or an iterator over references) instead of cloning.
Unnecessary cloning wastes memory and CPU, and often hides unclear ownership.

## Prefer Borrowing

```rust
// Bad: takes ownership just to read.
fn print_name(name: String) {
    println!("{name}");
}
let name = "Alice".to_string();
print_name(name.clone());

// Good: borrow for read-only access.
fn print_name(name: &str) {
    println!("{name}");
}
let name = "Alice".to_string();
print_name(&name);
```

## Common Cleanup Patterns

- Iterate over references: use `for item in &items` or `items.iter()` instead of
  `items.clone()` when the loop only reads.
- Compare directly: use `input == expected` instead of `input.clone() == expected`.
- Return references when callers do not need ownership, e.g. `&str` instead of
  `String` for struct fields.
- Avoid cloning in hot loops; move invariant owned values outside the loop or use
  references.
- Prefer `Arc<T>` for intentional shared ownership instead of repeated deep
  clones.
- Use clone-on-write (`std::borrow::Cow`) when data is usually borrowed and only
  occasionally modified.

## When Cloning Is Appropriate

- Moving owned data into `async move`, threads, or spawned tasks that must outlive
  the current stack frame.
- Storing borrowed data in an owning struct or long-lived collection.
- Duplicating values intentionally for multiple owners where borrowing cannot
  satisfy lifetimes; consider `Arc<T>` when that pattern is frequent.
- Generated protocol/model code where owned conversions are required by the API.

## Enforcement

Keep these lints enabled in crate/workspace Clippy configuration where practical:

```toml
[lints.clippy]
clone_on_copy = "warn"
clone_on_ref_ptr = "warn"
redundant_clone = "warn"
```

Before introducing a new `.clone()`, check whether a borrow, move, `Arc`, or
`Cow` communicates the ownership requirement more clearly.
