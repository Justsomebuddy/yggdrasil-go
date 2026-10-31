# Ironwood: bounded retries for an unresolved explicit lookup

This is a source patch and reproducible handoff, not an installed Yggdrasil
release or a general transport/session recovery fix.

## Problem and scope

Immediately after a peer enters the spanning tree, its Bloom routing data may
not yet be usable. A single local `SendLookup` can then remain unresolved until
another application write. The patch marks an existing rumor for at most two
maintenance-driven retries, respecting `max(1s, pathThrottle)`. Automatic retries
do not extend its expiration; an accepted signed path notification cancels them.

No new timers, map entries or packet buffers are allocated. Wire format,
encrypted sessions, replay guards and nonce handling are unchanged. The existing
latest-packet buffering policy is unchanged: this is not delivery of every
initial datagram, not a cure for packet loss along an Internet path, and not an
anonymity or censorship-resistance guarantee.

## Exact source

- Upstream dependency: https://github.com/Arceliar/ironwood
- Base: `0712eccbb9625c96df38beca1d9649c9bfd1a4d2`
- Patched commit: `3ea3608cf18c665ae5731ae710b2f371d6f370e7`
- Public source: https://github.com/Justsomebuddy/ironwood/tree/fix/explicit-lookup-retry
- Patch: [ironwood-explicit-lookup-retry.patch](ironwood-explicit-lookup-retry.patch)

The patch changes only seven dependency files, including a README explanation
and nine permanent regression/control tests. It does not vendor Ironwood into
Yggdrasil, modify Yggdrasil's go.mod, or change a default branch.

## Reproduce

Use a disposable checkout of the dependency at the exact base above. The
module requires Go 1.26 or newer. Keep any local modifications out of this
checkout before applying the patch.

```sh
git checkout --detach 0712eccbb9625c96df38beca1d9649c9bfd1a4d2
git apply --check /path/to/ironwood-explicit-lookup-retry.patch
git apply /path/to/ironwood-explicit-lookup-retry.patch
go test -race -count=3 ./network -run '^TestExplicitLookup'
go test -race -p 4 ./...
go vet -unreachable=false -p 4 ./...
go mod verify
```

The fresh-peer regression uses one public SendLookup and a real signed path
notification, without an application retry, external network or diagnostic
seams. Original source times out; the patch passes three race-enabled repeats.
Eight controls cover retry budget, repeated writes, throttle, transformed keys,
signed notification, expiration, remote admission and unmarked rumors.

Default full `go vet` has three inherited unreachable-code warnings in
network/core_test.go; the scoped command above does not claim a clean default
vet. A separate single-packet native QUIC/VPN experiment with a full Yggdrasil
0.5.15 CLI observed stock 0/1 and patched 1/1 cold ping, without application
warmup. This is one finite observation, not a statistical reliability claim.

## Contribution status

The public dependency branch is available for review. An official Ironwood PR
was attempted but GitHub rejected creation under its prior-contributors-only
interaction limit. No upstream PR or acceptance is claimed. This handoff does
not reopen a declined NodeInfo retry proposal; it documents a lower routing-layer
case separately. No automatic deployment or dependency replacement is enabled.

## Patch-file whitespace

Unified patches intentionally contain a leading space on unchanged context
lines. The adjacent .gitattributes disables whitespace diagnostics only for
this literal patch artifact, not for source code. Applying the patch to its
exact base and comparing all seven resulting source hashes is the relevant
integrity check; the resulting source also passes its regression tests.
