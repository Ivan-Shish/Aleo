# Aleo Setup `phase1-coordinator` Requirements

Vaguely following the format outlined here: <https://www.lucidchart.com/blog/software-requirements-documentation>.

This document contains documentation of the required functionality for the [`phase1-coordinator`](../) module.

Related overall setup requirements document here [Aleo Setup PRD][Aleo Setup PRD]

## Definitions

Definitions of various terms which are relevant to the purpose and implementation of the coordinator. This list is sorted in alphabetical order, however a good place to start with general reading is at the [Setup Ceremony](#Setup-Ceremony)

### Chunk

The parameters are broken into chunks so that multiple [contributor](#Contributor)s can make contributions in parallel. See [Optimistic Pipelining](#Optimistic-Pipelining) for an overview of the process.

### Contribution

As part of the design for [Optimistic Pipelining](#Optimistic-Pipelining), at the end of a round, each [chunk](#Chunk) will contain one contribution for each [contributor](#Contributor) participating in the round.

### Contributor

A contributor is a type of [participant](#Participant), typically run by a member of the public who wishes to participate in the [ceremony](#Setup-Ceremony) by making [contributions](#Contribution). There are currently two contributor software implementations: [cli contributor](../../setup1-contributor) (in this repository) and the [web contributor](https://github.com/AleoHQ/aleo-setup-frontend). A contributor runs this software to participate in the ceremony. If a contributor is [dropped](#Drop) during a [round](#Round) they can be replaced by a [Replacement Contributor](#Replacement-Contributor).

A contributor's contributions are verified by a [Verifier](#Verifier) participant before they are accepted and can have other contributions made based on them.

### Coordinator

The coordinator directs [participants](#Participant) to perform the [Setup Ceremony](#Setup-Ceremony), and collates/checks the all chunks/contributions before continuing to the next [round](#Round) of the ceremony. The core logic for the coordinator is contained within this [`phase1-coordinator`](../) module, and the web server interface to this logic resides in the [`aleo-setup-coordinator` repository](https://github.com/AleoHQ/aleo-setup-coordinator/).

### Drop

A participant may be "dropped" from a [round](#Round) in progress by the [Coordinator](#Coordinator) for a variety of reasons. In the case of a dropped [Contributor](#Contributor) this involves removing their contributions, and, replacing them with a [Replacement Contributor](#Replacement-Contributor).

### Optimistic Pipelining

The marketing logic is that by making it easy for more people to contribute, it will increase the public's perceived trustworthiness of the system in question. **Optimistic Pipelining** is a new technique implemented for this project to allow [contributor](#Contributor)s to make contributions in parallel for a given round.

![Powers of Tau Diagram](./PowersofTau.jpg)

With optimistic pipelining:

+ For a given [round](#Round) the [parameters](#Parameters) are divided into [chunk](#Chunk)s. **TODO**: is this the correct terminology?.
+ A [chunk](#Chunk) will be contributed to once by every contributor in the round, each [contribution](#Contribution) is based on the previous.
+ After a contribution has been made to a chunk, it needs to be verified before another contributor can make the next contribution.
+ A chunk can only have one contributor at a given time.

See [Setup Ceremonies ZKProof][Setup Ceremonies ZKProof] for a more detailed explanation and background for this technique.

### Parameters

TODO: specifically what are the parameters?

### Participant

A participant is an actor who performs a role during the [Setup Ceremony](#Setup-Ceremony): the [coordinator](#Coordinator) provides them with tasks to perform. There are currently two type of participants: [contributor](#Contributor)s and [verifiers](#Verifier)s.

### Replacement Contributor

A [contributor](#Contributor) run by `aleo` which waits idle for a contributor to be [dropped](#Drop) from a [round](#Round) in progress, at which point the [coordinator](#Coordinator) will assign it to take over the dropped contributor's tasks to allow the round to be completed.

### Round

A round of the [setup ceremony](#Setup-Ceremony). Each round ideally consists of a new set of [contributor](#Contributor)s. A ceremony will consist of many rounds in order to involve more contributors across a larger time scale. The number of contributors in a single round is limited, so this also provides a mechanism to increase capacity for unique contributors in the ceremony.

### Setup Ceremony

A **Setup Ceremony** (or **Setup** for short) is currently necessary pre-requisite task for a Zero Knowledge cryptography system. The aim of a setup is to generate a set of [parameters](#Parameters) using random numbers which will be used as a basis for proofs **TODO is this correct?**. In order for the system to be trustworthy, the source of randomness needs to be discarded. The ceremony is in effect, a public stage-show where members of the public are invited to participate as [contributor](#Contributor)s. The advantage of involving multiple contributors from the public is that in order for the system to be trustworthy, only one contributor needs to be honest with disposing of their source of randomness used to generate parameters.

Extra useful references:

+ [Zcash Parameter Generation](https://z.cash/technology/paramgen/)
+ [Setup Ceremonies ZKProof][Setup Ceremonies ZKProof]

### Verifier

TODO: description

## Purpose

The purpose of the `phase1-contributor` module is to provide the logic for performing a [setup ceremony](#Setup-Ceremony)

<!-- References -->
[Aleo Setup PRD]: https://docs.google.com/document/d/1Vyg2J60zRU6023KXBjZx8CP3V-Nz6hPOUCMplbCxVB4/
[Setup Ceremonies ZKProof]: https://docs.google.com/document/d/189hwHm5UFxT2jOFeCzL_YUvRfXEpTyQ1dsWEtuimR94/
