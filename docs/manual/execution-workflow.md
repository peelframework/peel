---
layout: manual
title: Execution Workflow
nav: [ manual, execution-workflow ]
---

# {{ page.title }}



## Execution Lifecycle

The execution lifecycle of an experiment suite and undergoes the following phases:

<div class="deep-tree">
1. For each experiment in the suite, ensure that the required inputs are materialized (either generated or copied) in the respective file system.
1. Systems with *Suite *lifespan required for execution are set up and started by Peel. Systems with *Provided* lifespan are assumed to be already up and running, but are re-configured if necessary.
1. For each experiment in the suite
1. Check the configuration of associated descendant systems with Provided or Suite lifespan against the values defined in the current experiment config. If the values do not match, reconfigure and restart the system.
    1. Set up systems with *Experiment *lifespan.
    1. For each experiment run which has not been completed by a previous invocation of the same suite,
        1. check and set up systems with *Run* lifespan
        1. execute the experiment,
        1. collect the log data from the runner system
        1. tear down systems with *Run* lifespan, and
        1. clear the produced outputs.
    1. Tear down systems with *Experiment* lifespan.
1. Tear down systems with Suite lifespan. Leave systems with Provided lifespan up and running with the current configuration.
</div>

## Example