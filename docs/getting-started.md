---
layout: simple
title: Getting Started
nav: getting-started
---

# Getting Started

A Peel package bundles together the configuration data, datasets, and programs required for the execution of a particular set of experiments, and is therefore shortly referred to as a *Peel bundle*. 

## Bootstrap a Bundle

To get started, you need to get a Peel bundle using one of the following methods. 

The snippets of code rely on the following shell variables. Modify them accordingly to reflect your developer machine environment.

{% highlight bash %}
export BUNDLE_BIN=/path/to/bundle/binaries          # bundle binaries
export BUNDLE_SRC=/path/to/bundle/sources           # bundle sources
export BUNDLE_GID=com.acme.peel                     # bundle group
export BUNDLE_AID=peel-bundle                       # bundle name
{% endhighlight %}


### Pre-Packaged Binary

The simplest way is to download and extract the [pre-packaged empty bundle archive](http://peel-framework.org/peel-empty-bundle.tar.gz).

{% highlight bash %}
wget http://peel-framework.org/peel-bundle.tar.gz   # download
tar -xzvf peel-bundle.tar.gz -C "$BUNDLE_BIN"       # extract
mv "$BUNDLE_BIN/peel-bundle"                        \
   "$BUNDLE_BIN/$BUNDLE_AID"                        # move to bundle bin
cd "$BUNDLE_BIN/$BUNDLE_AID"                        # go to bin
{% endhighlight %}

### Build from Source

You can also build an empty bundle from source:

{% highlight bash %}
cd "$BUNDLE_SRC"                                    # go to src
if [ ! -d "$BUNDLE_SRC/peel" ]; then                # if not done before
  git clone --single-branch                         \
      https://github.com/stratosphere/peel.git      # clone peel repo
fi
cd peel                                             # go to repo src
mvn clean package -DskipTests                       # build
mv "peel-bundle/target/peel-bundle"                 \
cd "peel-bundle/target"                             # go to target
mv "peel-bundle"                                    \
   "$BUNDLE_BIN/$BUNDLE_AID"                        # move to bundle bin
cd "$BUNDLE_BIN/$BUNDLE_AID"                        # go to bin
{% endhighlight %}

### Maven Archetype

If you intend to version the code in your bundle, the best way to start is the Peel bootstrap Maven archetype:  

{% highlight bash %}
cd "$BUNDLE_SRC"                                    # go to src
mvn archetype:generate                              \
    -DgroupId=$BUNDLE_GID                           \
    -DartifactId=$BUNDLE_AID                        \
    -DarchetypeArtifactId=peel-bootstrap-bundle     \
    -DinteractiveMode=false                         # init. bundle
mvn clean package -DskipTests                       # build
cd "peel-$BUNDLE_AID-bundle/target"                 # go to target
mv "peel-$BUNDLE_AID-bundle"                        \
   "$BUNDLE_BIN/$BUNDLE_AID"                        # move to bundle bin
cd "$BUNDLE_BIN/$BUNDLE_AID"                        # go to bin
{% endhighlight %}

## Run the Example Experiment

[TODO]

## Check the Results

[TODO]

## Next Steps

Interested in learning more? Check the [Motivation]({{ site.baseurl }}/manual/motivation.html) section for a brief introduction to system experiments and an overview of the problems Peel will solve for you.
Alternatively, go directly to [Bundle Basics]({{ site.baseurl }}/manual/bundle-basics.html) if you want to get your hands right away!