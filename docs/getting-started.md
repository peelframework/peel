---
layout: simple
title: Getting Started
nav: getting-started
---

# Getting Started

A Peel package bundles together the configuration data, datasets, and programs required for the execution of a particular set of experiments, and is therefore shortly referred to as a *Peel bundle*. 

## Bootstrap a Bundle

To get started, you need to get a Peel bundle using one of the following methods.

### Pre-Packaged Archive

The simplest way is download and extract the [pre-packaged empty bundle archive](http://peel-framework.org/peel-empty-bundle.tar.gz).

{% highlight bash %}
wget http://peel-framework.org/peel-empty-bundle.tar.gz  # download empty bundle archive
tar -xzvf peel-empty-bundle.tar.gz -C $BUNDLE_DIST       # extract the archive
cd $BUNDLE_DEST                                          # go to dest. dir
{% endhighlight %}

### Build from Source

You can also build an empty bundle from source:

{% highlight bash %}
git clone https://github.com/stratosphere/peel.git       # clone
cd peel                                                  # go to the src dir
mvn clean package -DskipTests                            # build from src
cp -R peel-dist/target/peel-bundle $BUNDLE_DEST          # copy empty bundle
cd $BUNDLE_DEST                                          # go to dest. dir
{% endhighlight %}

### Maven Archetype

If you intend to version the code in your bundle, the best way to start is the Peel bootstrap Maven archetype:  

{% highlight bash %}
mkdir -p $BUNDLE_DEST && cd $BUNDLE_DEST                 # go to dest. dir
mvn archetype:generate                                   \
  -DgroupId=$BUNDLE_GROUP_ID                             \
  -DartifactId=$BUNDLE_ARTIFACT_ID                       \
  -DarchetypeArtifactId=peel-bootstrap-bundle            \
  -DinteractiveMode=false                                # init. new bundle 
{% endhighlight %}

## Run the Example Experiment

[TODO]

## Check the Results

[TODO]

## Next Steps

Interested in learning more? Check the [Motivation]({{ site.baseurl }}/manual/motivation.html) section for a brief introduction to system experiments and an overview of the problems Peel will solve for you.
Alternatively, go directly to [Bundle Basics]({{ site.baseurl }}/manual/bundle-basics.html) if you want to get your hands right away!