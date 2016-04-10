---
layout: repository
title: Environment Configurations Repository
date: 2015-07-05 10:00:00
nav: repository
---

# {{ page.title }}

One of the main advantages of Peel is the ability to share hand-crafted configurations for a set of systems on a particular host environment.
The suggested way to do so is through a dedicated Git repository. 

If you are using a versioned bundle, must clone the repository under your `*-bundle` project.

For example, we offer an example ACME cluster has a shared configuration available [at GitHub](https://github.com/stratosphere/peelconfig.acme), you can use the following command to add it to your `peel-wordcount` bundle:

{% highlight bash %}
git clone \
    git@github.com:stratosphere/peelconfig.acme.git \
    $(pwd)-bundle/src/main/resources/config/hosts/acme-master
{% endhighlight %}

We also encourage beginners to use the [devhost config](https://github.com/stratosphere/peelconfig.devhost) which contains best practice configurations for your developer machine.

{% highlight bash %}
git clone \
    git@github.com:stratosphere/peelconfig.devhost.git \
    $(pwd)-bundle/src/main/resources/config/hosts/$HOSTNAME
{% endhighlight %}

*__Tip__: You can also fork `peelconfig.devhost.git` and share it and instead include the fork.*

The table below lists some shared configurations available at GitHub (to access some of them you need special rights).

| Hostname        | URL                                                       | Owner                                        |
| --------------- | --------------------------------------------------------- | -------------------------------------------- |
| acme-master     | https://github.com/stratosphere/peelconfig.acme           | [DIMA, TU Berlin](http://dima.tu-berlin.de/) |
| [your-host]     | https://github.com/stratosphere/peelconfig.devhost        | [DIMA, TU Berlin](http://dima.tu-berlin.de/) |