---
layout: repository
title: Bundles Repository
date: 2015-07-04 10:00:00
nav: repository
---

# {{ page.title }}

One of the main advantages of Peel is the ability to share hand-crafted configurations for a set of systems on a particular host environment.
The suggested way to do so is through a dedicated Git repository. If you are using a versioned bundle, can then link the respository as [a Git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

For example, we offer an example ACME cluster has a shared configuration available [at GitHub](https://github.com/stratosphere/peelconfig.acme), you can use the following command to add it as Git submodule in your `peel-wordcount` bundle:

{% highlight bash %}
git submodule add \
    git@github.com:stratosphere/peelconfig.acme.git \
    [your-project]-bundle/src/main/resources/config/acme-master
{% endhighlight %}

We also encourage beginners to use the [devhost config](https://github.com/stratosphere/peelconfig.devhost) which contains best practice configurations for your developer machine.

{% highlight bash %}
git submodule add \
    git@github.com:stratosphere/peelconfig.devhost.git \
    [your-project]-bundle/src/main/resources/config/$HOSTNAME
{% endhighlight %}

Replace the [your-project] placeholder with the name of your project.