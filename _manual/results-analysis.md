---
layout: manual
title: Results Analysis
date: 2015-07-09 10:00:00
nav: [ manual, results-analysis ]
---

# {{ page.title }}

As we saw in the [Execution Workflow]({{ site.baseurl }}/manual/execution-workflow.html) section, Peel experiments results are stored in a folder structure and contain log file data collected from the systems involved in the experiment.
In order to make sense of the data, Peel ships with an extensible ETL pipeline that extracts relevant data from the log files, transforms it into a relational schema, and loads it into a database.
You can then analyze various aspects of the obtained results by querying the underlying result schema with SQL statements.

## Backends

Peel supports multiple relational database engines as a possible backend for your experiment data. The decision which backend to use depends on your use case.

### H2

The [H2 backend](http://www.h2database.com/html/main.html) is the easy, quick and dirty option for beginners. 
If your experiment logs are small, this is the best way to go as it requires zero overhead for setup. 
With the default H2 connection `h2`, Peel will initialize and populate a results database in a file named `h2.mov.db` located in the `${app.path.results}` folder.

### MonetDB

If you have a lot of data or want to do more advanced analytics on the extracted database instance, we recommend using a [column store](https://en.wikipedia.org/wiki/Column-oriented_DBMS) like [MonetDB](http://monetdb.org/).

As a prerequisite, you will have to [install MonetDB](https://www.monetdb.org/Documentation/Guide/Installation) on your machine. Upon that, you need to execute the following commands in order to set up the Peel results database:

{% highlight bash %}
sudo usermod -a -G monetdb $USER       # add yourself as MonetDB admin
cd "$BUNDLE_BIN/peel-wordcount"        # go to your bundle root
monetdbd create $(pwd)/results/monetdb # initialize DB farm
monetdbd start $(pwd)/results/monetdb  # start MonetDB with this farm  
monetdb create peel                    # create DB
monetdb release peel                   # release DB maintenance mode
mclient -u monetdb -d peel             # connect to DB (pw:monetdb)
{% endhighlight %}

The default MonetDB connection `monetdb` should now work for you.

### Other Backends

The above list is not exclusive. Other backends can be easily integrated through the following procedure:

1. Add the the required JDBC driver to the `$BUNDLE_ROOT/lib` folder.
1. Setup a new connection named `${connName}` in the `app.db.${connName}` section of your *application.conf*. The minimum information you should supply is the JDBC connection `url`. Optionally, you can also set  `user` and `password`, if your connection requires authentication.

## DB Initialization

Once you have decided on a backend, you need to initialize the DB schema. 
Before you do this, make sure that the the proper connection information is configured under `app.db.${connName}`. 
As described above, Peel has [two pre-configured connections](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/resources/reference.peel.conf#L15): `h2` (for an H2 database), and `monetdb` (for a MonetDB backend). 
You can initialize the database for a specified connection with the `db:initialize` command.

{% highlight bash %}
./peel.sh db:initialize --connection=h2 --force 
{% endhighlight %}

Use the optional `--force` flag if you want to delete existing old schema and data.

## DB Import

After the DB schema is initialized, you can [extract, transform, and load (ETL)](https://en.wikipedia.org/wiki/Extract,_transform,_load) the raw data from the results folder into your backend results database with the help of the `db:import` command. 
For example, in order to load the data form the `wordcount.scale-out` suite into your (previously intialized) `h2` database , you need to run the following code.

{% highlight bash %}
./peel.sh db:import --connection=h2 wordcount.scale-out 
{% endhighlight %}

## Analysis

To visually explore and analyse the results of your experiments, you can connect the database schema produced by Peel with a reporting tool like [JasperReports](http://community.jaspersoft.com/project/ireport-designer), an OLAP cube analysis tool like [Pentaho](http://www.pentaho.com/), or a visual data exploration tool like [Tableau](http://www.tableau.com/).

In addition to that, you can also add extra commands to your `*-peelextensions` package that issue SQL statements to the results database and visualize them appropriately.
Take a look at [the `query:runtimes` command](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-peelextensions/src/main/scala/org/peelframework/wordcount/cli/command/QueryRuntimes.scala) from the `peel-wordcount` bundle for a small example.
