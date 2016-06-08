This folder contains the archived versions of the systems in your bundle.

To download the archives for the list of systems pre-configured in

    https://github.com/peelframework/peel/blob/master/peel-extensions/src/main/resources/peel-extensions.xml

open a shell in this folder and execute the following command

    wget -N -i systems.txt

If you have multiple bundles sharing the same set of systems, you might want to
point the 'app.paths.downloads' folder to a shared location in the
application.conf file for your host.