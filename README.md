Source code for Hadoop in Practice, Second Edition
==================================================

This project contains the source code that accompanies the book "Hadoop in Practice, Second Edition".

The book is currently in [MEAP](http://www.manning.com/about/meap.html) form, which gives you early access to chapters
as they are being completed. I'll add a link to the book once it's up on Manning's site.

## License

Apache version 2.0 (for more details look at the [license](LICENSE)).

## Usage

### Tarball

The easiest way to start working with the examples is to download a tarball distribution of this project.
Doing so will mean that running your first example is just three steps away:

1. Go to the [releases](https://github.com/alexholmes/hiped2/releases) and download the most recent tarball.
2. Extract the contents ot the tarball.

        $ tar -xzvf hip-<version>-package.tar.gz

3. The examples in the book all use the `hip` script located in `bin/hip` to
execute the examples. While it's not required, it's recommended that you
add hip-<version>/bin to your
path so that you can simply execute `hip` and execute the examples in the
book by directly copy-pasting the commands.

4. Run the "hello world" example, which is

```bash
$ cd hip-<version>

# create two input files in HDFS
$ hadoop fs -mkdir -p hip1/input
$ echo "cat sat mat" | hadoop fs -put - hip1/input/1.txt
$ echo "dog lay mat" | hadoop fs -put - hip1/input/2.txt

# run the inverted index example
$ ./hip hip.ch1.InvertedIndexJob --input hip1/input --output hip1/output

# examine the results in HDFS
$ hadoop fs -cat hip1/output/part*
```

Done! The tarball also includes the sources and JavaDocs.

### Building your own distribution

Here you're going to checkout the trunk and then use Maven to run a build.

1. Checkout the code.

        $ git clone git@github.com:alexholmes/hiped2.git

2. Build the code and distribution tarball.

```bash
$ cd hiped2
$ mvn clean
$ mvn validate
$ mvn package
```

The JAR's and tarball will be under the `target` directory. Now you can follow the instructions in the
"Tarball" section above to explode the tarball and run an example.

## What's next?

At this point check out the book for more examples and how you can execute them. Or if you find any issues then
please go to the [issues](https://github.com/alexholmes/hiped2/issues) and open a new issue.