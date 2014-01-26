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

1. Click on "releases" and download the most recent tarball.
2. Extract the contents ot the tarball, i.e. `tar -xzvf hip-<version>-package.tar.gz`
3. Run the "hello world" example, which is

```ruby
$ cd hip-<version>
$
```
Click on the "releases" link


### Building your own distribution

To get started, simply:

1. Download, and run `mvn package`.
2. Use the generated JAR `target/hadoop-utils-<version>.jar` in your application.
3. Understand the API's by reading the generated JavaDocs in `target/hadoop-utils-<version>-javadoc.jar`.

### To run the bundled utilities

Look at file [CLI.md](https://github.com/alexholmes/hadoop-utils/blob/master/CLI.md) for more details.

