## What?

A python serialization performance comparison which also became an experiment in:

* NamedTuples
* Hierarchical data
* Recursive iterators
* Serialization strategies

**NOTE**: This is a quick and dirty benchmark to answer a specific question. It is a repo because it is a decent platform to evaluate new encodings and to revisit each protocol as they evolve or with new test cases. Potentially, it is also the basis of a generally useful blog article.

**_TL;DR:_** Generalized storage solutions are too slow, I need a custom serialization strategy. Skip to the Lessons Learned and then the Project Setup sections below to play with the repo.

## Why?

I have a specific need; high performance serialization of large hierarchical datasets - let's say a million nodes of related data that must be fetched and written en masse. First task in selection is to narrow the field. I can safely omit:

### ORM

Built for convenience, simplified reasoning, not speed. The per-node serialization hit let's us quickly exclude this strategy.

### Relation datastore

Hierarchical data is not a good fit for traditional relational datastores. There are [many solutions optimized for specific needs](https://stackoverflow.com/questions/4048151/what-are-the-options-for-storing-hierarchical-data-in-a-relational-database) but none are great with bulk read/write of large datasets. I could arrange the data so that each node contains enough information to reconstruct the hierarchy in code which sounds promising. But that leads to a further complication: I need to store daily snapshots of many of these large datasets - how do I control database growth and the effects on read/write performance? All solutions make me pause at infrastructure or operational cost.

### Graph databases

Graph databases are primarily used for AI Knowledge Representation - reasoning about data. But if the system stores data in a tree, it might be a good fit, especially if each dataset is ephemeral - I load the dataset, perform operations, then dispose of the dataset. Moderate ops cost, low infrastructure costs, and [Neo4j's article on ingesting 10 million nodes in 3 minutes](https://neo4j.com/blog/import-10m-stack-overflow-questions/) made this strategy sound promising. 

Under the covers, I found this article ommitted several details: the actual time is closer to 4 minutes, the database must be created offline, and then the data must be indexed. Generating the data this way is fast, but starting Neo4J in a container takes 10s and indexing takes a while. Creating a live system to interrogate takes far too long. After playing around a bit, I reached a live system ingestion speed of about 18K nodes / sec. This means nearly a minute for a million node dataset - still far too slow. 

A little googling showed that Neptune and Janusgraph top out at about the same rate.

### A custom solution

At this point, it dawned on me that a generalized solution would always suffer some performance hit simply because of the generalization - I need to try a custom serialization strategy to quantify that difference.

"Common" wisdom is that `ujson` is the fastest way to serialize python. Much of that information is dated, generated for python2, or does not include new or special case encodings. I wanted a quick test to verify common wisdom. This repo is the result.

## Lesson's learned

There is a lot of legacy python performance information that is simply not correct or relevent for modern python. Many include non-disclosed assumptions that may make the results irrelevant. Some interesting fallacies of "general python knowledge" in a modern python (3.7.1) world:

* Pickle is slow. Nope. It is the fastest I have tested.
* cPickle is always better than Pickle. Don't care. In python 3, cPickle is always loaded if it is available (which includes every one of my use cases).
* Pickle is not a good serialization strategy. Many articles point out rough spots in pickle serialization and leave you to believe that is the end of the story. Comparing pickle to java persistance, it is reasonably robust with convenient callouts to allow rehydrating versioned models. Especially if one codebase is reading / writing pickles and you can protect your pickles or otherwise prevent misuse of the REDUCE opcode, this is a great solution.
* Python's json package is slow. Nope. In my use case, it is just behind `ujson`.
* SimpleJson serializes NamedTuples with key names. Although slower than other json serializers, this encoding omit's the need to call `NamedTuple._asdict()` to create a json serializable dict-like output.

## Getting started

### Create the virtualenv

Create a 3.7.1 virtualenv and install dependencies. I use pyenv; ignoring pyenv install, it goes like this:

```
pyenv install 3.7.1
pyenv virtualenv 3.7.1 py-serialization-3.7.1
pip install -r requirements_dv.txt
```

If using PyCharm, set your project interpreter to the new venv, and turn off py2.7 version compatibility inspections.

### Generate datasets

The benchmarks use dir-file hierarchies to quickly generate test datasets. The datasets are graduated to test performance across different datasets. We use the cpython and golang repos to generate the data. The first step is to clone these:

* [https://github.com/python/cpython](https://github.com/python/cpython)
* [https://github.com/golang](https://github.com/golang)

Point `py-serialization.config` at these cloned directories.

Generate the default datasets

```
./generator.py -d
```

Copy the output to `generator.py::CASE_INFO`

**NOTE:** If you want a simple large dataset, uncomment the `case_home` lines in `generator.pickle_default_datasets()`. If generation produces a traceback (errors are OK), add the offending directory to the exclusions set in `generator.pickle_default_datasets()` for `case_home`. This dataset isn't essential to get a performance indicator - if you compare serialization speed for cases 100, 1000, 10000, you can readily see the trend. Although, it was important in identifying fragility in the MSGPACK protocol.

### Run a sample benchmark

A general, quick performance indicator:

```
```

## Benchmark results

See `./bench.py -h` for more info.

**WiP**: example run: `./bench.py --read --case case_10000 -i3 -t all`

```
Case	Nodes	Duration	Nodes/sec
PICKLE   	9847	0.0219	448834
BSON     	9847	0.0915	107591
CBOR     	9847	0.2267	 43434
CBOR2    	9847	0.4950	 19892
CSV      	9847	0.1769	 55663
JSON     	9847	0.0813	121073
MSGPACK  	9847	0.0747	131866
RAPIDJSON	9847	0.0990	 99474
SIMPLEJSON	9847	0.0952	103470
UJSON    	9847	0.0849	115930
```
