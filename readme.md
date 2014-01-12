Data processing using Hadoop and OpenNLP for a [facebook-sponsored Kaggle competition](http://www.kaggle.com/c/facebook-recruiting-iii-keyword-extraction).

The jar file that is given to Hadoop is produced by `mvn package`, which creates `target/KaggleFB-1.0-SNAPSHOT-job.jar`.

## Sentence extraction

The data processing happens in two steps. First, we parse the CSV file and extract individual sentences from the body
and title fields using the OpenNLP sentence extractor. Unfortunately, this
cannot be done in parallel because CSV files with multi-line fields can't be split by Hadoop (or at least, I can't
figure out a way).

The sentence extraction step is run like so:

```
hadoop jar KaggleFB-1.0-SNAPSHOT-job.jar KaggleFB.SentExtractMR KaggleFB/Train.csv KaggleFB/sent_output
```

This will produce a dataset in the `sent_output` directory with the format

```
id  source  content tags
```

where the fields are defined as

* id - original post id, as defined in `Train.csv`
* source - either `body` or `title` depending on whether the sentence came from a post body or title
* content - the sentence
* tags - the tags associated with the post this sentence came from

## Feature extraction

The feature extraction step is run on the output of the sentence extraction step. The code is run like so:

```
hadoop jar KaggleFB-1.0-SNAPSHOT-job.jar KaggleFB.FeatExtractMR KaggleFB/sent_output/ KaggleFB/feat_output
```

This tokenizes the sentences into words using OpenNLP. It then extracts a set of features and a target for each word.
The targets can be one of

* NOTAG - the word is not part of a tag
* TAGSTART - the word is the first word of a multi-word tag, or a single word tag
* TAGMID - the word is part of a multi-word tag, but not the first word

The features are subject to change as I work on this but they are currently:

* previous word
* next word
* previous target
* word
* is word capitalized?

The output file is in the format:

```
tags    prevWord    nextWord    prevTarget  word    cap?    target
```

The plan is to train a MaxEnt model using Mahout that will determine whether or not a given word is tag-like.