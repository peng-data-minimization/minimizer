# Generate Config to emulate K-Anonymity
Applying k-Anonymity in a streaming context is no straight forward task. There is no way you can guarantee a certain k
for an incomplete dataset (which natively applies the case in a streaming context). In order to achieve a similar result,
we try to derive rules from running a k-anonymity algorithm on a pre defined, representative, set of data that we can
then apply to our stream of data.

You can generate a config file, that you can use for the [data minimization spi](https://github.com/peng-data-minimization/kafka-spi>),
by running the `generate_config.py`. Note, that this configuration is specific to the tool mentioned above, but you can
derive the rules by looking at the tasks that are created and apply them to any given context.

