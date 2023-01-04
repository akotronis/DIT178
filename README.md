# Setup environment

```Python
virtualenv -p python3 .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

# Run program

- `python main.py --input ca-AstroPh.txt.gz --edges-num 30 --score-metric cn --master "local[*]"`

or

- `spark-submit main.py --input ca-AstroPh.txt.gz --edges-num 30 --score-metric cn --master "local[*]"`

where

`score-metric` can be

- `cn` &rarr; (Common Neighbors)
- `jc` &rarr; (Jaccard Coefficients)
- `aa` &rarr; (Adamic/Adar)
