# HackerNews analysis - How to use it?

Running the scraping script is very simple. You just need to run the following command:

```shell
cd ./scrapper
poetry run python main.py
```

The analysis is spread across multiple scripts.

```shell
cd ./analysis
poetry run python core_analysis.py # Main visualization
poetry run python thematic_extraction.py # Use GPT-4 to extract the topics of interest
poetry run python thematic_analysis.py # Analyze the topics extracted and visualize them
```