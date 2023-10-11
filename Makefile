clean:
	rm -rf ./data/db/*
	rm -rf ./data/raw/*
	rm -rf ./dbt_project/target/*


install:
	python3 -m venv myenv && \
	. myenv/bin/activate && \
	pip install -e .'[dev]'

run-dagster:
	. myenv/bin/activate && \
	dagster dev

