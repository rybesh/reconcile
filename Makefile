PYTHON := ./venv/bin/python
.DEFAULT_GOAL := run

$(PYTHON): requirements.txt
	python3 -m venv venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install wheel
	$(PYTHON) -m pip install -r requirements.txt
	touch $(PYTHON)

.PHONY: run clean

run: | $(PYTHON)
	./reconcile.py

clean:
	rm -rf venv
	rm -f secrets/token.json
