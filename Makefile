setup: requirements.txt
	pip install -r requirements.txt
clean:
	rm -rf __pycache__
build:
	python3 setup.py sdist bdist_wheel
upload:
	python3 setup.py sdist bdist_wheel; twine upload dist/*
.PHONY: build clean upload